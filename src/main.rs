mod waifu2x;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tokio::fs;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use axum::{
    extract::{Multipart, Path as AxumPath, State},
    http::StatusCode,
    routing::{get, post},
    Router,
};
use axum::Json;

use serde::Serialize;
use walkdir::WalkDir;

// ── 任务状态 ──
#[derive(Clone, Serialize)]
enum TaskStatus {
    Queued,
    Processing,
    Done { output_path: String },
    Failed(String),
}

// ── 共享状态 ──
#[derive(Clone)]
struct AppState {
    tasks: Arc<Mutex<HashMap<String, TaskStatus>>>,
    upload_dir: String,
    output_dir: String,
    max_storage: u64,
    sender: tokio::sync::mpsc::Sender<String>,
}

// ── 返回 JSON ──
#[derive(Serialize)]
struct Response {
    task_id: String,
    status: String,
}

// ── POST /upload ──
async fn upload(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Json<Response> {
    println!("[upload] ====== 收到上传请求 ======");

    // 1. 检查存储上限
    let total_size: u64 = WalkDir::new(&state.upload_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();
    println!("[upload] 当前存储用量: {} bytes / {} bytes", total_size, state.max_storage);

    if total_size > state.max_storage {
        println!("[upload] 拒绝: 存储已满");
        return Json(Response {
            task_id: String::new(),
            status: "storage_full".into(),
        });
    }

    // 2. 读上传的文件
    println!("[upload] 开始解析 multipart 字段...");
    loop {
        let field_result = multipart.next_field().await;
        match field_result {
            Ok(Some(field)) => {
                let original_name = field
                    .file_name()
                    .unwrap_or("(no filename)")
                    .to_string();
                let field_name = field.name().unwrap_or("(no name)").to_string();
                println!("[upload] 字段名={}, 文件名={}", field_name, original_name);

                let ext = original_name
                    .rsplit('.')
                    .next()
                    .unwrap_or("jpg")
                    .to_string();

                match field.bytes().await {
                    Ok(data) => {
                        println!("[upload] 收到 {} bytes", data.len());
                        if data.is_empty() {
                            println!("[upload] 警告: 文件数据为空!");
                        }

                        let task_id = uuid::Uuid::new_v4().to_string();
                        let filename = format!("{}/{}.{}", state.upload_dir, task_id, ext);

                        match fs::write(&filename, &data).await {
                            Ok(()) => println!("[upload] 已保存: {}", filename),
                            Err(e) => {
                                println!("[upload] 写文件失败: {}", e);
                                return Json(Response {
                                    task_id: String::new(),
                                    status: format!("write_error: {}", e),
                                });
                            }
                        }

                        {
                            let mut tasks = state.tasks.lock().await;
                            tasks.insert(task_id.clone(), TaskStatus::Queued);
                        }
                        match state.sender.send(task_id.clone()).await {
                            Ok(()) => println!("[upload] 已入队: {}", task_id),
                            Err(e) => println!("[upload] 入队失败: {}", e),
                        }

                        return Json(Response {
                            task_id,
                            status: "queued".into(),
                        });
                    }
                    Err(e) => {
                        println!("[upload] 读字段数据失败: {}", e);
                        return Json(Response {
                            task_id: String::new(),
                            status: format!("read_error: {}", e),
                        });
                    }
                }
            }
            Ok(None) => {
                println!("[upload] multipart 字段结束，没收到文件");
                break;
            }
            Err(e) => {
                println!("[upload] multipart 解析错误: {}", e);
                return Json(Response {
                    task_id: String::new(),
                    status: format!("multipart_error: {}", e),
                });
            }
        }
    }

    println!("[upload] 返回 no_file");
    Json(Response {
        task_id: String::new(),
        status: "no_file".into(),
    })
}

// ── GET /status/:task_id ──
async fn get_status(
    State(state): State<AppState>,
    AxumPath(task_id): AxumPath<String>,) -> Result<Json<serde_json::Value>, StatusCode> {
    let tasks = state.tasks.lock().await;
    match tasks.get(&task_id) {
        Some(TaskStatus::Done { .. }) => Ok(Json(serde_json::json!({
            "task_id": task_id,
            "status": "done",
            "download_url": format!("/download/{}", task_id),
        }))),
        Some(TaskStatus::Failed(msg)) => Ok(Json(serde_json::json!({
            "task_id": task_id,
            "status": "failed",
            "message": msg,
        }))),
        Some(TaskStatus::Processing) => Ok(Json(serde_json::json!({
            "task_id": task_id,
            "status": "processing",
        }))),
        Some(TaskStatus::Queued) => Ok(Json(serde_json::json!({
            "task_id": task_id,
            "status": "queued",
        }))),
        None => Err(StatusCode::NOT_FOUND),
    }
}

// ── GET /download/:task_id ──
async fn download(
    State(state): State<AppState>,
    AxumPath(task_id): AxumPath<String>,) -> Result<(StatusCode, [(String, String); 2], Vec<u8>), StatusCode> {
    let tasks = state.tasks.lock().await;
    let path = match tasks.get(&task_id) {
        Some(TaskStatus::Done { output_path }) => output_path.clone(),
        _ => return Err(StatusCode::NOT_FOUND),
    };
    drop(tasks);

    match tokio::fs::read(&path).await {
        Ok(data) => {
            let filename = format!("{}.png", task_id);
            Ok((
                StatusCode::OK,
                [
                    ("Content-Type".to_string(), "image/png".to_string()),
                    ("Content-Disposition".to_string(), format!("inline; filename=\"{}\"", filename)),
                ],
                data,
            ))
        }
        Err(_) => Err(StatusCode::NOT_FOUND),
    }
}

#[tokio::main]
async fn main() {
    // 工作目录下的绝对路径
    let base_dir = std::env::current_dir().unwrap();
    let upload_dir = base_dir.join("uploads");
    let output_dir = base_dir.join("outputs");

    fs::create_dir_all(&upload_dir).await.unwrap();
    fs::create_dir_all(&output_dir).await.unwrap();

    // 创建通道
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<String>(32);

    let state = AppState {
        tasks: Arc::new(Mutex::new(HashMap::new())),
        upload_dir: upload_dir.display().to_string(),
        output_dir: output_dir.display().to_string(),
        max_storage: 500 * 1024 * 1024,
        sender,
    };

    // 后台 worker
    let tasks = state.tasks.clone();
    let upload_dir = state.upload_dir.clone();
    let output_dir = state.output_dir.clone();

    tokio::spawn(async move {
        while let Some(task_id) = receiver.recv().await {
            println!("[worker] 开始处理: {}", task_id);

            // 更新 Processing
            {
                let mut map = tasks.lock().await;
                map.insert(task_id.clone(), TaskStatus::Processing);
            }

            // 找输入文件（不管后缀，jpg/png/webp 都可以）
            let input_path = find_file_in_dir(&upload_dir, &task_id);
            let output_path = format!("{}/{}.png", output_dir, task_id);

            match waifu2x::convert(
                Path::new(&input_path),
                Path::new(&output_path),
            ) {
                Ok(()) => {
                    println!("[worker] 完成: {}", task_id);
                    let mut map = tasks.lock().await;
                    let _ =fs::remove_file(&input_path).await;
                    map.insert(
                        task_id,
                        TaskStatus::Done {
                            output_path: output_path.clone(),
                        },
                    );
                }
                Err(e) => {
                    eprintln!("[worker] 失败: {} — {}", task_id, e);
                    let mut map = tasks.lock().await;
                    map.insert(task_id, TaskStatus::Failed(e.to_string()));
                }
            }
        }
    });

    println!("后台 worker 已启动");

    // ── 定时清理（每小时扫一次，删超过 2 天的文件） ──
    let tasks_cleanup = state.tasks.clone();
    let upload_dir_clean = state.upload_dir.clone();
    let output_dir_clean = state.output_dir.clone();

    tokio::spawn(async move {
        loop {
            // 睡 1 小时
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;

            let now = std::time::SystemTime::now();
            let two_days = std::time::Duration::from_secs(2 * 24 * 3600);

            for dir in [&upload_dir_clean, &output_dir_clean] {
                if let Ok(entries) = std::fs::read_dir(dir) {
                    for entry in entries.flatten() {
                        if let Ok(meta) = entry.metadata() {
                            if let Ok(modified) = meta.modified() {
                                if now.duration_since(modified).unwrap_or_default() > two_days {
                                    let path = entry.path();
                                    println!("[cleanup] 删除过期文件: {}", path.display());
                                    let _ = fs::remove_file(&path).await;
                                    // 如果是 output，也从任务表里删掉
                                    if let Some(name) = path.file_stem() {
                                        let id = name.to_string_lossy().to_string();
                                        tasks_cleanup.lock().await.remove(&id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    // HTTP 服务器
    let app: Router = Router::new()
        .route("/upload", post(upload))
        .route("/status/{task_id}", get(get_status))
        .route("/download/{task_id}", get(download))
        .with_state(state)
        .layer(CorsLayer::permissive()) 
        
        ;

    let listener = tokio::net::TcpListener::bind("0.0.0.0:5090")
        .await
        .unwrap();

    println!("服务器启动: http://127.0.0.1:5090");
    axum::serve(listener, app).await.unwrap();
}

// 在目录里找 task_id.xxx 的文件
fn find_file_in_dir(dir: &str, task_id: &str) -> String {
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        // 文件名格式: {task_id}.{ext}
        if name.starts_with(task_id) {
            return format!("{}/{}", dir, name);
        }
    }
    // 找不到就猜 jpg
    format!("{}/{}.jpg", dir, task_id)
}
