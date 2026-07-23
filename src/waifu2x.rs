use std::path::Path;
use std::process::Command;

/// 调用 waifu2x-converter-cpp 处理图片
pub fn convert(input: &Path, output: &Path) -> std::io::Result<()> {
    let build_dir = Path::new("/root/waifu2x-converter-cpp/build");
    let model_dir = "/root/waifu2x-converter-cpp/models_rgb";

    let result = Command::new("./waifu2x-converter-cpp")
        .arg("-i")
        .arg(input)
        .arg("-o")
        .arg(output)
        .arg("--scale-ratio")
        .arg("2")
        .arg("--noise-level")
        .arg("1")
        .arg("--model-dir")
        .arg(model_dir)
        .arg("-j")
        .arg("1")
        .current_dir(build_dir)
        .output()?;

    if result.status.success() {
        println!("完成！输出: {}", output.display());
    } else {
        eprintln!("waifu2x 出错: {}", String::from_utf8_lossy(&result.stderr));
    }

    Ok(())
}
