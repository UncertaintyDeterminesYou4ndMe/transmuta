use crate::error::{Result, TransmutaError};
use std::path::Path;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use log::{info, debug};

/// 获取文件扩展名
pub fn get_file_extension(path: &Path) -> Result<String> {
    match path.extension() {
        Some(ext) => Ok(ext.to_string_lossy().to_lowercase()),
        None => Err(TransmutaError::FileFormatError("文件没有扩展名".to_string())),
    }
}

/// 创建缓冲读取器
pub fn create_buf_reader(path: &Path) -> Result<BufReader<File>> {
    let file = File::open(path)
        .map_err(|e| TransmutaError::IoError(e))?;
    Ok(BufReader::new(file))
}

/// 创建缓冲写入器
pub fn create_buf_writer(path: &Path) -> Result<BufWriter<File>> {
    let file = File::create(path)
        .map_err(|e| TransmutaError::IoError(e))?;
    Ok(BufWriter::new(file))
}

/// 获取处理数据时使用的线程数
pub fn get_thread_count(threads: Option<usize>) -> usize {
    match threads {
        Some(t) if t > 0 => t,
        _ => {
            let cpu_count = num_cpus::get();
            // 默认使用可用CPU核心数
            info!("未指定线程数，将使用{}个线程（系统CPU核心数）", cpu_count);
            cpu_count
        }
    }
}

/// 计算处理进度百分比
pub fn calculate_progress(current: usize, total: usize) -> f64 {
    if total == 0 {
        return 0.0;
    }
    (current as f64 / total as f64) * 100.0
}

/// 估计剩余时间（秒）
pub fn estimate_time_remaining(elapsed_secs: f64, progress_percent: f64) -> Option<f64> {
    if progress_percent <= 0.0 {
        return None;
    }
    
    Some((elapsed_secs / progress_percent) * (100.0 - progress_percent))
}

/// 创建输出目录（如果不存在）
pub fn ensure_output_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            debug!("创建输出目录: {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }
    Ok(())
} 