use crate::cli::OutputFormat;
use crate::error::{Result, TransmutaError};
use crate::utils;
use std::path::Path;
use std::fs::File;
use std::io::BufReader;
use log::{info, warn};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::time::Instant;
use std::sync::Arc;
use csv::{ReaderBuilder, StringRecord};

/// 转换CSV文件到其他格式
pub fn convert_csv(
    input_path: &Path,
    output_path: &Path,
    format: &OutputFormat,
    batch_size: usize,
    delimiter: char,
    threads: Option<usize>,
    has_header: bool,
) -> Result<()> {
    let start_time = Instant::now();
    
    // 检查输入文件扩展名
    let ext = utils::get_file_extension(input_path)?;
    if ext != "csv" {
        warn!("输入文件扩展名不是.csv: {}", ext);
    }
    
    info!("开始处理CSV文件: {}", input_path.display());
    
    // 打开CSV文件
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);
    
    // 创建CSV读取器
    let mut csv_reader = ReaderBuilder::new()
        .delimiter(delimiter as u8)
        .has_headers(has_header)
        .from_reader(reader);
    
    // 获取标题
    let headers = if has_header {
        csv_reader.headers()?.clone()
    } else {
        // 如果没有标题，读取第一行数据，然后为其创建默认标题
        if let Some(result) = csv_reader.records().next() {
            let first_row = result?;
            let col_count = first_row.len();
            let default_headers = StringRecord::from(
                (0..col_count).map(|i| format!("Column{}", i + 1)).collect::<Vec<String>>()
            );
            default_headers
        } else {
            return Err(TransmutaError::DataProcessingError("CSV文件为空".to_string()));
        }
    };
    
    // 重新打开文件，因为我们可能已经读取了一些数据
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);
    let mut csv_reader = ReaderBuilder::new()
        .delimiter(delimiter as u8)
        .has_headers(has_header)
        .from_reader(reader);
    
    // 如果之前读取了一行数据（没有标题的情况），需要把文件指针重置
    if !has_header {
        // 跳过第一行
        if csv_reader.records().next().is_none() {
            return Err(TransmutaError::DataProcessingError("CSV文件为空".to_string()));
        }
    }
    
    // 计算文件总行数（这可能会遍历整个文件，对于大文件可能效率不高）
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);
    let count_reader = ReaderBuilder::new()
        .delimiter(delimiter as u8)
        .has_headers(has_header)
        .from_reader(reader);
    
    let total_rows = count_reader.into_records().count();
    info!("CSV文件共有{}行数据", total_rows);
    
    // 创建进度条
    let pb = ProgressBar::new(total_rows as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-"));
    
    // 计算处理批次
    let batch_count = (total_rows + batch_size - 1) / batch_size;
    info!("将数据分为{}个批次处理，每批次{}行", batch_count, batch_size);
    
    // 重新打开文件
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);
    let mut csv_reader = ReaderBuilder::new()
        .delimiter(delimiter as u8)
        .has_headers(has_header)
        .from_reader(reader);
    
    // 跳过标题行
    if has_header {
        csv_reader.headers()?;
    }
    
    // 创建schema
    let fields: Vec<Field> = headers.iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect();
    
    let schema = Arc::new(Schema::new(fields));
    
    // 设置线程数
    let thread_count = utils::get_thread_count(threads);
    
    // 处理每个批次
    let mut records = csv_reader.records();
    let mut processed_records = 0;
    
    for batch_idx in 0..batch_count {
        // 创建列构建器
        let mut string_builders: Vec<StringBuilder> = headers.iter()
            .map(|_| StringBuilder::new())
            .collect();
        
        // 读取批次数据
        let mut batch_records = 0;
        
        while batch_records < batch_size {
            if let Some(result) = records.next() {
                let record = result?;
                
                // 添加每列数据
                for (col_idx, field) in record.iter().enumerate() {
                    if col_idx < string_builders.len() {
                        string_builders[col_idx].append_value(field);
                    }
                }
                
                // 如果某行数据列数少于标题列数，填充空值
                for col_idx in record.len()..headers.len() {
                    string_builders[col_idx].append_value("");
                }
                
                batch_records += 1;
                processed_records += 1;
                pb.set_position(processed_records as u64);
            } else {
                // 没有更多数据了
                break;
            }
        }
        
        if batch_records == 0 {
            // 这个批次没有任何数据，跳过
            continue;
        }
        
        // 创建数组
        let arrays: Vec<Arc<dyn Array>> = string_builders.into_iter()
            .map(|mut builder| Arc::new(builder.finish()) as Arc<dyn Array>)
            .collect();
        
        // 创建RecordBatch
        let record_batch = RecordBatch::try_new(schema.clone(), arrays)?;
        
        // 确定输出路径
        let mut output_file_path = output_path.to_path_buf();
        
        // 为多批次生成不同的文件名
        if batch_count > 1 {
            if let Some(file_name) = output_path.file_stem() {
                let mut new_file_name = file_name.to_string_lossy().to_string();
                new_file_name.push_str(&format!("_part{:04}", batch_idx + 1));
                
                if let Some(ext) = output_path.extension() {
                    new_file_name.push('.');
                    new_file_name.push_str(&ext.to_string_lossy());
                }
                
                output_file_path = output_path.with_file_name(new_file_name);
            }
        }
        
        // 保存到指定格式
        super::common::save_data(&record_batch, &output_file_path, format, delimiter)?;
    }
    
    pb.finish_with_message("CSV文件转换完成");
    
    let elapsed = start_time.elapsed();
    info!("总处理时间: {:.2}秒", elapsed.as_secs_f64());
    
    Ok(())
} 