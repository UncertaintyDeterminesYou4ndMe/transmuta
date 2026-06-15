use crate::cli::OutputFormat;
use crate::error::{Result, TransmutaError};
use crate::utils;
use std::path::Path;
use std::fs::File;
use std::io::{BufReader, Read};
use log::{info, warn};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Instant;
use std::sync::Arc;
use serde_json::{Value, json};

/// 转换JSON文件到其他格式
pub fn convert_json(
    input_path: &Path,
    output_path: &Path,
    format: &OutputFormat,
    batch_size: usize,
    delimiter: char,
    threads: Option<usize>,
) -> Result<()> {
    let start_time = Instant::now();
    
    // 检查输入文件扩展名
    let ext = utils::get_file_extension(input_path)?;
    if ext != "json" {
        warn!("输入文件扩展名不是.json: {}", ext);
    }
    
    info!("开始处理JSON文件: {}", input_path.display());
    
    // 读取JSON文件
    let file = File::open(input_path)?;
    let mut reader = BufReader::new(file);
    let mut contents = String::new();
    reader.read_to_string(&mut contents)?;
    
    // 解析JSON数据
    let json_value: Value = match serde_json::from_str(&contents) {
        Ok(value) => value,
        Err(e) => return Err(TransmutaError::DataProcessingError(format!("解析JSON失败: {}", e)).into()),
    };
    
    // 将JSON数据转换为数组格式
    let json_data = match json_value {
        Value::Array(arr) => arr,
        Value::Object(obj) => {
            // 如果是单个对象，将其转换为包含单个元素的数组
            vec![Value::Object(obj)]
        },
        _ => return Err(TransmutaError::DataProcessingError("JSON文件必须是对象或对象数组".to_string()).into()),
    };
    
    let total_rows = json_data.len();
    info!("JSON文件共有{}条记录", total_rows);
    
    if total_rows == 0 {
        return Err(TransmutaError::DataProcessingError("JSON文件为空".to_string()).into());
    }
    
    // 创建进度条
    let pb = ProgressBar::new(total_rows as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .unwrap()
        .progress_chars("#>-"));
    
    // 计算处理批次
    let batch_count = (total_rows + batch_size - 1) / batch_size;
    info!("将数据分为{}个批次处理，每批次{}条记录", batch_count, batch_size);
    
    // 收集所有字段名
    let mut all_fields = std::collections::HashSet::new();
    for record in &json_data {
        if let Value::Object(obj) = record {
            for key in obj.keys() {
                all_fields.insert(key.clone());
            }
        }
    }
    
    let field_names: Vec<String> = all_fields.into_iter().collect();
    info!("JSON数据包含{}个字段", field_names.len());
    
    // 设置线程数
    let _thread_count = utils::get_thread_count(threads);
    
    // 处理每个批次
    for batch_idx in 0..batch_count {
        let start_idx = batch_idx * batch_size;
        let end_idx = std::cmp::min(start_idx + batch_size, total_rows);
        let batch_data = &json_data[start_idx..end_idx];
        
        // 创建列构建器
        let mut string_builders: Vec<StringBuilder> = field_names.iter()
            .map(|_| StringBuilder::new())
            .collect();
        
        // 填充数据
        for record in batch_data {
            if let Value::Object(obj) = record {
                for (field_idx, field_name) in field_names.iter().enumerate() {
                    let field_value = obj.get(field_name).unwrap_or(&json!(null));
                    let value_str = match field_value {
                        Value::Null => "",
                        Value::String(s) => s,
                        _ => &field_value.to_string(),
                    };
                    string_builders[field_idx].append_value(value_str);
                }
            }
            
            pb.inc(1);
        }
        
        // 创建数组
        let arrays: Vec<Arc<dyn Array>> = string_builders.into_iter()
            .map(|mut builder| Arc::new(builder.finish()) as Arc<dyn Array>)
            .collect();
        
        // 创建字段和模式
        let fields: Vec<Field> = field_names.iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        
        let schema = Arc::new(Schema::new(fields));
        
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
    
    pb.finish_with_message("JSON文件转换完成");
    
    let elapsed = start_time.elapsed();
    info!("总处理时间: {:.2}秒", elapsed.as_secs_f64());
    
    Ok(())
} 