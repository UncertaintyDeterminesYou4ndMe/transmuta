use crate::cli::OutputFormat;
use crate::error::{Result, TransmutaError};
use crate::utils;
use calamine::{open_workbook, Reader, Xlsx, DataType as ExcelDataType};
use std::path::Path;
use log::{info, debug};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::time::Instant;
use std::sync::Arc;

/// 将Excel单元格数据转换为字符串
fn cell_to_string(cell: &ExcelDataType) -> String {
    match cell {
        ExcelDataType::Empty => String::new(),
        ExcelDataType::String(s) => s.clone(),
        ExcelDataType::Float(f) => f.to_string(),
        ExcelDataType::Int(i) => i.to_string(),
        ExcelDataType::Bool(b) => if *b { "true".to_string() } else { "false".to_string() },
        ExcelDataType::DateTime(dt) => {
            // 将Excel日期时间转换为字符串 (Excel日期是从1900-01-01开始的天数)
            // 这里简化处理，实际应用中可能需要更精确的转换
            let days_since_1900 = *dt;
            format!("{:.6}", days_since_1900) // 以浮点数形式保存
        },
        ExcelDataType::Error(_) => "[ERROR]".to_string(),
        ExcelDataType::Duration(d) => format!("{:.6}", d),
        ExcelDataType::DateTimeIso(s) => s.clone(),
        ExcelDataType::DurationIso(s) => s.clone(),
    }
}

/// 转换Excel文件到其他格式
pub fn convert_excel(
    input_path: &Path,
    output_path: &Path,
    format: &OutputFormat,
    batch_size: usize,
    delimiter: char,
    threads: Option<usize>,
    skip_rows: usize,
) -> Result<()> {
    let start_time = Instant::now();
    
    // 检查输入文件是否是Excel文件
    let ext = utils::get_file_extension(input_path)?;
    if !["xlsx", "xls", "xlsm"].contains(&ext.as_str()) {
        return Err(TransmutaError::FileFormatError(format!(
            "不支持的Excel文件格式: {}", ext
        )));
    }
    
    info!("开始处理Excel文件: {}", input_path.display());
    
    // 打开Excel文件
    let mut workbook: Xlsx<_> = open_workbook(input_path)?;
    
    // 获取第一个工作表
    let sheet_names = workbook.sheet_names().to_vec();
    if sheet_names.is_empty() {
        return Err(TransmutaError::DataProcessingError("Excel文件中没有工作表".to_string()));
    }
    
    let sheet_name = &sheet_names[0];
    info!("使用工作表: {}", sheet_name);
    
    // 读取工作表内容
    if let Some(Ok(range)) = workbook.worksheet_range(sheet_name) {
        // 获取总行数
        let row_count = range.height();
        if row_count <= skip_rows {
            return Err(TransmutaError::DataProcessingError(format!(
                "工作表行数({})小于等于要跳过的行数({})", row_count, skip_rows
            )));
        }
        
        let effective_row_count = row_count - skip_rows;
        info!("总行数: {}, 有效行数: {}", row_count, effective_row_count);
        
        // 设置进度条
        let pb = ProgressBar::new(effective_row_count as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"));
        
        // 确定并创建标题
        let headers: Vec<String> = if skip_rows < range.height() && range.width() > 0 {
            range.rows()
                .nth(skip_rows)
                .map(|row| {
                    row.iter()
                       .enumerate()
                       .map(|(i, cell)| {
                           // 如果单元格为空，生成默认的列名
                           match cell {
                               ExcelDataType::Empty => format!("Column{}", i + 1),
                               _ => cell_to_string(cell),
                           }
                       })
                       .collect()
                })
                .unwrap_or_else(|| {
                    // 如果没有有效行，创建默认列名
                    (0..range.width()).map(|i| format!("Column{}", i + 1)).collect()
                })
        } else {
            (0..range.width()).map(|i| format!("Column{}", i + 1)).collect()
        };
        
        debug!("列标题: {:?}", headers);
        
        // 初始化Arrow字段
        let schema = Schema::new(
            headers.iter().map(|name| {
                Field::new(name, DataType::Utf8, true)
            }).collect::<Vec<Field>>()
        );
        
        // 计算批次数
        let batch_count = (effective_row_count + batch_size - 1) / batch_size;
        info!("将数据分为{}个批次处理，每批次{}行", batch_count, batch_size);
        
        // 设置线程数
        let thread_count = utils::get_thread_count(threads);
        
        // 处理数据
        let mut processed_rows = 0;
        
        for batch_idx in 0..batch_count {
            let start_row = skip_rows + batch_idx * batch_size;
            let end_row = std::cmp::min(skip_rows + (batch_idx + 1) * batch_size, row_count);
            let current_batch_size = end_row - start_row;
            
            debug!("处理批次 {}/{}: 行 {} 到 {}", batch_idx + 1, batch_count, start_row, end_row - 1);
            
            // 为每列创建一个StringBuilder
            let mut string_builders: Vec<StringBuilder> = headers.iter()
                .map(|_| StringBuilder::new())
                .collect();
            
            // 添加数据到builders
            for row_idx in start_row..end_row {
                if let Some(row) = range.rows().nth(row_idx) {
                    for (col_idx, cell) in row.iter().enumerate() {
                        if col_idx < string_builders.len() {
                            string_builders[col_idx].append_value(&cell_to_string(cell));
                        } else {
                            string_builders.push(StringBuilder::new());
                            string_builders.last_mut().unwrap().append_value(&cell_to_string(cell));
                        }
                    }
                    
                    // 对于缺失的列，添加空字符串
                    for col_idx in row.len()..headers.len() {
                        string_builders[col_idx].append_value("");
                    }
                }
                
                processed_rows += 1;
                pb.set_position(processed_rows as u64);
            }
            
            // 创建数组
            let arrays: Vec<Arc<dyn Array>> = string_builders.into_iter()
                .map(|mut builder| Arc::new(builder.finish()) as Arc<dyn Array>)
                .collect();
            
            // 创建RecordBatch
            let record_batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)?;
            
            // 确定输出路径
            let mut output_file_path = output_path.to_path_buf();
            
            // 如果有多个批次，为每个批次生成不同的文件名
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
        
        pb.finish_with_message("Excel文件转换完成");
        
        let elapsed = start_time.elapsed();
        info!("总处理时间: {:.2}秒", elapsed.as_secs_f64());
        
        Ok(())
    } else {
        Err(TransmutaError::ExcelError(format!("无法读取工作表: {}", sheet_name)))
    }
} 