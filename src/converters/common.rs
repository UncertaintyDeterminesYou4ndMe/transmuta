use crate::cli::OutputFormat;
use crate::error::Result;
use std::path::Path;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use parquet::file::properties::WriterProperties;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use log::{info, debug};
use serde_json::{json, Value};

/// 将数据保存为CSV格式
pub fn save_as_csv(
    data: &RecordBatch, 
    output_path: &Path, 
    delimiter: char
) -> Result<()> {
    debug!("将数据保存为CSV格式: {:?}", output_path);
    
    let file = File::create(output_path)?;
    let mut writer = csv::WriterBuilder::new()
        .delimiter(delimiter as u8)
        .from_writer(file);
    
    // 写入标题行
    let schema = data.schema();
    let header: Vec<String> = schema.fields().iter()
        .map(|f| f.name().clone())
        .collect();
    
    writer.write_record(&header)?;
    
    // 写入数据行
    for row_idx in 0..data.num_rows() {
        let mut record = Vec::new();
        
        for col_idx in 0..data.num_columns() {
            let column = data.column(col_idx);
            let value = array_value_to_string(column, row_idx);
            record.push(value);
        }
        
        writer.write_record(&record)?;
    }
    
    writer.flush()?;
    Ok(())
}

/// 将数据保存为JSON格式
pub fn save_as_json(data: &RecordBatch, output_path: &Path) -> Result<()> {
    debug!("将数据保存为JSON格式: {:?}", output_path);
    
    let schema = data.schema();
    let mut json_records = Vec::new();
    
    for row_idx in 0..data.num_rows() {
        let mut row_obj = serde_json::Map::new();
        
        for col_idx in 0..data.num_columns() {
            let field = schema.field(col_idx);
            let column = data.column(col_idx);
            let field_name = field.name();
            
            let value = array_value_to_json(column, row_idx);
            row_obj.insert(field_name.clone(), value);
        }
        
        json_records.push(Value::Object(row_obj));
    }
    
    let file = File::create(output_path)?;
    serde_json::to_writer_pretty(file, &json_records)?;
    
    Ok(())
}

/// 将数据保存为Parquet格式
pub fn save_as_parquet(data: &RecordBatch, output_path: &Path) -> Result<()> {
    debug!("将数据保存为Parquet格式: {:?}", output_path);
    
    let file = File::create(output_path)?;
    
    let props = WriterProperties::builder()
        .build();
    
    let schema = data.schema();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    
    writer.write(data)?;
    writer.close()?;
    
    Ok(())
}

/// 根据输出格式选择合适的保存方式
pub fn save_data(
    data: &RecordBatch,
    output_path: &Path,
    format: &OutputFormat,
    delimiter: char
) -> Result<()> {
    crate::utils::ensure_output_dir(output_path)?;
    
    match format {
        OutputFormat::Csv => save_as_csv(data, output_path, delimiter)?,
        OutputFormat::Json => save_as_json(data, output_path)?,
        OutputFormat::Parquet => save_as_parquet(data, output_path)?,
    }
    
    info!("数据已保存到: {}", output_path.display());
    Ok(())
}

/// 将数组元素转换为字符串
fn array_value_to_string(array: &ArrayRef, index: usize) -> String {
    if array.is_null(index) {
        return String::new();
    }
    
    match array.data_type() {
        DataType::Null => String::new(),
        DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            array.value(index).to_string()
        }
        DataType::Int8 => {
            let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::Int16 => {
            let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::UInt8 => {
            let array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::UInt16 => {
            let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::UInt32 => {
            let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::UInt64 => {
            let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::Float32 => {
            let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::Float64 => {
            let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            array.value(index).to_string()
        }
        DataType::Date32 => {
            let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
            array.value(index).to_string()
        }
        DataType::Date64 => {
            let array = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let ms = array.value(index);
            chrono::NaiveDateTime::from_timestamp_millis(ms)
                .map(|dt| dt.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| ms.to_string())
        }
        DataType::Timestamp(time_unit, _) => {
            match time_unit {
                TimeUnit::Second => {
                    let array = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                    let ts = array.value(index);
                    chrono::NaiveDateTime::from_timestamp_opt(ts, 0)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| ts.to_string())
                }
                TimeUnit::Millisecond => {
                    let array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                    let ts = array.value(index);
                    chrono::NaiveDateTime::from_timestamp_millis(ts)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
                        .unwrap_or_else(|| ts.to_string())
                }
                TimeUnit::Microsecond => {
                    let array = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    let ts = array.value(index);
                    chrono::NaiveDateTime::from_timestamp_micros(ts)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                        .unwrap_or_else(|| ts.to_string())
                }
                TimeUnit::Nanosecond => {
                    let array = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                    let ts = array.value(index);
                    // 将纳秒转换为秒和纳秒部分
                    let seconds = ts / 1_000_000_000;
                    let nanos = (ts % 1_000_000_000) as u32;
                    chrono::NaiveDateTime::from_timestamp_opt(seconds, nanos)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string())
                        .unwrap_or_else(|| ts.to_string())
                }
            }
        }
        _ => format!("{:?}", array),
    }
}

/// 将数组元素转换为JSON值
fn array_value_to_json(array: &ArrayRef, index: usize) -> Value {
    if array.is_null(index) {
        return Value::Null;
    }
    
    match array.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            json!(array.value(index))
        }
        DataType::Int8 => {
            let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
            json!(array.value(index))
        }
        DataType::Int16 => {
            let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
            json!(array.value(index))
        }
        DataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            json!(array.value(index))
        }
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            json!(array.value(index))
        }
        DataType::UInt8 => {
            let array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            json!(array.value(index))
        }
        DataType::UInt16 => {
            let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            json!(array.value(index))
        }
        DataType::UInt32 => {
            let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            json!(array.value(index))
        }
        DataType::UInt64 => {
            let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            json!(array.value(index))
        }
        DataType::Float32 => {
            let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            json!(array.value(index))
        }
        DataType::Float64 => {
            let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            json!(array.value(index))
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            json!(array.value(index))
        }
        DataType::Date32 => {
            let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
            json!(array.value(index).to_string())
        }
        DataType::Date64 => {
            let array = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let ms = array.value(index);
            match chrono::NaiveDateTime::from_timestamp_millis(ms) {
                Some(dt) => json!(dt.format("%Y-%m-%d").to_string()),
                None => json!(ms.to_string()),
            }
        }
        DataType::Timestamp(time_unit, _) => {
            match time_unit {
                TimeUnit::Second => {
                    let array = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                    let ts = array.value(index);
                    match chrono::NaiveDateTime::from_timestamp_opt(ts, 0) {
                        Some(dt) => json!(dt.format("%Y-%m-%d %H:%M:%S").to_string()),
                        None => json!(ts.to_string()),
                    }
                }
                TimeUnit::Millisecond => {
                    let array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                    let ts = array.value(index);
                    match chrono::NaiveDateTime::from_timestamp_millis(ts) {
                        Some(dt) => json!(dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
                        None => json!(ts.to_string()),
                    }
                }
                TimeUnit::Microsecond => {
                    let array = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    let ts = array.value(index);
                    match chrono::NaiveDateTime::from_timestamp_micros(ts) {
                        Some(dt) => json!(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
                        None => json!(ts.to_string()),
                    }
                }
                TimeUnit::Nanosecond => {
                    let array = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                    let ts = array.value(index);
                    // 将纳秒转换为秒和纳秒部分
                    let seconds = ts / 1_000_000_000;
                    let nanos = (ts % 1_000_000_000) as u32;
                    match chrono::NaiveDateTime::from_timestamp_opt(seconds, nanos) {
                        Some(dt) => json!(dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string()),
                        None => json!(ts.to_string()),
                    }
                }
            }
        }
        _ => json!(format!("{:?}", array)),
    }
} 