use crate::cli::{OutputFormat, SchemaFormat};
use crate::error::{Result, TransmutaError};
use std::path::Path;
use std::fs::File;
use std::io::BufReader;
use log::{info, debug};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use rand::{Rng, SeedableRng};
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use std::time::{SystemTime, UNIX_EPOCH};

// 支持的数据类型
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DataType {
    #[serde(rename = "string")]
    String,
    #[serde(rename = "integer")]
    Integer,
    #[serde(rename = "float")]
    Float,
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "date")]
    Date,
    #[serde(rename = "timestamp")]
    Timestamp,
}

// 列定义
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
}

// 从CSV文件读取列定义
fn read_schema_from_csv(path: &Path, delimiter: char) -> Result<Vec<ColumnDefinition>> {
    info!("从CSV文件读取列定义: {}", path.display());
    
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    
    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(delimiter as u8)
        .has_headers(false)
        .from_reader(reader);
    
    let mut column_defs = Vec::new();
    
    for (row_idx, result) in csv_reader.records().enumerate() {
        let record = result?;
        if record.len() < 2 {
            return Err(TransmutaError::DataProcessingError(format!(
                "第{}行格式错误，需要至少包含列名和数据类型两列", row_idx + 1
            )));
        }
        
        let name = record[0].trim().to_string();
        let type_str = record[1].trim().to_lowercase();
        
        let data_type = match type_str.as_str() {
            "string" => DataType::String,
            "integer" | "int" => DataType::Integer,
            "float" | "double" => DataType::Float,
            "boolean" | "bool" => DataType::Boolean,
            "date" => DataType::Date,
            "timestamp" => DataType::Timestamp,
            _ => return Err(TransmutaError::DataProcessingError(format!(
                "第{}行不支持的数据类型: {}", row_idx + 1, type_str
            ))),
        };
        
        column_defs.push(ColumnDefinition { name, data_type });
    }
    
    if column_defs.is_empty() {
        return Err(TransmutaError::DataProcessingError("列定义为空".to_string()));
    }
    
    Ok(column_defs)
}

// 从JSON文件读取列定义
fn read_schema_from_json(path: &Path) -> Result<Vec<ColumnDefinition>> {
    info!("从JSON文件读取列定义: {}", path.display());
    
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    
    let column_defs: Vec<ColumnDefinition> = serde_json::from_reader(reader)?;
    
    if column_defs.is_empty() {
        return Err(TransmutaError::DataProcessingError("列定义为空".to_string()));
    }
    
    Ok(column_defs)
}

// 获取当前时间戳作为默认种子
fn get_default_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// 生成随机字符串
fn generate_random_string(rng: &mut StdRng, min_len: usize, max_len: usize) -> String {
    let len = rng.gen_range(min_len..=max_len);
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

// 生成随机整数
fn generate_random_integer(rng: &mut StdRng, min: i32, max: i32) -> i32 {
    rng.gen_range(min..=max)
}

// 生成随机浮点数
fn generate_random_float(rng: &mut StdRng, min: f64, max: f64) -> f64 {
    rng.gen_range(min..=max)
}

// 生成随机布尔值
fn generate_random_boolean(rng: &mut StdRng) -> bool {
    rng.gen()
}

// 生成随机日期（从2000-01-01到现在）
fn generate_random_date(rng: &mut StdRng) -> i32 {
    // 2000-01-01对应的天数
    let min_days = 10957;
    // 当前日期对应的天数（近似值）
    let max_days = 19000;
    rng.gen_range(min_days..=max_days)
}

// 生成随机时间戳（从2000-01-01到现在，毫秒级）
fn generate_random_timestamp(rng: &mut StdRng) -> i64 {
    // 2000-01-01 00:00:00对应的毫秒数
    let min_ms = 946684800000;
    // 当前时间对应的毫秒数
    let max_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    rng.gen_range(min_ms..=max_ms)
}

/// 根据列定义生成随机数据
pub fn generate_data(
    schema_path: &Path,
    schema_format: &SchemaFormat,
    output_path: &Path,
    format: &OutputFormat,
    rows: usize,
    delimiter: char,
    seed: Option<u64>,
) -> Result<()> {
    // 读取列定义
    let column_defs = match schema_format {
        SchemaFormat::Csv => read_schema_from_csv(schema_path, delimiter)?,
        SchemaFormat::Json => read_schema_from_json(schema_path)?,
    };
    
    info!("读取了{}个列定义", column_defs.len());
    for (i, col) in column_defs.iter().enumerate() {
        debug!("列 {}: {} ({})", i + 1, col.name, format!("{:?}", col.data_type));
    }
    
    // 创建Arrow Schema
    let fields: Vec<Field> = column_defs.iter()
        .map(|col| {
            let arrow_type = match col.data_type {
                DataType::String => arrow::datatypes::DataType::Utf8,
                DataType::Integer => arrow::datatypes::DataType::Int32,
                DataType::Float => arrow::datatypes::DataType::Float64,
                DataType::Boolean => arrow::datatypes::DataType::Boolean,
                DataType::Date => arrow::datatypes::DataType::Date32,
                DataType::Timestamp => arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, None),
            };
            Field::new(&col.name, arrow_type, true)
        })
        .collect();
    
    let schema = Arc::new(Schema::new(fields));
    
    // 初始化随机数生成器
    let seed_value = seed.unwrap_or_else(get_default_seed);
    info!("使用随机种子: {}", seed_value);
    let mut rng = StdRng::seed_from_u64(seed_value);
    
    // 创建并填充数组
    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
    
    for col in &column_defs {
        match col.data_type {
            DataType::String => {
                let mut builder = StringBuilder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_string(&mut rng, 5, 20));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Integer => {
                let mut builder = Int32Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_integer(&mut rng, -1000, 1000));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Float => {
                let mut builder = Float64Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_float(&mut rng, -1000.0, 1000.0));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Boolean => {
                let mut builder = BooleanBuilder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_boolean(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Date => {
                let mut builder = Date32Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_date(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Timestamp => {
                let mut builder = TimestampMillisecondBuilder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_timestamp(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
        }
    }
    
    // 创建RecordBatch
    let record_batch = RecordBatch::try_new(schema, arrays)?;
    
    info!("生成了{}行随机数据", rows);
    
    // 保存到指定格式
    super::common::save_data(&record_batch, output_path, format, delimiter)?;
    
    Ok(())
} 