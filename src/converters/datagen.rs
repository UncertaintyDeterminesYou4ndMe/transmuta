use crate::cli::{OutputFormat, SchemaFormat};
use crate::error::{Result, TransmutaError};
use std::path::Path;
use std::fs::File;
use std::io::BufReader;
use log::{info, debug};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::datatypes::IntervalMonthDayNano;
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
    // 字符串类型
    #[serde(rename = "string")]
    String,
    
    // 基本数值类型
    #[serde(rename = "integer")]
    Integer, // 向后兼容的通用整数类型
    #[serde(rename = "float")]
    Float,   // 向后兼容的通用浮点数类型
    #[serde(rename = "boolean")]
    Boolean,
    
    // 精确数值类型 - 整数
    #[serde(rename = "int8")]
    Int8,
    #[serde(rename = "int16")]
    Int16,
    #[serde(rename = "int32")]
    Int32,
    #[serde(rename = "int64")]
    Int64,
    #[serde(rename = "uint8")]
    UInt8,
    #[serde(rename = "uint16")]
    UInt16,
    #[serde(rename = "uint32")]
    UInt32,
    #[serde(rename = "uint64")]
    UInt64,
    
    // 精确数值类型 - 浮点数
    #[serde(rename = "float32")]
    Float32,
    #[serde(rename = "float64")]
    Float64,
    
    // 高精度数值类型
    #[serde(rename = "decimal")]
    Decimal,
    #[serde(rename = "decimal128")]
    Decimal128,
    #[serde(rename = "decimal256")]
    Decimal256,
    
    // 日期和时间类型
    #[serde(rename = "date")]
    Date,      // 向后兼容的日期类型
    #[serde(rename = "date32")]
    Date32,    // 天数表示的日期
    #[serde(rename = "timestamp")]
    Timestamp, // 向后兼容的时间戳类型
    #[serde(rename = "time32")]
    Time32,    // 秒或毫秒精度的时间
    #[serde(rename = "time64")]
    Time64,    // 微秒或纳秒精度的时间
    #[serde(rename = "interval")]
    Interval,  // 时间间隔
    #[serde(rename = "duration")]
    Duration,  // 持续时间
    
    // 二进制数据类型
    #[serde(rename = "binary")]
    Binary,         // 可变长二进制数据
    #[serde(rename = "fixedsizebinary")]
    FixedSizeBinary, // 固定长度二进制数据
    
    // 特殊类型
    #[serde(rename = "uuid")]
    Uuid,           // 通用唯一标识符
    #[serde(rename = "null")]
    Null,           // 空值类型
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
            // 基本类型
            "string" => DataType::String,
            "integer" => DataType::Integer,
            "float" | "double" => DataType::Float,
            "boolean" | "bool" => DataType::Boolean,
            
            // 精确整数类型
            "int8" | "tinyint" => DataType::Int8,
            "int16" | "smallint" => DataType::Int16,
            "int32" => DataType::Int32,
            "int" => DataType::Integer, // 将"int"映射到通用Integer类型
            "int64" | "bigint" => DataType::Int64,
            "uint8" | "utinyint" => DataType::UInt8,
            "uint16" | "usmallint" => DataType::UInt16,
            "uint32" | "uint" => DataType::UInt32,
            "uint64" | "ubigint" => DataType::UInt64,
            
            // 精确浮点数类型
            "float32" | "real" => DataType::Float32,
            "float64" | "double precision" => DataType::Float64,
            
            // 高精度数值类型
            "decimal" | "numeric" => DataType::Decimal,
            "decimal128" => DataType::Decimal128,
            "decimal256" => DataType::Decimal256,
            
            // 日期和时间类型
            "date" => DataType::Date,
            "date32" => DataType::Date32,
            "timestamp" => DataType::Timestamp,
            "time32" => DataType::Time32,
            "time64" => DataType::Time64,
            "interval" => DataType::Interval,
            "duration" => DataType::Duration,
            
            // 二进制数据类型
            "binary" | "varbinary" => DataType::Binary,
            "fixedsizebinary" => DataType::FixedSizeBinary,
            
            // 特殊类型
            "uuid" => DataType::Uuid,
            "null" => DataType::Null,
            
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

// 生成随机8位整数
fn generate_random_int8(rng: &mut StdRng) -> i8 {
    rng.gen_range(i8::MIN..=i8::MAX)
}

// 生成随机16位整数
fn generate_random_int16(rng: &mut StdRng) -> i16 {
    rng.gen_range(i16::MIN..=i16::MAX)
}

// 生成随机32位整数
fn generate_random_int32(rng: &mut StdRng) -> i32 {
    rng.gen_range(i32::MIN/2..=i32::MAX/2) // 使用一半范围以避免极端值
}

// 生成随机64位整数
fn generate_random_int64(rng: &mut StdRng) -> i64 {
    rng.gen_range(i64::MIN/1000..=i64::MAX/1000) // 使用较小范围以避免极端值
}

// 生成随机无符号8位整数
fn generate_random_uint8(rng: &mut StdRng) -> u8 {
    rng.gen()
}

// 生成随机无符号16位整数
fn generate_random_uint16(rng: &mut StdRng) -> u16 {
    rng.gen()
}

// 生成随机无符号32位整数
fn generate_random_uint32(rng: &mut StdRng) -> u32 {
    rng.gen_range(0..=u32::MAX/2) // 使用一半范围以避免极端值
}

// 生成随机无符号64位整数
fn generate_random_uint64(rng: &mut StdRng) -> u64 {
    rng.gen_range(0..=u64::MAX/1000) // 使用较小范围以避免极端值
}

// 生成随机32位浮点数
fn generate_random_float32(rng: &mut StdRng) -> f32 {
    rng.gen_range(-1000.0..=1000.0)
}

// 生成随机64位浮点数
fn generate_random_float64(rng: &mut StdRng) -> f64 {
    rng.gen_range(-1000000.0..=1000000.0)
}

// 生成随机小数（使用字符串表示，模拟Decimal类型）
fn generate_random_decimal(rng: &mut StdRng, precision: usize) -> String {
    let whole_part = rng.gen_range(0..10000);
    let decimal_part = rng.gen_range(0..10u32.pow(precision as u32));
    format!("{}.{:0width$}", whole_part, decimal_part, width = precision)
}

// 生成随机日期（从2000-01-01到现在）
fn generate_random_date(rng: &mut StdRng) -> i32 {
    // 2000-01-01对应的天数
    let min_days = 10957;
    // 当前日期对应的天数（近似值）
    let max_days = 19000;
    rng.gen_range(min_days..=max_days)
}

// 生成随机32位日期（从2000-01-01到现在）
fn generate_random_date32(rng: &mut StdRng) -> i32 {
    generate_random_date(rng)
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

// 生成随机时间（32位，秒或毫秒精度）
fn generate_random_time32(rng: &mut StdRng, is_millis: bool) -> i32 {
    if is_millis {
        // 毫秒精度，范围为0到86400000（一天的毫秒数）
        rng.gen_range(0..86400000)
    } else {
        // 秒精度，范围为0到86400（一天的秒数）
        rng.gen_range(0..86400)
    }
}

// 生成随机时间（64位，微秒或纳秒精度）
fn generate_random_time64(rng: &mut StdRng, is_nanos: bool) -> i64 {
    if is_nanos {
        // 纳秒精度，范围为0到86400000000000（一天的纳秒数）
        rng.gen_range(0..86400000000000)
    } else {
        // 微秒精度，范围为0到86400000000（一天的微秒数）
        rng.gen_range(0..86400000000)
    }
}

// 生成随机时间间隔
fn generate_random_interval(rng: &mut StdRng) -> IntervalMonthDayNano {
    // 月，日，毫秒
    let months = rng.gen_range(-1200..1200); // -100年到+100年
    let days = rng.gen_range(-3650..3650);   // -10年到+10年
    let millis = rng.gen_range(-86400000..86400000); // -1天到+1天
    // 将毫秒转换为纳秒
    let nanos = millis as i64 * 1_000_000;
    IntervalMonthDayNano::new(months, days, nanos)
}

// 生成随机持续时间（纳秒）
fn generate_random_duration(rng: &mut StdRng) -> i64 {
    // 生成从0到约1年的纳秒
    rng.gen_range(0..31536000000000000)
}

// 生成随机二进制数据
fn generate_random_binary(rng: &mut StdRng, min_len: usize, max_len: usize) -> Vec<u8> {
    let len = rng.gen_range(min_len..=max_len);
    let mut bytes = vec![0u8; len];
    rng.fill(&mut bytes[..]);
    bytes
}

// 生成固定大小的随机二进制数据
fn generate_random_fixed_size_binary(rng: &mut StdRng, size: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; size];
    rng.fill(&mut bytes[..]);
    bytes
}

// 生成随机UUID
fn generate_random_uuid(rng: &mut StdRng) -> String {
    use std::fmt::Write;
    
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes[..]);
    
    // 设置版本(v4)和变体位
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // 版本4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // 变体1
    
    let mut uuid = String::with_capacity(36);
    
    for (i, b) in bytes.iter().enumerate() {
        // 在特定位置添加连字符
        if i == 4 || i == 6 || i == 8 || i == 10 {
            uuid.push('-');
        }
        write!(&mut uuid, "{:02x}", b).unwrap();
    }
    
    uuid
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
                // 基本类型
                DataType::String => arrow::datatypes::DataType::Utf8,
                DataType::Integer => arrow::datatypes::DataType::Int32,
                DataType::Float => arrow::datatypes::DataType::Float64,
                DataType::Boolean => arrow::datatypes::DataType::Boolean,
                
                // 精确整数类型
                DataType::Int8 => arrow::datatypes::DataType::Int8,
                DataType::Int16 => arrow::datatypes::DataType::Int16,
                DataType::Int32 => arrow::datatypes::DataType::Int32,
                DataType::Int64 => arrow::datatypes::DataType::Int64,
                DataType::UInt8 => arrow::datatypes::DataType::UInt8,
                DataType::UInt16 => arrow::datatypes::DataType::UInt16,
                DataType::UInt32 => arrow::datatypes::DataType::UInt32,
                DataType::UInt64 => arrow::datatypes::DataType::UInt64,
                
                // 精确浮点数类型
                DataType::Float32 => arrow::datatypes::DataType::Float32,
                DataType::Float64 => arrow::datatypes::DataType::Float64,
                
                // 高精度数值类型 (用字符串表示)
                DataType::Decimal | DataType::Decimal128 | DataType::Decimal256 => arrow::datatypes::DataType::Utf8,
                
                // 日期和时间类型
                DataType::Date => arrow::datatypes::DataType::Date32,
                DataType::Date32 => arrow::datatypes::DataType::Date32,
                DataType::Timestamp => arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Time32 => arrow::datatypes::DataType::Time32(TimeUnit::Millisecond),
                DataType::Time64 => arrow::datatypes::DataType::Time64(TimeUnit::Nanosecond),
                DataType::Interval => arrow::datatypes::DataType::Interval(IntervalUnit::MonthDayNano),
                DataType::Duration => arrow::datatypes::DataType::Duration(TimeUnit::Nanosecond),
                
                // 二进制数据类型
                DataType::Binary => arrow::datatypes::DataType::Binary,
                DataType::FixedSizeBinary => arrow::datatypes::DataType::FixedSizeBinary(16), // 默认16字节
                
                // 特殊类型
                DataType::Uuid => arrow::datatypes::DataType::Utf8,
                DataType::Null => arrow::datatypes::DataType::Null,
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
            // 基本类型
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
            
            // 精确整数类型
            DataType::Int8 => {
                let mut builder = Int8Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_int8(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Int16 => {
                let mut builder = Int16Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_int16(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Int32 => {
                let mut builder = Int32Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_int32(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_int64(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::UInt8 => {
                let mut builder = UInt8Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_uint8(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::UInt16 => {
                let mut builder = UInt16Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_uint16(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::UInt32 => {
                let mut builder = UInt32Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_uint32(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::UInt64 => {
                let mut builder = UInt64Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_uint64(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            
            // 精确浮点数类型
            DataType::Float32 => {
                let mut builder = Float32Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_float32(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Float64 => {
                let mut builder = Float64Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_float64(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            
            // 高精度数值类型 (用字符串表示)
            DataType::Decimal | DataType::Decimal128 | DataType::Decimal256 => {
                let mut builder = StringBuilder::new();
                for _ in 0..rows {
                    builder.append_value(&generate_random_decimal(&mut rng, 6));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            
            // 日期和时间类型
            DataType::Date => {
                let mut builder = Date32Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_date(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Date32 => {
                let mut builder = Date32Builder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_date32(&mut rng));
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
            DataType::Time32 => {
                let mut builder = Time32MillisecondBuilder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_time32(&mut rng, true)); // 使用毫秒精度
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Time64 => {
                let mut builder = Time64NanosecondBuilder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_time64(&mut rng, true)); // 使用纳秒精度
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Interval => {
                // Interval类型使用三个整数表示：月、日、纳秒
                let mut builder = IntervalMonthDayNanoBuilder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_interval(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Duration => {
                let mut builder = DurationNanosecondBuilder::new();
                for _ in 0..rows {
                    builder.append_value(generate_random_duration(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            
            // 二进制数据类型
            DataType::Binary => {
                let mut builder = BinaryBuilder::new();
                for _ in 0..rows {
                    builder.append_value(&generate_random_binary(&mut rng, 4, 20));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::FixedSizeBinary => {
                let mut builder = FixedSizeBinaryBuilder::new(16); // 默认16字节
                for _ in 0..rows {
                    let data = generate_random_fixed_size_binary(&mut rng, 16);
                    builder.append_value(&data).unwrap();
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            
            // 特殊类型
            DataType::Uuid => {
                let mut builder = StringBuilder::new();
                for _ in 0..rows {
                    builder.append_value(&generate_random_uuid(&mut rng));
                }
                arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
            },
            DataType::Null => {
                // Null类型，所有值都是null
                let mut builder = NullBuilder::new();
                for _ in 0..rows {
                    builder.append_null();
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