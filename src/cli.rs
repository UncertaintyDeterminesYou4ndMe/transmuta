use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    /// CSV格式
    Csv,
    /// JSON格式
    Json,
    /// Parquet格式
    Parquet,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Csv => write!(f, "csv"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::Parquet => write!(f, "parquet"),
        }
    }
}

#[derive(Debug, Clone, ValueEnum)]
pub enum SchemaFormat {
    /// CSV格式的列定义
    Csv,
    /// JSON格式的列定义
    Json,
}

#[derive(Parser, Debug)]
#[command(
    name = "transmuta",
    about = "用于数据文件格式转换的工具",
    version,
    author,
    long_about = "一个安全的本地数据文件格式转换工具，支持Excel、CSV、JSON和Parquet等格式之间的转换"
)]
pub struct Cli {
    /// 日志级别: trace, debug, info, warn, error
    #[arg(short, long, default_value = "info")]
    pub log_level: String,

    #[command(subcommand)]
    pub command: Commands,
}

/// 解析分隔符字符串，支持特殊字符
pub fn parse_delimiter(s: &str) -> Result<char, String> {
    match s {
        r"\t" => Ok('\t'),  // 制表符
        r"\n" => Ok('\n'),  // 换行符
        r"\r" => Ok('\r'),  // 回车符
        _ => {
            if s.chars().count() != 1 {
                Err(format!("分隔符必须是单个字符，或特殊符号如\\t（制表符）、\\n（换行符）等"))
            } else {
                Ok(s.chars().next().unwrap())
            }
        }
    }
}

// 从文件扩展名推断输出格式
pub fn guess_format_from_extension(path: &Path) -> Option<OutputFormat> {
    path.extension()
        .and_then(|ext| {
            let ext = ext.to_string_lossy().to_lowercase();
            match ext.as_str() {
                "csv" => Some(OutputFormat::Csv),
                "json" => Some(OutputFormat::Json),
                "parquet" => Some(OutputFormat::Parquet),
                _ => None,
            }
        })
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// 转换Excel文件
    Excel {
        /// 输入Excel文件路径（支持.xlsx、.xls、.xlsm格式）
        #[arg(short, long, value_name = "EXCEL_FILE")]
        input: PathBuf,
        
        /// 输出文件路径（如果不指定--format，将从文件扩展名推断输出格式）
        #[arg(short, long, value_name = "OUTPUT_FILE")]
        output: PathBuf,
        
        /// 输出格式（csv、json或parquet），如不指定则从输出文件扩展名推断
        #[arg(short, long, value_enum)]
        format: Option<OutputFormat>,
        
        /// 批处理大小，指定一次处理的行数（较大的值可能提高性能但增加内存使用）
        #[arg(short, long, default_value = "10000")]
        batch_size: usize,
        
        /// CSV分隔符（当输出为CSV时使用），支持特殊字符如\t表示制表符
        #[arg(short, long, default_value = ",", value_parser = parse_delimiter)]
        delimiter: char,
        
        /// 使用的线程数，默认为CPU核心数
        #[arg(short, long)]
        threads: Option<usize>,
        
        /// 跳过前几行（例如标题行）
        #[arg(long, default_value = "0")]
        skip_rows: usize,
    },
    
    /// 转换CSV文件
    Csv {
        /// 输入CSV文件路径
        #[arg(short, long, value_name = "CSV_FILE")]
        input: PathBuf,
        
        /// 输出文件路径（如果不指定--format，将从文件扩展名推断输出格式）
        #[arg(short, long, value_name = "OUTPUT_FILE")]
        output: PathBuf,
        
        /// 输出格式（csv、json或parquet），如不指定则从输出文件扩展名推断
        #[arg(short, long, value_enum)]
        format: Option<OutputFormat>,
        
        /// 批处理大小，指定一次处理的行数（较大的值可能提高性能但增加内存使用）
        #[arg(short, long, default_value = "10000")]
        batch_size: usize,
        
        /// CSV分隔符，支持特殊字符如\t表示制表符
        #[arg(short, long, default_value = ",", value_parser = parse_delimiter)]
        delimiter: char,
        
        /// 使用的线程数，默认为CPU核心数
        #[arg(short, long)]
        threads: Option<usize>,
        
        /// CSV是否有标题行
        #[arg(long, default_value = "true")]
        has_header: bool,
    },
    
    /// 生成随机数据
    DataGen {
        /// 列定义文件路径（CSV或JSON格式）
        #[arg(short, long, value_name = "SCHEMA_FILE")]
        schema: PathBuf,
        
        /// 列定义文件格式（csv或json）
        #[arg(short, long, value_enum)]
        schema_format: SchemaFormat,
        
        /// 输出文件路径（如果不指定--format，将从文件扩展名推断输出格式）
        #[arg(short, long, value_name = "OUTPUT_FILE")]
        output: PathBuf,
        
        /// 输出格式（csv、json或parquet），如不指定则从输出文件扩展名推断
        #[arg(short, long, value_enum)]
        format: Option<OutputFormat>,
        
        /// 生成的行数
        #[arg(short, long, default_value = "1000")]
        rows: usize,
        
        /// CSV分隔符（当输入或输出为CSV时使用），支持特殊字符如\t表示制表符
        #[arg(short, long, default_value = ",", value_parser = parse_delimiter)]
        delimiter: char,
        
        /// 随机数据种子，用于生成可重复的随机数据，默认为当前时间
        #[arg(long)]
        seed: Option<u64>,
    },
} 