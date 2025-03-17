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

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// 转换Excel文件
    Excel {
        /// 输入Excel文件路径（支持.xlsx、.xls、.xlsm格式）
        #[arg(short, long, value_name = "EXCEL_FILE")]
        input: PathBuf,
        
        /// 输出文件路径（根据--format参数决定输出格式）
        #[arg(short, long, value_name = "OUTPUT_FILE")]
        output: PathBuf,
        
        /// 输出格式（csv、json或parquet）
        #[arg(short, long, value_enum)]
        format: OutputFormat,
        
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
        
        /// 输出文件路径（根据--format参数决定输出格式）
        #[arg(short, long, value_name = "OUTPUT_FILE")]
        output: PathBuf,
        
        /// 输出格式（csv、json或parquet）
        #[arg(short, long, value_enum)]
        format: OutputFormat,
        
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
} 