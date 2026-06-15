use clap::{Parser, Subcommand, ValueEnum};
use std::path::{PathBuf, Path};

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

/// 可作为转换「源」的格式（注意：parquet 暂不支持作为源读取）
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InputFormat {
    /// Excel 工作簿（.xlsx/.xls/.xlsm/.xlsb）
    Excel,
    /// CSV 文本
    Csv,
    /// JSON（对象或对象数组）
    Json,
}

impl std::fmt::Display for InputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputFormat::Excel => write!(f, "excel"),
            InputFormat::Csv => write!(f, "csv"),
            InputFormat::Json => write!(f, "json"),
        }
    }
}

/// 从输入文件扩展名推断源格式
pub fn guess_input_format_from_extension(path: &Path) -> Option<InputFormat> {
    path.extension().and_then(|ext| {
        match ext.to_string_lossy().to_lowercase().as_str() {
            "xlsx" | "xls" | "xlsm" | "xlsb" => Some(InputFormat::Excel),
            "csv" | "tsv" => Some(InputFormat::Csv),
            "json" => Some(InputFormat::Json),
            _ => None,
        }
    })
}

#[derive(Debug, Clone, ValueEnum)]
pub enum SchemaFormat {
    /// CSV格式的列定义
    Csv,
    /// JSON格式的列定义
    Json,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum DiffOutputMode {
    /// 并集：两个文件中所有的字段
    Union,
    /// 补集：在文件1或文件2中出现但不同时出现在两个文件中的字段
    Complement,
    /// 差集（以文件1为基准）：将文件2独有的字段添加到文件1
    DiffBasedOnFile1,
    /// 差集（以文件2为基准）：将文件1独有的字段添加到文件2
    DiffBasedOnFile2,
    /// 仅保留文件1独有的字段
    OnlyInFile1,
    /// 仅保留文件2独有的字段
    OnlyInFile2,
    /// 原文件1的所有字段（按字母排序）
    SortedFile1,
    /// 原文件2的所有字段（按字母排序）
    SortedFile2,
}

#[derive(Parser, Debug)]
#[command(
    name = "transmuta",
    about = "纯本地、离线的数据文件格式转换工具（Excel/CSV/JSON/Parquet）",
    version,
    author,
    long_about = "transmuta —— 纯本地、离线、无网络依赖的数据文件格式转换 CLI。\n\n\
        无需编写脚本或配置语言环境：一条命令即可在 Excel、CSV、JSON、Parquet 之间转换，\n\
        源/目标格式默认从文件扩展名自动推断。\n\n\
        可作为「源」的格式：excel(.xlsx/.xls/.xlsm/.xlsb)、csv/tsv、json\n\
        可作为「目标」的格式：csv、json、parquet",
    after_help = "常用示例：\n  \
        transmuta convert data.xlsx data.csv      # Excel → CSV\n  \
        transmuta convert data.csv data.parquet   # CSV → Parquet\n  \
        transmuta convert data.json data.csv      # JSON → CSV\n  \
        transmuta diff a.csv b.csv -o diff.json   # 比较两个文件的字段差异\n  \
        transmuta data-gen -s schema.csv -m csv -o out.csv  # 生成随机测试数据\n\n\
        用 `transmuta <子命令> -h` 查看某个子命令的完整用法与更多示例。"
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
                "xlsx" | "xls" | "xlsm" | "xlsb" => Some(OutputFormat::Csv), // Excel文件默认转为CSV格式
                _ => None,
            }
        })
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// 在数据文件格式之间转换（源/目标格式默认从文件扩展名推断）
    #[command(
        long_about = "在数据文件格式之间转换。\n\n\
            可作为「源」的格式：excel(.xlsx/.xls/.xlsm/.xlsb)、csv/tsv、json\n\
            可作为「目标」的格式：csv、json、parquet\n\n\
            源格式默认从 INPUT 扩展名推断，目标格式默认从 OUTPUT 扩展名推断；\n\
            扩展名不足以判断时用 --from / --to 显式指定。",
        after_help = "示例：\n  \
            # 扩展名即可推断两端格式\n  \
            transmuta convert data.xlsx data.csv\n  \
            transmuta convert data.csv data.parquet\n  \
            transmuta convert data.json data.csv\n\n  \
            # 输出 TSV（制表符分隔）\n  \
            transmuta convert data.json out.tsv -d '\\t'\n\n  \
            # 扩展名无法推断时显式指定\n  \
            transmuta convert in.dat out.dat --from csv --to json\n\n  \
            # Excel 跳过前 2 行说明文字；源 CSV 无表头\n  \
            transmuta convert report.xlsx report.csv --skip-rows 2\n  \
            transmuta convert raw.csv raw.json --no-header"
    )]
    Convert {
        /// 输入文件路径（源格式默认从扩展名推断）
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// 输出文件路径（目标格式默认从扩展名推断）
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,

        /// 源格式，覆盖从 INPUT 扩展名的推断（excel、csv、json）
        #[arg(long, value_enum)]
        from: Option<InputFormat>,

        /// 目标格式，覆盖从 OUTPUT 扩展名的推断（csv、json、parquet）
        #[arg(long, value_enum)]
        to: Option<OutputFormat>,

        /// 批处理大小，一次处理的记录数（较大可能提速但更耗内存）
        #[arg(short, long, default_value = "10000")]
        batch_size: usize,

        /// CSV/TSV 分隔符，支持特殊字符如 \t（制表符）
        #[arg(short, long, default_value = ",", value_parser = parse_delimiter)]
        delimiter: char,

        /// 使用的线程数，默认为 CPU 核心数
        #[arg(short, long)]
        threads: Option<usize>,

        /// 跳过开头 N 行（仅 Excel 源生效，用于跳过说明性文字行）
        #[arg(long, default_value = "0")]
        skip_rows: usize,

        /// 源 CSV 没有表头行（默认认为有表头）
        #[arg(long)]
        no_header: bool,
    },

    /// 生成随机数据
    DataGen {
        /// 列定义文件路径（CSV或JSON格式）
        #[arg(short, long, value_name = "SCHEMA_FILE")]
        schema: PathBuf,
        
        /// 列定义文件格式（csv或json）
        #[arg(short = 'm', long, value_enum)]
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
    
    /// 比较两个文件的字段差异
    Diff {
        /// 输入文件1路径
        #[arg(short = '1', long, value_name = "FILE1")]
        input1: PathBuf,
        
        /// 输入文件2路径
        #[arg(short = '2', long, value_name = "FILE2")]
        input2: PathBuf,
        
        /// 输出文件路径
        #[arg(short, long, value_name = "OUTPUT_FILE")]
        output: PathBuf,
        
        /// 差异输出模式
        #[arg(short = 'm', long, value_enum, default_value = "union")]
        mode: DiffOutputMode,
        
        /// 字段分隔符，支持特殊字符如\t表示制表符
        #[arg(short = 'd', long, default_value = ",", value_parser = parse_delimiter)]
        delimiter: char,
        
        /// 输出详细的差异报告到指定文件
        #[arg(short = 'r', long, value_name = "REPORT_FILE")]
        report: Option<PathBuf>,
        
        /// 忽略大小写差异
        #[arg(short = 'i', long)]
        ignore_case: bool,
        
        /// 忽略空白字符差异
        #[arg(short = 'w', long)]
        ignore_whitespace: bool,
        
        /// 将每行作为一个单独的字段读取（适用于每个字段占一行的文件）
        #[arg(short = 'l', long)]
        one_field_per_line: bool,
    },
} 