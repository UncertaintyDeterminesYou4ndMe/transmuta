mod cli;
mod converters;
mod error;
mod utils;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands, OutputFormat, DiffOutputMode};
use log::{error, info, warn};
use std::path::Path;

// 获取输出格式，优先使用用户指定的格式，否则从文件扩展名推断
fn get_output_format(format_opt: Option<OutputFormat>, output_path: &Path) -> Result<OutputFormat, String> {
    match format_opt {
        // 用户明确指定了格式
        Some(format) => Ok(format),
        // 尝试从文件扩展名推断格式
        None => {
            cli::guess_format_from_extension(output_path)
                .ok_or_else(|| format!("无法从输出文件路径 '{}' 推断格式，请使用 --format 参数指定格式", 
                                      output_path.display()))
        }
    }
}

fn main() -> Result<()> {
    // 初始化日志
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let cli = Cli::parse();
    info!("传变工具 (transmuta) v{}", env!("CARGO_PKG_VERSION"));

    match cli.command {
        Commands::Excel { input, output, format, batch_size, delimiter, threads, skip_rows } => {
            // 获取输出格式，如果未指定则从文件扩展名推断
            let format = match get_output_format(format, &output) {
                Ok(f) => f,
                Err(e) => {
                    error!("{}", e);
                    return Err(anyhow::anyhow!(e));
                }
            };
            
            if let Err(e) = converters::excel::convert_excel(
                &input, 
                &output, 
                &format, 
                batch_size, 
                delimiter, 
                threads, 
                skip_rows
            ) {
                error!("转换Excel失败: {}", e);
                return Err(e.into());
            }
        }
        Commands::Csv { input, output, format, batch_size, delimiter, threads, has_header } => {
            // 获取输出格式，如果未指定则从文件扩展名推断
            let format = match get_output_format(format, &output) {
                Ok(f) => f,
                Err(e) => {
                    error!("{}", e);
                    return Err(anyhow::anyhow!(e));
                }
            };
            
            if let Err(e) = converters::csv::convert_csv(
                &input, 
                &output, 
                &format, 
                batch_size, 
                delimiter, 
                threads,
                has_header
            ) {
                error!("转换CSV失败: {}", e);
                return Err(e.into());
            }
        }
        Commands::DataGen { schema, schema_format, output, format, rows, delimiter, seed } => {
            // 获取输出格式，如果未指定则从文件扩展名推断
            let format = match get_output_format(format, &output) {
                Ok(f) => f,
                Err(e) => {
                    error!("{}", e);
                    return Err(anyhow::anyhow!(e));
                }
            };
            
            if let Err(e) = converters::datagen::generate_data(
                &schema,
                &schema_format,
                &output,
                &format,
                rows,
                delimiter,
                seed
            ) {
                error!("生成随机数据失败: {}", e);
                return Err(e.into());
            }
        }
        Commands::Diff { input1, input2, output, mode, delimiter, report, ignore_case, ignore_whitespace } => {
            if let Err(e) = converters::diff::diff_fields(
                &input1,
                &input2,
                &output,
                match mode {
                    cli::DiffOutputMode::Union => converters::diff::DiffOutputMode::Union,
                    cli::DiffOutputMode::Complement => converters::diff::DiffOutputMode::Complement,
                    cli::DiffOutputMode::DiffBasedOnFile1 => converters::diff::DiffOutputMode::DiffBasedOnFile1,
                    cli::DiffOutputMode::DiffBasedOnFile2 => converters::diff::DiffOutputMode::DiffBasedOnFile2,
                    cli::DiffOutputMode::OnlyInFile1 => converters::diff::DiffOutputMode::OnlyInFile1,
                    cli::DiffOutputMode::OnlyInFile2 => converters::diff::DiffOutputMode::OnlyInFile2,
                    cli::DiffOutputMode::SortFile1 => converters::diff::DiffOutputMode::SortFile1,
                    cli::DiffOutputMode::SortFile2 => converters::diff::DiffOutputMode::SortFile2,
                },
                converters::diff::DiffOptions {
                    delimiter,
                    ignore_case,
                    ignore_whitespace,
                    report_path: report.as_deref(),
                }
            ) {
                error!("比较字段差异失败: {}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}
