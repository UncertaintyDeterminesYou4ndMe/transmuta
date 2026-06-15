mod cli;
mod converters;
mod error;
mod utils;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands, InputFormat, OutputFormat};
use log::{error, info};
use std::path::Path;

// 获取输出格式，优先使用用户指定的格式，否则从文件扩展名推断
fn get_output_format(format_opt: Option<OutputFormat>, output_path: &Path) -> Result<OutputFormat, String> {
    match format_opt {
        // 用户明确指定了格式
        Some(format) => Ok(format),
        // 尝试从文件扩展名推断格式
        None => {
            cli::guess_format_from_extension(output_path)
                .ok_or_else(|| format!(
                    "无法从输出文件路径 '{}' 推断目标格式，请用 --to 指定（可选：csv、json、parquet）",
                    output_path.display()))
        }
    }
}

// 获取源格式，优先使用用户指定的格式，否则从输入文件扩展名推断
fn get_input_format(format_opt: Option<InputFormat>, input_path: &Path) -> Result<InputFormat, String> {
    match format_opt {
        Some(format) => Ok(format),
        None => {
            cli::guess_input_format_from_extension(input_path)
                .ok_or_else(|| format!(
                    "无法从输入文件路径 '{}' 推断源格式，请用 --from 指定（可选：excel、csv、json）",
                    input_path.display()))
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // 初始化日志：优先使用 RUST_LOG 环境变量，否则用 --log-level 的值（默认 info）。
    // 日志统一写入 stderr，stdout/输出文件保持纯净，便于脚本与 AI 解析。
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(cli.log_level.clone()),
    )
    .init();
    info!("传变工具 (transmuta) v{}", env!("CARGO_PKG_VERSION"));

    match cli.command {
        Commands::Convert { input, output, from, to, batch_size, delimiter, threads, skip_rows, no_header } => {
            // 推断源格式与目标格式
            let from = match get_input_format(from, &input) {
                Ok(f) => f,
                Err(e) => {
                    error!("{}", e);
                    return Err(anyhow::anyhow!(e));
                }
            };
            let to = match get_output_format(to, &output) {
                Ok(f) => f,
                Err(e) => {
                    error!("{}", e);
                    return Err(anyhow::anyhow!(e));
                }
            };

            info!("转换 {} ({}) → {} ({})", input.display(), from, output.display(), to);

            let result = match from {
                InputFormat::Excel => converters::excel::convert_excel(
                    &input, &output, &to, batch_size, delimiter, threads, skip_rows,
                ),
                InputFormat::Csv => converters::csv::convert_csv(
                    &input, &output, &to, batch_size, delimiter, threads, !no_header,
                ),
                InputFormat::Json => converters::json::convert_json(
                    &input, &output, &to, batch_size, delimiter, threads,
                ),
            };

            if let Err(e) = result {
                error!("转换失败（{} → {}）: {}", from, to, e);
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
        Commands::Diff { input1, input2, output, mode, delimiter, report, ignore_case, ignore_whitespace, one_field_per_line } => {
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
                    cli::DiffOutputMode::SortedFile1 => converters::diff::DiffOutputMode::SortedFile1,
                    cli::DiffOutputMode::SortedFile2 => converters::diff::DiffOutputMode::SortedFile2,
                },
                converters::diff::DiffOptions {
                    delimiter,
                    ignore_case,
                    ignore_whitespace,
                    report_path: report.as_deref(),
                    one_field_per_line,
                }
            ) {
                error!("比较字段差异失败: {}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}
