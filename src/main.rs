mod cli;
mod converters;
mod error;
mod utils;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands};
use log::{error, info};

fn main() -> Result<()> {
    // 初始化日志
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let cli = Cli::parse();
    info!("传变工具 (transmuta) v{}", env!("CARGO_PKG_VERSION"));

    match cli.command {
        Commands::Excel { input, output, format, batch_size, delimiter, threads, skip_rows } => {
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
    }

    Ok(())
}
