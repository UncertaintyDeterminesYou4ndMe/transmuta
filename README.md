# Transmuta 数据格式转换工具

Transmuta是一个轻量级、高效的数据文件格式转换工具，用于在各种数据格式之间安全地进行本地转换。

## 特性

- 支持多种格式转换：
  - Excel (.xlsx, .xls, .xlsm) → CSV / JSON / Parquet
  - CSV → JSON / Parquet
- 批量处理：可以控制每批次处理的数据量，避免内存溢出
- 多线程处理：优化性能，加快处理速度
- 本地处理：所有数据在本地处理，保证数据安全
- 进度监控：实时显示转换进度

## 安装

### 从源码编译

1. 确保已安装Rust工具链：https://www.rust-lang.org/tools/install
2. 克隆此仓库：`git clone <仓库地址>`
3. 进入项目目录：`cd transmuta`
4. 编译项目：`cargo build --release`
5. 可执行文件将在 `target/release` 目录下

### 预编译二进制文件

从 [Releases](https://github.com/yourname/transmuta/releases) 页面下载预编译的二进制文件。

## 使用方法

### 基本用法

```bash
# 将Excel文件转换为CSV
transmuta excel -i input.xlsx -o output.csv -f csv

# 将Excel文件转换为JSON
transmuta excel -i input.xlsx -o output.json -f json

# 将Excel文件转换为Parquet
transmuta excel -i input.xlsx -o output.parquet -f parquet

# 将CSV文件转换为JSON
transmuta csv -i input.csv -o output.json -f json

# 将CSV文件转换为Parquet
transmuta csv -i input.csv -o output.parquet -f parquet
```

### 高级选项

```bash
# 使用自定义分隔符处理CSV
transmuta csv -i input.csv -o output.parquet -f parquet -d ";"

# 控制批处理大小
transmuta excel -i large_file.xlsx -o output.csv -f csv -b 5000

# 指定使用的线程数
transmuta excel -i large_file.xlsx -o output.json -f json -t 4

# 跳过Excel文件的前几行
transmuta excel -i data.xlsx -o output.csv -f csv --skip-rows 2

# 指定CSV文件没有标题行
transmuta csv -i data.csv -o output.json -f json --has-header false
```

### 完整帮助信息

使用 `-h` 或 `--help` 参数查看完整帮助信息：

```bash
transmuta --help
transmuta excel --help
transmuta csv --help
```

## 注意事项

- 处理大型文件时，请适当调整批处理大小(`-b`参数)以优化内存使用
- 默认情况下，工具将使用所有可用的CPU核心进行处理，可以使用`-t`参数限制线程数
- 当转换大文件时，结果可能会被分割成多个批次文件

## 许可证

MIT

## 贡献

欢迎贡献代码、报告问题或提出建议！