# Transmuta（传变）

一个用于数据文件格式转换的命令行工具，支持Excel、CSV、JSON和Parquet等格式之间的互相转换。

## 功能特点

- 支持Excel（xlsx、xls、xlsm）、CSV、JSON和Parquet格式的互相转换
- 批量处理大型文件，自动分片处理
- 提供进度显示
- 支持多线程处理
- 自动类型推断
- 支持生成随机测试数据

## 安装

```bash
cargo install transmuta
```

或者从源码编译：

```bash
git clone https://github.com/yourusername/transmuta.git
cd transmuta
cargo build --release
```

## 使用方法

### Excel转换

```bash
transmuta excel --input data.xlsx --output data.csv --format csv
transmuta excel --input data.xlsx --output data.json --format json
transmuta excel --input data.xlsx --output data.parquet --format parquet
```

支持的选项：
- `--delimiter`：CSV分隔符，默认为`,`
- `--batch-size`：批处理大小，默认10000行
- `--threads`：线程数，默认为CPU核心数
- `--skip-rows`：跳过前几行，默认为0

### CSV转换

```bash
transmuta csv --input data.csv --output data.json --format json
transmuta csv --input data.csv --output data.parquet --format parquet
```

支持的选项：
- `--delimiter`：CSV分隔符，默认为`,`
- `--batch-size`：批处理大小，默认10000行
- `--threads`：线程数，默认为CPU核心数
- `--has-header`：是否有标题行，默认为true

### 数据生成

生成随机数据，需要提供列定义文件（CSV或JSON格式）：

```bash
transmuta data-gen --schema schema.csv --schema-format csv --output data.csv --format csv
transmuta data-gen -s schema.json -f json -o data.json -m json
```

支持的选项：
- `-s, --schema`：列定义文件路径
- `-f, --schema-format`：列定义文件格式（csv或json）
- `-o, --output`：输出文件路径
- `-m, --format`：输出格式（csv、json或parquet）
- `-r, --rows`：生成的行数，默认为1000
- `-d, --delimiter`：CSV分隔符，默认为`,`
- `--seed`：随机数种子，用于生成可重复的随机数据

#### 列定义格式

CSV格式的列定义（每行包含列名和数据类型）：
```
姓名,string
年龄,integer
工资,float
在职,boolean
入职日期,date
最后登录,timestamp
```

JSON格式的列定义：
```json
[
  {
    "name": "姓名",
    "data_type": "string"
  },
  {
    "name": "年龄",
    "data_type": "integer"
  }
]
```

支持的数据类型：
- `string`：字符串
- `integer` / `int`：通用整数类型（向后兼容）
- `float` / `double`：通用浮点数类型（向后兼容）
- `boolean` / `bool`：布尔值
- `date`：日期
- `timestamp`：时间戳

精确数值类型：
- `int8` / `tinyint`：8位有符号整数，范围 -128 到 127
- `int16` / `smallint`：16位有符号整数，范围 -32,768 到 32,767
- `int32` / `int`：32位有符号整数
- `int64` / `bigint`：64位有符号整数
- `uint8` / `utinyint`：8位无符号整数，范围 0 到 255
- `uint16` / `usmallint`：16位无符号整数，范围 0 到 65,535
- `uint32` / `uint`：32位无符号整数
- `uint64` / `ubigint`：64位无符号整数
- `float32` / `real`：32位单精度浮点数
- `float64` / `double precision`：64位双精度浮点数
- `decimal` / `numeric`：高精度小数（使用字符串存储）
- `decimal128`：128位高精度小数
- `decimal256`：256位高精度小数

时间类型：
- `date32`：天数表示的日期（从UNIX纪元开始的天数）
- `time32`：秒或毫秒精度的时间（一天内的时间）
- `time64`：微秒或纳秒精度的时间
- `interval`：时间间隔（月、日、纳秒）
- `duration`：持续时间（纳秒）

二进制和特殊类型：
- `binary` / `varbinary`：可变长度二进制数据
- `fixedsizebinary`：固定长度二进制数据（默认16字节）
- `uuid`：通用唯一标识符
- `null`：空值类型

## 许可证

MIT License

## 贡献

欢迎提交问题和PR！