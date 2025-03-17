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
transmuta datagen --schema schema.csv --schema-format csv --output data.csv --format csv
transmuta datagen --schema schema.json --schema-format json --output data.json --format json
```

支持的选项：
- `--rows`：生成的行数，默认为1000
- `--delimiter`：CSV分隔符，默认为`,`
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
- `integer`：整数
- `float`：浮点数
- `boolean`：布尔值
- `date`：日期
- `timestamp`：时间戳

## 许可证

MIT License

## 贡献

欢迎提交问题和PR！