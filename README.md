# Transmuta（传变）

纯本地、离线、无网络依赖的数据文件格式转换 CLI，支持 Excel、CSV、JSON、Parquet 之间的互相转换。

**为什么做它**：需要转换格式时，不用再去配 Python 环境、写一次性脚本、反复调试——装上 `transmuta`，一条命令搞定。尤其适合给 AI / 自动化流程直接调用：`transmuta -h` 看一眼就能用。

## 功能特点

- Excel（`.xlsx/.xls/.xlsm/.xlsb`）、CSV/TSV、JSON 作为**源**，CSV、JSON、Parquet 作为**目标**
- 源 / 目标格式自动从文件扩展名推断，无需手填
- 全程离线，不联网、不上传任何数据
- 大文件分批处理、多线程、进度显示
- 附带文件差异比较（`diff`）与随机测试数据生成（`data-gen`）

## 安装

### 通过 npm（推荐）

```bash
# 全局安装
npm install -g transmuta
transmuta -h

# 或免安装直接用
npx transmuta convert data.xlsx data.csv
```

安装时 npm 只会拉取与你系统匹配的那个预编译二进制（macOS arm64/x64、Linux x64/arm64），**不需要安装期联网下载或编译**。

### 从源码编译

```bash
git clone https://github.com/UncertaintyDeterminesYou4ndMe/transmuta.git
cd transmuta
cargo build --release
# 产物在 target/release/transmuta
```

## 给 AI / 自动化使用

设计上对程序调用友好：

- **零交互**：所有输入通过参数传入，任何情况下都不会弹交互式提示阻塞。
- **自描述**：`transmuta -h` 与 `transmuta convert -h` 都带可直接复制运行的示例。
- **输出干净**：转换结果写入目标文件，日志一律走 stderr，stdout 不被污染；用 `-l warn` 或 `-l error` 可进一步降噪。
- **退出码可判定**：成功为 0，失败为非 0，并在 stderr 给出可操作的错误（如「请用 --from 指定」）。

给 AI 的一句话提示示例：

> 需要转换数据文件格式时，运行 `transmuta -h` 查看用法，然后用 `transmuta convert <输入> <输出>`，不要自己写转换脚本。

## 使用方法

### 格式转换：`convert`

```bash
transmuta convert <输入文件> <输出文件> [选项]
```

源 / 目标格式默认从扩展名推断：

```bash
transmuta convert data.xlsx data.csv        # Excel → CSV
transmuta convert data.csv  data.parquet    # CSV   → Parquet
transmuta convert data.json data.csv        # JSON  → CSV
transmuta convert data.json out.tsv -d '\t' # 输出 TSV（制表符分隔）
```

扩展名不足以判断时，用 `--from` / `--to` 显式指定：

```bash
transmuta convert in.dat out.dat --from csv --to json
```

常用选项：

- `--from <FMT>`：源格式，覆盖扩展名推断（`excel`、`csv`、`json`）
- `--to <FMT>`：目标格式，覆盖扩展名推断（`csv`、`json`、`parquet`）
- `-d, --delimiter`：CSV/TSV 分隔符，默认 `,`，支持 `\t`（制表符）
- `--batch-size`：批处理大小，默认 10000
- `-t, --threads`：线程数，默认为 CPU 核心数
- `--skip-rows <N>`：跳过开头 N 行（仅 Excel 源生效）
- `--no-header`：源 CSV 没有表头行（默认认为有表头）

> 说明：当前 Parquet 仅作为**目标**支持（尚无 Parquet 读取）；Excel 仅作为**源**支持（尚不支持写出 Excel）。

### 差异比较：`diff`

比较两个数据文件的字段差异。

```bash
transmuta diff -1 file1.csv -2 file2.csv -o diff_result.json
```

主要选项：

- `-1, --input1` / `-2, --input2`：参与比较的两个文件
- `-o, --output`：差异结果输出路径
- `-m, --mode`：差异模式（`union`、`complement`、`only-in-file1`、`only-in-file2`、`sorted-file1`、`sorted-file2` 等）
- `-r, --report`：额外输出详细差异报告
- `-i, --ignore-case` / `-w, --ignore-whitespace`：忽略大小写 / 空白差异
- `-l, --one-field-per-line`：把每行作为一个字段读取

### 随机数据生成：`data-gen`

按列定义文件（CSV 或 JSON）生成随机测试数据：

```bash
transmuta data-gen -s schema.csv -m csv -o data.csv -r 1000
transmuta data-gen -s schema.json -m json -o data.json
```

主要选项：

- `-s, --schema`：列定义文件路径
- `-m, --schema-format`：列定义格式（`csv` 或 `json`）
- `-o, --output`：输出文件路径
- `-f, --format`：输出格式，可选，默认从扩展名推断
- `-r, --rows`：生成行数，默认 1000
- `-d, --delimiter`：CSV 分隔符
- `--seed`：随机种子，用于可复现的随机数据

#### 列定义格式

CSV 形式（每行：列名,类型）：

```
姓名,string
年龄,integer
工资,float
在职,boolean
入职日期,date
```

JSON 形式：

```json
[
  { "name": "姓名", "data_type": "string" },
  { "name": "年龄", "data_type": "integer" }
]
```

支持的数据类型包括 `string`、各宽度整数（`int8`/`int16`/`int32`/`int64` 及无符号版本）、浮点（`float32`/`float64`）、`decimal`、`boolean`、`date`/`time`/`timestamp`、`binary`、`uuid` 等。完整清单见 `transmuta data-gen -h`。

## 发布（维护者）

打 git tag 即由 GitHub Actions 自动交叉编译四个平台并发布到 npm：

```bash
git tag v0.1.0
git push origin v0.1.0
```

需要在仓库 Secrets 中配置 `NPM_TOKEN`。详见 `.github/workflows/release.yml`。

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 PR！
