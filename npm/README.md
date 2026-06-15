# transmuta

纯本地、离线的数据文件格式转换 CLI（Excel / CSV / JSON / Parquet）。一条命令完成转换，无需编写脚本或配置语言环境——尤其适合给 AI / 自动化流程直接调用。

```bash
npm install -g transmuta
# 或免安装：
npx transmuta convert data.xlsx data.csv
```

## 用法

```bash
transmuta convert <输入> <输出>     # 源/目标格式自动从扩展名推断
transmuta convert data.csv  data.parquet
transmuta convert data.json data.csv
transmuta convert in.dat out.dat --from csv --to json   # 扩展名无法推断时显式指定
transmuta -h                       # 查看全部命令与示例
```

- 可作为**源**：`excel`(.xlsx/.xls/.xlsm/.xlsb)、`csv`/`tsv`、`json`
- 可作为**目标**：`csv`、`json`、`parquet`
- 还包含 `diff`（文件字段差异比较）与 `data-gen`（随机测试数据生成）

## 平台

安装时只会拉取与当前系统匹配的预编译二进制，无需安装期联网或编译：

- macOS：arm64 / x64
- Linux：x64 / arm64（glibc）

完整文档见 [GitHub 仓库](https://github.com/UncertaintyDeterminesYou4ndMe/transmuta)。

## 许可证

MIT
