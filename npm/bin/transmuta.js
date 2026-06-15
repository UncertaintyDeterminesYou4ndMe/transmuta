#!/usr/bin/env node
"use strict";

// transmuta 主包启动器。
//
// npm 安装时只会拉取与当前系统匹配的那个平台子包（由各子包的 os/cpu 字段限定），
// 这里在运行时把真正的原生二进制定位出来并原样转发参数、stdio 与退出码。
// 采用「启动器」而非 postinstall 拷贝：不需要安装期联网或执行脚本，
// 因此在受限/离线沙箱（含 --ignore-scripts）中也能可靠工作。

const { spawnSync } = require("child_process");

// node 的 process.platform/arch 与子包命名约定：transmuta-<platform>-<arch>
const SUPPORTED = ["darwin-arm64", "darwin-x64", "linux-x64", "linux-arm64"];

function resolveBinary() {
  const key = `${process.platform}-${process.arch}`;
  const pkg = `transmuta-${key}`;
  try {
    // 解析子包内的二进制文件绝对路径（文件由发布流程预置在 bin/transmuta）。
    return require.resolve(`${pkg}/bin/transmuta`);
  } catch (_) {
    return null;
  }
}

function fail(message) {
  process.stderr.write(`transmuta: ${message}\n`);
  process.exit(1);
}

const binary = resolveBinary();
if (!binary) {
  const key = `${process.platform}-${process.arch}`;
  fail(
    `当前平台 (${key}) 没有可用的预编译二进制，或对应子包未安装。\n` +
      `        已支持的平台：${SUPPORTED.join(", ")}。\n` +
      `        如果安装时用了 --no-optional / --ignore-optional，请去掉后重装。`
  );
}

const result = spawnSync(binary, process.argv.slice(2), { stdio: "inherit" });

if (result.error) {
  fail(result.error.message);
}

// 透传被信号终止的情况，否则透传子进程退出码。
if (typeof result.status === "number") {
  process.exit(result.status);
}
process.exit(1);
