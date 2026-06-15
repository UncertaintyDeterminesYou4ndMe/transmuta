#!/usr/bin/env node
// 把同一个版本号写入主包、4 个平台子包，以及主包的 optionalDependencies 固定版本。
// 用法：node scripts/set-version.mjs 1.2.3
// 也接受带前缀的 git tag（如 v1.2.3）。

import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const npmRoot = join(__dirname, "..");

const raw = process.argv[2];
if (!raw) {
  console.error("用法: node scripts/set-version.mjs <version>");
  process.exit(1);
}
const version = raw.replace(/^v/, "");
if (!/^\d+\.\d+\.\d+(?:[-+].+)?$/.test(version)) {
  console.error(`无效的版本号: ${raw}（期望 semver，如 1.2.3 或 v1.2.3）`);
  process.exit(1);
}

const platforms = [
  "darwin-arm64",
  "darwin-x64",
  "linux-x64",
  "linux-arm64",
];

function patch(path, mutate) {
  const pkg = JSON.parse(readFileSync(path, "utf8"));
  mutate(pkg);
  writeFileSync(path, JSON.stringify(pkg, null, 2) + "\n");
  console.log(`已更新 ${path} -> ${pkg.version}`);
}

// 平台子包
for (const p of platforms) {
  patch(join(npmRoot, "platforms", p, "package.json"), (pkg) => {
    pkg.version = version;
  });
}

// 主包：自身版本 + optionalDependencies 固定到同一版本
patch(join(npmRoot, "package.json"), (pkg) => {
  pkg.version = version;
  for (const p of platforms) {
    pkg.optionalDependencies[`transmuta-${p}`] = version;
  }
});
