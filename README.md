# collab-flow

跨设备、可迭代、开源的「设计 → 审查 → 执行」多角色 AI 协作开发框架。

把「AI 设计师出方案 → AI critic 审查 → AI 执行器编码 → 人类拍板」的协作流，抽象为与具体人、具体模型、具体执行器解耦的通用引擎。

## 核心概念

| 概念 | 说明 |
|---|---|
| **四角色** | `designer`（方案设计，默认 dsh + pro 模型）/ `critic`（审查/批评/翻译）/ `executor`（编码执行）/ `approver`（人类拍板） |
| **质量门禁** | 方案过 critic 审查才进执行层 |
| **测试门禁** | 测试全过 + diff 对照方案 + 错误处理表核对（防重大 bug 的命门） |
| **模型分级** | 设计用强模型（pro）、执行用便宜模型（flash）——成本与质量的平衡 |
| **上下文契约** | 标准输入包模板（背景/目标/约束/红线/验收），复用降本（cache 命中 65-73%） |
| **分层设计** | designer 设计「怎么做」；critic 设计「做什么、对不对、怎么验、怎么落地」 |

## 快速开始

```bash
# 1. 前置：dsh（DeepSeek Harness）+ DEEPSEEK_API_KEY
# 2. 生成设计方案（强制 pro 模型，read-only 沙箱）
./scripts/flow-config -d <项目目录> "任务描述或简报文件路径"

# 输出: <项目>/designs/<时间戳>-<slug>.md + 成本摘要
# 3. critic 按 审查清单 审查 → 通过后翻译成执行器任务书
```

无任何配置时开箱即用（`config/defaults.yaml` 内置默认值）；用户配置放 `~/.config/collabflow/config.yaml` 按需覆盖。

## 配置

- `config/defaults.yaml` — 全局默认（零个人标识）
- `config/config.example.yaml` — 用户配置示例
- 合并优先级：`CLI flag > 环境变量 > user config > defaults`
- key 只允许环境变量引用（`${DEEPSEEK_API_KEY}`），明文拒绝

## 测试

```bash
bash tests/run-smoke.sh        # 15 断言（stub 零 API）
bash tests/run-config-smoke.sh # 17 断言（配置层，stub 零 API）
```

## 目录

```
scripts/          # dsh-design(设计器 CLI) + flow-config(配置加载层) + stats(成本统计)
config/           # defaults.yaml + example
tests/            # 冒烟测试（stub 模式，零 API 消耗）
templates/        # 上下文包/任务书/审查清单模板（去标识，{{role}} 占位）
learning/         # 协作方法论（symlink → 个人笔记区，Obsidian 可读改）
```

## 方法论

- [长任务后台化与定时唤醒](learning/长任务后台化与定时唤醒.md) — 防上下文撑满：轮询上限 / 后台模式 / 定时唤醒协议 / 宿主集成示例（配合 flow workitem 的 `--async` / `--check`）

## 路线图

- [x] P0 冻结基线 / P1 配置层抽取（flow-config）
- [ ] P2 work-item 约定（.flow/ + 状态机 + engine）
- [ ] P3 执行器适配（executor 抽象 + verify）
- [ ] P4 跨设备（git 同步 + 单写者锁）
- [ ] P5 复盘版本化（retro.jsonl + semver 流程升级）
- [ ] P6 双仓拆分 / 文档 / CI 完善
- [ ] P7 发布迭代

## License

MIT
