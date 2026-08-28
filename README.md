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
- `config/pricing.yaml` — **DeepSeek 计费模式唯一真相源**（高峰时段/工作日限定/时区/价目；官方 api-docs.deepseek.com 同步）。改计费模式只改此文件，`scripts/window.py`（selftest）与 flowq/flow-task 自动跟随，勿在任何代码/文档硬编码时段数字；`scripts/check-pricing.py` 可手动对比官方页
- 合并优先级：`CLI flag > 环境变量 > user config > defaults`
- key 只允许环境变量引用（`${DEEPSEEK_API_KEY}`），明文拒绝

## 测试

```bash
python3 -m unittest discover tests   # 单测（状态机/store/执行器契约/verify 门/async 契约）
bash tests/run-smoke.sh              # 15 断言（stub 零 API）
bash tests/run-config-smoke.sh       # 17 断言（配置层，stub 零 API）
bash tests/run-flow-smoke.sh         # C1-C21（flow CLI 冒烟，含 --async/--check）
```

## 目录

```
scripts/          # dsh-design(设计器 CLI) + flow-config(配置加载层) + stats(成本统计)
config/           # defaults.yaml + example + pricing.yaml（峰谷计价唯一真相源）
executors/        # reasonix(主力) / local(本地模型档) / script / stub
plan/             # 产品核心目标（北极星）+ 优化方案 + 运维方案（决策唯一真相源）
archive/          # 历史产物归档（早期任务书/brief 存档，git 历史可恢复）
tests/            # 冒烟测试（stub 模式，零 API 消耗）
templates/        # 上下文包/任务书/审查清单模板（去标识，{{role}} 占位）
memory/           # ocr 记录/任务书（审计）
designs/          # dsh 设计输出（本地，不入仓）
.flow/            # workitem 数据（本地，不入仓；已完成 workitem 归档于 .flow/archive/）
learning/         # 协作方法论（symlink → 个人笔记区，Obsidian 可读改）
```

> **北极星**：`plan/产品核心目标.md` —— 自动化 + 成本核算（降本增效），所有讨论/开发以此为准。
> **优化路线**：`plan/优化方案-v1.0.md` —— P0/P1/P2 按 ROI 排序，长任务锁 offpeak。

## 方法论

- [长任务后台化与定时唤醒](learning/长任务后台化与定时唤醒.md) — 防上下文撑满：轮询上限 / 后台模式 / 定时唤醒协议 / 宿主集成示例（配合 flow workitem 的 `--async` / `--check`）
- [架构测试清单](learning/架构测试清单.md) — 架构验收/回归手工用例（状态机/队列/异步/超时韧性/验证机制/门禁/宿主集成）；**测试用例存档纪律：新能力 → 补用例，技术沉淀**

## workitem 长设计（后台化，--async / --check）

pro 模型设计耗时 5-8 分钟，前台同步阻塞会撑爆宿主上下文。三步后台化：

```bash
# 1. 后台起设计(立即返回,输出 worker pid + 日志路径)
./scripts/flow workitem design w1 --async [--expected 480]

# 2. 确认 pid 后设一次性定时唤醒,宿主结束回合(不轮询)

# 3. 醒来后幂等查询(完成才落盘 design.md 并转 designed)
./scripts/flow workitem design w1 --check
```

- `--check` 幂等：已完成再查 → exit 0 no-op（不重落盘、不重发 `design` 事件）；运行中/失败分支零写入，可无限重试
- 退出码：`0` 完成 / `3` 运行中（未超预期，宿主继续等唤醒）/ `124` 超时报警（`alarm:"timeout"`，含超预期时长仍未完成）/ `1` 失败（dsh-design 失败、后台崩溃、结果损坏）/ `2` 用法或前置错误
- 日志与完成记录：`<wi_dir>/design-async.log` + `design-async-result.json`（单真相源）；`expected_seconds` 默认 480（`config/defaults.yaml` 或 `--expected N` 覆盖，软截止非硬超时）
- 失败重跑：`flow workitem design w1 --async` 直接重起（在途任务会被拒，先 `--check`）

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
