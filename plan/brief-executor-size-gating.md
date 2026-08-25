# brief: executor-size-gating — execute 自动规模判定（W-S1）

> 2026-08-24 ｜ collab-flow 仓 ｜ 背景：今晚 chain/task-terminal-hooks 双双超时（500k token 上下文膨胀）
> 根因：大改任务用默认 flash + 1800s 超时（TOOLS.md 早有「难任务用 pro」教训，靠人记得 = 不健壮）
> 目标：模型/超时选择机器强制，agent 选错需 --force（fail-closed）

## 1. 任务范围（做什么 / 不做什么）

做（改动限 flow-task-core.py/flow-core.py + config + 测试）：
1. **size 三档声明**：taskbook/brief 支持 `size: small|medium|large`（flow 前置块或 frontmatter，机器读）
   - 缺失时自动估算：design.md 字节数（>30KB → large）+ 改动文件数（>5 → large）综合判定
2. **模型/超时映射**（config/defaults.yaml `executor` 段，可配）：
   - small → flash + 1200s；medium → flash + 1800s；large → **deepseek-pro + 2400s** + RX_TIMEOUT 放大
3. **--force 门禁**：size=large 时若显式 `--model flash` 或 `--timeout <2400` → 拒绝（GateReject），
   需 `--force` 附理由（复用 force_reason 机制）
4. **execute 构造处接入**：flow execute 解析 size → 注入 model/timeout/RX_TIMEOUT env
5. **单测**（~10 用例）：size 解析（声明/缺失估算/非法值）、映射表、--force 门禁、env 注入、
   估算边界（字节/文件数阈值）

不做：
- 不动终态钩子/超时抢救（W-S2）
- 不改 reasonix 本体（只在 flow 侧注入参数）

## 2. 验收标准

1. `python3 -m unittest discover tests` 全绿（141 旧 + W-V1/V2 新增 + ~10 新）
2. `ruff check` 干净
3. 冒烟：fixture workitem（size=large）→ flow execute --sync 注入 pro + 2400 + RX_TIMEOUT；
   `--model flash` 被拒；无 size 声明的大 design.md 自动判 large

## 3. 默认约束

- 只改 flow-task-core.py/flow-core.py/config/defaults.yaml + 测试；不碰 git；完成后独立报告
- 参考：task-terminal-hooks 的 expected_seconds 学习（同文件已有模式）、config/defaults.yaml 结构
