# Workitem: cost-weekly-report（成本周报自动化）

## 背景

北极星 v2（plan/产品核心目标.md 目标 3）：成本核算与规则约束——每任务成本落账已有（flow-cost-ledger，~/.collabflow/tasks.json 的 cost_usd），但**成本不可视**（无周报）。用户拍板（2026-08-29）：
- 周报推送位置 = **飞书个人**（openid ou_20523b87f6ec1391275827a9476327f7）
- 月预算阈值 = ¥600（budget-guard 用，本 workitem 只出周报）

## 需求

### 1. 周报脚本 `scripts/cost-report.py`（零 LLM，纯 Python 标准库）
数据源：`~/.collabflow/tasks.json`（FLOW_TASK_DIR 可覆盖，测试隔离）：
- 任务注册表字段：task_id / kind / state / executor / model / workitem / workdir / started_at / finished_at / cost_usd / expected_seconds / priority

周报统计（按自然周，周一 00:00 起）：
- 本周任务总数（按 state 分类：done / failed / timeout / 其他）
- 总成本（cost_usd 求和 → 人民币，汇率可配 config budget.exchange_rate 默认 7.2）
- 按项目分布（workdir 解析：/home/l/.openclaw/workspace/projects/<项目>）
- 按执行器分布（reasonix / script / local，含任务数 + 成本）
- 按模型分布（model 字段：deepseek-v4-flash / deepseek-v4-pro / 本地 / 未知）
- 失败成本单列：failed + timeout 任务的花费合计（防隐形浪费）
- 峰谷节省标注：调用 scripts/window.py 判定每个任务运行时段（高峰全价 / 空闲半价），空闲窗口跑的任务标注"原价 vs 实付"省了多少（区间价取中间值或 config prices 段实际值）

### 2. CLI 形态
- `python3 scripts/cost-report.py` → 打印文本周报
- `python3 scripts/cost-report.py --weeks N` → 过去 N 周（默认 1）
- `python3 scripts/cost-report.py --push` → 打印 + 调 feishu-notify.sh 推送飞书个人
- `--json` → 机器可读（测试用）

### 3. cron（周一 09:00，command payload）
`cost-weekly-report`：`bash .../cost-report-weekly.sh`（薄壳：python3 cost-report.py --push）
- 用 command payload（用户 08-11 规则：cron 优先纯 shell 零 LLM）
- 周一早上推上周周报（周末数据已完整）

## 验收标准

1. 429 全量测试不回归；新增单测覆盖：周统计聚合、按项目/执行器/模型分布、失败成本、峰谷节省计算（mock 数据）
2. `--push` 实测推送到飞书个人成功（code 0）
3. 无数据（tasks.json 空/缺失）不崩，输出"无数据"提示
4. 汇率、周起始可配置（config budget 段），测试用 FLOW_TASK_DIR 隔离

## 边界

- 不改 flow-task-core.py 落账逻辑（本 workitem 只读）
- 不新增 LLM 调用（零成本）
- 峰谷节省口径与 scripts/window.py 保持一致（唯一权威），不内联复制
