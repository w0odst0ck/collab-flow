# Workitem: cost-budget-guard（预算上限告警 + 超限暂停）

## 背景

北极星 v2 目标 3：预算控制——周/月预算阈值，超限告警 + 暂停新长任务。用户拍板（2026-08-29）：**月预算 ¥600**（建议按近一月实际花费 ×1.5 得出）。

## 需求

### 1. config 预算段（config/defaults.yaml 新增 `budget:` 段，全标量，P1 解析器照常通过）
```yaml
budget:
  month_cny: 600          # 月预算上限（人民币），0/缺失 = 不启用
  week_cny: 0             # 周预算上限（0 = 不启用）
  exchange_rate: 7.2      # cost_usd → CNY 换算（周报共用，唯一真相源）
  alert_cooldown_h: 24    # 飞书告警冷却（防刷屏）
  pause_long_task_s: 600  # 超限后暂停的长任务阈值（expected_seconds >= 此值 → 拒绝）
```
env 覆盖：FLOW_BUDGET_MONTH_CNY / FLOW_BUDGET_WEEK_CNY / FLOW_BUDGET_EXCHANGE_RATE（测试隔离用）

### 2. 预算检查模块（scripts/flow-task-core.py 内新增纯函数，或独立 budget.py 被引用）
- `budget_usage(now, cfg) -> (month_used_cny, week_used_cny)`：从 tasks.json 聚合当月/当周 cost_usd × 汇率
- `budget_exceeded(cfg) -> (exceeded: bool, level: month|week, used, limit)`：超限判定（fail-closed：读不到数据按 0 算，不误杀）
- 判断口径：cost_usd 非 null 的任务才算（script/local 落 0，不计费）；按 finished_at 归月/归周（无 finished_at 的 running 任务不计）

### 3. 接入点（三处，全部 best-effort 不阻断主流程）
- **flow task add 门禁**：超限 + 任务 expected_seconds >= pause_long_task_s + priority != P0 → 拒绝（P0 紧急放行）
- **flow task pump**：scheduled 到期任务同理（超限长任务暂停执行，留在 scheduled 不丢）
- **飞书告警**：超限状态变化（进入超限 / 每 alert_cooldown_h）→ feishu-notify.sh 推个人（预算使用率 + 超限说明 + 暂停规则）
  - 告警状态存 ~/.collabflow/budget-alert.json（上次告警时间戳 + 上次 level），防重复刷屏

### 4. CLI 辅助
- `flow task budget` 子命令：显示当前月/周用量、阈值、是否超限（--json 机器可读）
- 不加 cron（pump 每分钟已跑，add 时同步检查）

## 验收标准

1. 单测覆盖：聚合计算（当月/当周边界）、超限判定（月/周/未启用三态）、add 拒绝（长任务非 P0）、P0 放行、告警冷却、FLOW_TASK_DIR 隔离
2. 429 全量不回归
3. 实测：临时把阈值设很低 → add 长任务被拒 + 飞书告警收到 → 恢复阈值
4. fail-closed 纪律：config 段损坏/缺失 → 回退不启用（不误杀正常任务）

## 边界

- 不改落账逻辑；只读 tasks.json
- 告警走 feishu-notify.sh（复用现有 HTTP 通道，零新依赖）
- 暂停是"不启动"不是"删除"：scheduled 任务保留，超限解除后 pump 自然续跑
