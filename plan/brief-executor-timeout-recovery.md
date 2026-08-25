# brief: executor-timeout-recovery — 超时自动抢救 + token 预警（W-S2）

> 2026-08-24 ｜ collab-flow 仓 ｜ 背景：今晚两次 execute 超时（exit 124），靠人工抢救
> （补 result.json/diff.patch → verify → accept）——纯机械流程应机器化
> 目标：timeout/failed 终态自动抢救 + 上下文膨胀提前预警

## 1. 任务范围（做什么 / 不做什么）

做（改动限 flow-task-core.py 终态处理 + 测试，复用 task-terminal-hooks 的超时收口框架）：
1. **自动抢救钩子**（execute timeout/failed 终态触发）：
   - 实现完成度检查（机械判定）：`python3 -m unittest` 相关测试绿 + executor/result.json 缺失
   - 完成度达标（测试绿但缺 result.json）→ 自动补 result.json（note 注明「自动抢救，原始 timeout」）
     + 自动生成 diff.patch（git diff 未提交改动）→ 触发 verify --auto → 通过则推送「可 accept」报告
   - 完成度不达标（测试红/改动半成品）→ 自动重新入队 execute（下一空闲窗口，window.py）
2. **风暴防护**：自动重跑限次（默认 2，复用 chain_fail_count/frozen 机制）——超限冻结 + 升级人工
3. **audit**：抢救动作全写 events/audit.jsonl（谁/为什么/原始 exit/timeout/结果）
4. **token 预警**：终态时 grep reasonix 日志 usage 峰值行（`· NNN tok ·`）> 阈值（config，默认 300k）
   → 推送告警（notify 模板）「上下文过大，下次建议拆分任务」
5. **单测**（~12 用例）：抢救判定（测试绿+缺 result.json / 测试红 / 半成品）、自动补 result.json +
   diff.patch、自动重跑限次 + frozen、audit、token 峰值提取 + 阈值告警、非 execute 终态不触发

不做：
- 不动 size 判定（W-S1）
- 不自动 accept（质量门，只推送「可 accept」报告）
- 不碰 reasonix 本体

## 2. 验收标准

1. `python3 -m unittest discover tests` 全绿（141 旧 + W-V1/V2/S1 新增 + ~12 新）
2. `ruff check` 干净
3. 冒烟：fixture 模拟 timeout 终态（实现完整测试绿）→ 自动补 result.json/diff.patch → verify 通过；
   测试红场景 → 自动重跑；连跑 2 次失败 → frozen；token 超阈值 → 告警

## 3. 默认约束

- 只改 flow-task-core.py + 测试；不碰 git；完成后独立报告
- 参考：task-terminal-hooks 的终态处理（本任务在其上扩展）、chain-on-transition 的 frozen 机制、
  flow-cost-ledger 的 audit 模式
