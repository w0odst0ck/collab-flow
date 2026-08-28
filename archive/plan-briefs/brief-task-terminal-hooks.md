# brief: task-terminal-hooks + stale-gate — 任务终态钩子与超时收口（W-B）

> 2026-08-24 ｜ collab-flow 仓 ｜ 方案：plan/链路畅通-运维方案-v2.0.md（§2.2/§2.3/§8）
> 依赖：W-A（chain-on-transition）的钩子框架——W-B 在 W-A 之后执行（同仓串行）

## 1. 背景

W-A 实现 post-transition 钩子后，还需两个补位：
① 任务终态（pump runner 完成 design/execute）时要驱动 workitem 状态 + 触发链；
② 卡点超时（无人处理）要有兜底——挂在 pump dispatch 循环顺带（不新增 cron）。

## 2. 任务范围（做什么 / 不做什么）

做（改动限 flow-task-core.py + 测试，flow-core.py 的钩子框架复用 W-A，只读调用）：
1. **任务终态钩子**：pump runner 任务终态处理（既有代码路径）追加：
   - design done → 确认 workitem → designed → 触发 on_designed 钩子（W-A 实现）
   - execute done → 触发 on_executed 钩子（verify 链）
   - execute failed/timeout → 即时推送失败报告（含 failure_tail 指引），走 notify 模板
2. **超时收口（stale gate）**：pump dispatch 循环末尾顺带检查（同一进程同一循环，非新 cron）：
   - 卡点生命周期（按 events.jsonl 最后事件计时）：<2h 静默计数 → 2-24h 推送提醒（每天 ≤1 次）→
     >24h 升级提醒（@用户+明示将收口）→ >48h 强制收口：
       R1 待 review → 机械+LLM 双检自动 pass（机械不过 → 自动 reject 打回）
       R2 待 translate → 自动生成 taskbook（模板约束）
       R3 待 execute → 自动入队下一空闲窗口
       R4/R5 verify/accept → 标记 stale + 移入 `.flow/stale/` 归档（不删除，可恢复）+ 报告
   - 收口动作复用 W-A 钩子函数（不重复实现）
3. **expected_seconds 自适应**：auto_enqueue 时从 registry 同仓同类任务历史实际时长学习
   （无历史 → 默认 1500s）
4. **audit**：收口/推送动作写 `events/audit.jsonl`
5. **单测**（~18 用例）：终态钩子（done/failed/timeout 三态）、超时演进（静默→提醒→升级→收口）、
   >48h 强制动作（R1-R5 各态）、归档可恢复、expected_seconds 学习、audit 落盘

不做：
- 不动 flow-core.py 既有逻辑（钩子框架在 W-A，本任务只读调用）
- 不新增任何 cron（全部挂 pump 既有循环）
- 不改状态机转移表

## 3. 验收标准

1. `python3 -m unittest discover tests` 全绿（141 旧 + W-A ~20 + 本任务 ~18）
2. `ruff check` 干净
3. 冒烟：fixture 构造卡点 workitem（改 events 时间戳模拟超时）→ pump 循环顺带收口正确；
   现有 done 任务不误触发；stale 归档可恢复
4. 真实队列冒烟：pump 跑一轮，无卡点误报

## 4. 默认约束

- 只改 flow-task-core.py + 测试文件；不碰 git；不新增 cron；完成后独立报告
- 参考：collab-flow.md §3.3 verify 三条件；学习/2026-08-24-execute-前置状态fail-复盘.md
