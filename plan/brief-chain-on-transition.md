# brief: chain-on-transition — post-transition 钩子框架（W-A）

> 2026-08-24 ｜ collab-flow 仓 ｜ 方案：plan/链路畅通-运维方案-v2.0.md（§2.1/§3/§8）
> 目标：状态转移完成即触发下一步（事件驱动链式推进），零 cron 巡检

## 1. 背景

execute 前置状态三连 fail 复盘：状态机半自动，design 后 review/translate/入队 execute 全靠人记得。
根治方案 = Chain-on-Transition：post-transition 钩子挂在 `_do_transition` 写状态的公共路径，
状态一变当场推进下一步（推模式），不靠 cron 巡检（拉模式）。

## 2. 任务范围（做什么 / 不做什么）

做（改动限 flow-core.py + 测试，不动 flow-task-core.py——那是 W-B 的活）：
1. **钩子框架**：config `chain` 段（enabled/on_designed/on_reviewed/on_translated/on_executed/on_verified/
   warn_hours/force_hours/scan_projects/notify）；`_do_transition` 转移完成后调用钩子（手动 transition
   与 pump 内部写状态走同一公共路径）
2. **钩子类型**（各含 dry-run/audit/幂等/防重入锁）：
   - `on_designed → auto_review`（默认 off 只提醒）：机械检查确定性判定（design.md 非空/含测试策略/
     含错误处理表/含验收对照）→ 不过自动写 decision.yaml verdict=reject；llm 模式调 reasonix -p 审
     （verdict=机械&&LLM 双过）；`require_human_review` 标记跳过只提醒
   - `on_reviewed → auto_translate`：taskbook 模板渲染（```flow 前置块由脚本从 design.md 结构提取：
     test_command 声明>仓惯例探测；diff_scope.allow 从改动文件清单；deny 冻结清单）+ LLM 提炼正文
     （reasonix -p 模板化 prompt）→ 机械校验（前置块 test_command 非空 && allow 非空）→ 写盘 taskbook.md
   - `on_translated → auto_enqueue`：window.py 判下一空闲窗口 → flow task add 构造（白名单格式、
     --workdir=仓目录、expected_seconds 从 design-result 实际时长学习、同仓 diff_scope 重叠自动错开 ≥40min）
   - `on_executed → auto_verify`：调 verify --auto（测试/越界/错误表三条件）；失败打回 → 自动重新入队
     execute（重试环，连续失败 ≥3 冻结 workitem 升级人工）
   - `on_verified → notify_accept`：推送验收摘要（测试数/越界/错误表），待 accept
3. **安全**：钩子失败不阻塞转移（audit+推送）；防重入锁（flock 复用）；风暴防护（连续失败 N 次冻结）；
   graceful degradation（老 workitem 无 audit/events 兼容跳过；chain.enabled=false 完全退化）
4. **audit**：所有自动动作写 `events/audit.jsonl`（时间/动作/输入输出/结果）
5. **单测**（~20 用例）：钩子触发（手动与 pump 内部两路径）、幂等、防重入、机械判定 pass/reject、
   llm 模式 mock、taskbook 模板渲染+前置块校验、enqueue 窗口计算+同仓错开、dry-run 零写入、
   require_human_review 跳过、audit 落盘、老 workitem 兼容

不做：
- 不动 flow-task-core.py（任务终态钩子/超时收口 = W-B）
- 不新增任何 cron
- 不改既有状态机转移语义（TRANSITIONS 表原样，只加转移后动作）

## 3. 验收标准

1. `python3 -m unittest discover tests` 全绿（141 旧 + ~20 新）
2. `ruff check` 干净
3. 冒烟：fixture workitem 手动 transition → 钩子当场执行（同步语义：transition 返回前钩子已完成）；
   dry-run 零写入；chain.enabled=false 完全退化
4. 真实队列不误触发（现有 workitem 无 audit 产物兼容）

## 4. 默认约束

- 只改 flow-core.py + 测试文件；不碰 git；不新增 cron；完成后独立报告
- 参考：collab-flow.md §3.2 状态机/§3.4 taskbook；学习/2026-08-24-execute-前置状态fail-复盘.md
