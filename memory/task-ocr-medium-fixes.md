# 任务书：ocr medium 三修（F1 锁内无超时 subprocess / F2 终态钩子状态守门 / F3 resolve_execute_params 双调用）

> 2026-08-27 ｜ 来源：ocr-final-2026-08-25 findings #2/#4 + ocr-low-2026-08-27 中 low 攒批 #3
> 路由：ocr 修复（findings 自带方案）→ reasonix flash 直修，不走 design 路线（AGENTS.md）

## 背景

collab-flow 框架（MIT 公开仓）ocr review 落盘 3 个 medium/low findings，均为局部小修：
1. `scripts/flow-task-core.py` `assess_execute_completion` 在持 workitem 锁时跑无超时 subprocess 测试，可能阻塞该 workitem 全部转移
2. `scripts/flow-task-core.py` `_terminal_hooks_action` 的 execute done 分支缺与 design 分支等价的状态守门，可能对已转移 workitem 重复触发 auto_verify
3. `scripts/flow-core.py` `resolve_execute_params` 在 enqueue 路径被调两次（直接 + build_execute_command 内部），重复 design.md I/O + config 解析

## 目标（三处独立修复，各自带测试）

### F1：锁内测试加 bounded timeout

- **位置**：`scripts/flow-core.py` `_run_tests(wdir, command)`（约 1785 行）+ `scripts/flow-task-core.py` `assess_execute_completion(wi_dir, full_cfg)`（约 2719 行）
- **问题**：`_run_tests` 调 `subprocess.run(command, shell=True, ...)` 无 timeout；`assess_execute_completion` 在 `with_workitem_lock` 锁内调用它（flow-task-core.py 约 3087 行 `_fc.with_workitem_lock(wi_dir, lambda: assess_execute_completion(...))`），测试挂起 → 阻塞该 workitem 全部转移
- **修法**：`_run_tests` 加可选参数 `timeout=None`（默认 None 保持现行为），传值时 `subprocess.run(..., timeout=timeout)` 并捕获 `subprocess.TimeoutExpired` → 返回 `{"pass": False, "exit_code": None, "reason": "timeout"}`；`assess_execute_completion` 调用 `_run_tests` 时传一个 bounded timeout（建议从 config 读，如 `full_cfg.get("rescue", {}).get("test_timeout_s")`，默认 300；若 config 无此键就硬编码 300 常量并注释）
- **注意**：`_run_tests` 是 flow-core.py 里 verify 路径共用函数（verify --auto 也用），timeout 参数必须可选、默认 None，不能改变 verify 现有行为

### F2：execute 终态钩子补状态守门

- **位置**：`scripts/flow-task-core.py` `_terminal_hooks_action(t, cfg, state)`（约 2642 行）
- **问题**：`kind == "execute" and state == "done"` 分支直接 `_trigger_chain(wi, cfg, "execute", "executed")`，未像上面 design 分支那样先读 workitem 当前 state；workitem 已被转移（state != executed）时仍触发 → 重复 auto_verify（重复 LLM/测试）+ spurious audit
- **修法**：与 design 分支对称——先 `cur = _read_wi_state_safe(wi, cfg)`，仅 `cur == "executed"` 时 `_trigger_chain` 并返回 `execute_done`；否则返回 `{"action": "execute_done_no_transition", "detail": cur}`（命名与 design 分支 `design_done_no_transition` 对齐，保持返回结构一致）

### F3：build_execute_command 复用已解析 params

- **位置**：`scripts/flow-core.py` `build_execute_command(wi_dir, cfg, force=False, force_reason=None)`（约 3495 行）
- **问题**：两个调用方 `enqueue_execute_retry`（约 3547 行）和 `_hook_auto_enqueue`（约 3747 行）都已先 `resolve_execute_params(...)` 拿 params（算 exp 用），随后调 `build_execute_command` 时内部又调一次 `resolve_execute_params`（3502 行）→ 重复 design.md I/O + config 解析
- **修法**：`build_execute_command` 加可选参数 `params=None`；`params is None` 时内部解析（保持向后兼容），否则直接复用。两个调用方传入已解析的 params
- **注意**：`enqueue_execute_retry` 用 `force=True` 解析，`_hook_auto_enqueue` 用默认 force；传入的 params 必须与调用方自己解析的一致（把各自的 `params` 变量传进去即可）

## 约束

- 只改 `scripts/flow-core.py` + `scripts/flow-task-core.py` + 测试文件；不改无关代码
- 不碰 git（不 commit 不 push）
- 不新增外部依赖（保持零依赖标准库）
- 不改变 verify --auto 既有行为（F1 的 timeout 参数默认 None）
- 参考现有测试风格：tests/ 下与 flow-task 相关测试（test_flow_task_core.py / test_flow_task_m2.py 等）

## 验收标准

1. `python3 -m unittest discover tests` 全绿（现有 141 单测 + 新增用例）
2. 新增/更新测试覆盖：
   - F1：`_run_tests` 传 timeout 且命令超时 → 返回 reason=timeout 且 pass=False；不传 timeout 行为不变
   - F2：execute done 时 workitem state != executed → 不触发 chain、返回 execute_done_no_transition
   - F3：build_execute_command 传 params → 不重复解析（可断言 resolve 调用次数或行为等价）
3. `ruff check` 干净
4. `bash -n` 语法检查通过（如改到 .sh 文件——本任务不涉及）
5. 完成后输出：改了哪些文件哪些函数、测试结果、新增用例清单
