# brief: verify-baseline-snapshot — execute 基线快照（W-V1）

> 2026-08-24 ｜ collab-flow 仓 ｜ 方案：plan/verify-交叉污染修复方案-v1.0.md（§3.1/§7）
> 排期：明晚 18:00 窗口（与同仓 chain/task-terminal-hooks 串行）
> 目标：execute 任务启动拍 git 基线快照，verify 据此区分「本任务产物」vs「他任务产物/历史遗留」

## 1. 背景

verify 的 changed_files 收集 = 全仓 git status（M + ?? 全收），同仓并行 workitem 产物 + 历史
untracked 全算进当前任务 → out_of_scope 误报（今晚触发 4 次）。修复第一步：**基线快照**。

## 2. 任务范围（做什么 / 不做什么）

做（改动限 flow-task-core.py + 测试，不碰 flow-core.py verify 判定——那是 W-V2 的活）：
1. **execute 任务启动时拍快照**（runner 写 started_at 的同一处）：
   - 快照内容：`{head: <sha>, tracked_modified: {<path>: <content-hash>}, untracked: [<path>...]}`
   - 获取方式：`git rev-parse HEAD` + `git diff --name-only`（M 文件，逐文件 hash-object 记 hash）
     + `git ls-files -o --exclude-standard`（untracked 只记清单不 hash）
   - **工作目录**：任务实际执行目录（runner env 的 FLOW_WORKDIR，非 CLI 调用目录）
2. **快照存储**：独立文件 `.flow/tasks/<id>.snapshot.json`（不挤 registry 并发写热点）
   - 快照写入与 started_at 原子化（同一写入事务/顺序保证：先写快照文件再写 registry start，或同函数内）
3. **读取接口**：`flow task snapshot <id> [--json]`（查看/调试）；verify 侧读取函数（供 W-V2 调用）
4. **兼容**：无 git 仓（workdir 不是 git 仓）→ 快照 `{git: false}`，verify 退化全仓收集 + warning；
   快照文件损坏/缺失 → 同样退化 + warning（不阻断）
5. **生命周期**：任务终态后快照保留（审计）；`flow task prune` 时随任务记录清
6. **单测**（~8 用例）：快照内容正确（M hash/untracked 清单）、非 git 仓容错、快照损坏降级、
   读取接口、prune 清理、原子化（快照先于 start 或同事务）

不做：
- 不动 verify 判定逻辑（W-V2）
- 不改 taskbook 模板（W-V2）
- 不动 scheduled/queued 语义

## 3. 验收标准

1. `python3 -m unittest discover tests` 全绿（141 旧 + ~8 新）
2. `ruff check` 干净
3. 冒烟：fixture 仓（含 M + untracked 文件）→ 入队一个 execute（mock 执行器）→ 快照文件正确生成；
   非 git 仓容错；prune 清理

## 4. 默认约束

- 只改 flow-task-core.py + 测试；不碰 git 业务仓库；完成后独立报告
- 参考：flow-task-core.py runner 的 started_at 写入处、registry 结构
