# brief: verify-incremental-scope — verify 增量收集 + 智能豁免（W-V2）

> 2026-08-24 ｜ collab-flow 仓 ｜ 方案：plan/verify-交叉污染修复方案-v1.0.md（§3.2/§3.3/§7）
> 依赖：W-V1 verify-baseline-snapshot（快照已就绪）
> 排期：明晚 18:00 窗口，W-V1 之后串行
> 目标：verify 只收「任务期间增量」，豁免他任务产物/历史遗留；deny 语义修正

## 1. 背景

W-V1 提供基线快照后，verify 的 diff 判定改为增量收集：当前 git status − 基线 = 任务期间增量。
本任务实现增量收集 + 智能豁免 + taskbook 模板 deny 语义修正。

## 2. 任务范围（做什么 / 不做什么）

做（改动限 flow-core.py verify 逻辑 + 模板 + 测试，只读调用 W-V1 的快照）：
1. **增量收集**：verify 时读快照（W-V1 接口），changed_files 计算：
   - tracked M：基线 M 且 content-hash 未变 → 豁免（非本任务产物）；hash 变了 → 收集（本任务改过）
   - untracked：基线已存在 → 豁免；新增 → 收集
   - 无快照（老任务/损坏）→ 退化全仓收集 + warning「无基线快照，请人工确认范围」（不阻断）
   - HEAD 变化（任务期间有 commit）→ 退化全仓收集 + warning（罕见路径）
2. **智能豁免**：剩余 out_of_scope 归一化匹配——allow 目录/文件前缀命中（如 allow
   `intel/tools/tieba_single.py` 而收集到 `intel/tools/` → 按前缀豁免）；豁免项记入 verify 报告
   `waived_scope: [{path, reason}]`（reason = baseline_modified/baseline_untracked/prefix_match），
   透明可审计；warning 列未被豁免的越界项
3. **taskbook 模板 deny 语义修正**：模板 deny 只含真正核心冻结（jd_parse 类），加注释
   「同仓其他 workitem 的在制品路径不要写进 deny——verify 基线快照会自动豁免」
4. **单测**（~10 用例）：基线 M 未变豁免 / M 变更收集 / 基线 untracked 豁免 / 新增 untracked 收集 /
   无快照退化 + warning / HEAD 变化退化 / 前缀归一化豁免 / waived_scope 报告字段 / deny 核心仍强保护 /
   fixture 同仓并行（A 产物在 B verify 不被收）

不做：
- 不动快照生成（W-V1）
- 不改 verify 三条件语义（tests_pass/diff_match/error_table_match 结构不变，只改 diff 收集来源）
- 不改状态机

## 3. 验收标准

1. `python3 -m unittest discover tests` 全绿（141 旧 + W-V1 ~8 + 本任务 ~10）
2. `ruff check` 干净
3. 冒烟：fixture 构造「基线 M/untracked + 任务新增文件」→ verify 只收新增；同仓并行模拟不误报；
   老任务（无快照）不阻断
4. 真实队列冒烟：现有 done 任务 verify 重跑不误报

## 4. 默认约束

- 只改 flow-core.py verify 逻辑 + templates/ + 测试；不碰 git 业务仓库；完成后独立报告
- 参考：flow-core.py verify 的 diff 收集现实现（全仓 git status 处）、templates/taskbook 模板
