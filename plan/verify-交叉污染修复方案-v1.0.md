# verify 交叉污染修复方案 v1.0 — 基线快照 diff 判定

> 2026-08-24 ｜ collab-flow 仓 ｜ 触发：今晚 verify 越界误报 4 次（company-profile/l2/company-site/resume-star）
> 根因：verify 的 changed_files 收集 = 全仓 git status（M + ??），同仓并行 workitem 产物 + 历史
> untracked 全部算进当前 workitem → out_of_scope 误报；且 deny 优先于 allow 放大误伤

## 1. 问题复现（今晚 4 次）

| workitem | 误报文件 | 实际来源 |
|---|---|---|
| company-profile | test_jd_profile.py、plan/brief-l2-*.md | 历史 M + 其他 brief untracked |
| l2-company-profile | company_profile.py、fixtures/ | W2 产物被自己 deny 死 |
| company-site-collector | intel/tools/（tieba_single.py） | 08-22 历史 untracked |
| resume-star | asu_polish/main/build.py 等 19 个 | l2 + build-star + company-profile 产物 |

每次修法：手动补 taskbook allow 重验——**治标**，同仓并行越多越痛。

## 2. 根因

verify 的 diff 收集（flow-core verify 逻辑）：
```
changed_files = git status --porcelain 全量（M + ?? 全部）
out_of_scope = changed_files - diff_scope.allow（deny 无条件优先）
```
- **无基线**：无法区分「本任务产物」vs「他任务产物/历史遗留」——全仓未提交即本任务
- **deny 语义过强**：taskbook 作者为防交叉把同仓其他 workitem 路径写进 deny → deny 优先 allow → 自伤

## 3. 修复方案：execute 基线快照 + 增量收集 + 智能豁免

### 3.1 基线快照（核心，flow-task-core 扩展）

**execute 任务启动时**（runner 拉起瞬间）记录快照到任务记录（registry `start_git_snapshot`）：

```
{ "head": "<sha>",
  "tracked_modified": { "<path>": "<content-hash>" },   # 基线时已 M 的文件 + hash
  "untracked": ["<path>", ...] }                         # 基线时已存在的 ?? 文件
```

- 纯 git 只读操作（`git status --porcelain` + `git hash-object` 或 `git diff` 摘要），毫秒级
- **verify 时**：`当前 git status` − `基线快照` = **任务期间增量**（真正属于本任务的改动）
  - tracked 文件：基线 M 且内容 hash 未变 → 非本任务产物，豁免；hash 变了 → 收集
  - untracked：基线已存在 → 豁免；新增 → 收集
- HEAD 变化（任务期间有人 commit）→ 退化全仓收集 + 明确 warning（罕见路径）

### 3.2 智能豁免（flow-core verify 扩展）

基线快照已豁免大部分；剩余 out_of_scope 再判一次：
- 越界文件在 **allow 的父目录/前缀**下（如 allow `intel/tools/tieba_single.py` 而收集到 `intel/tools/`）→ 归一化匹配（目录级 vs 文件级统一按「allow 前缀命中」）
- 豁免项记入 verify 报告 `waived_scope: [...]`（透明可审计），不再打回

### 3.3 deny 语义修正（taskbook 模板）

- 模板 deny 默认只含**真正核心冻结**（jd_parse/jd_fetch/match_analyzer/keyword_pool/pdf_build/README/pool.json/.flow 等）
- 模板加注释：**「同仓其他 workitem 的在制品路径不要写进 deny——verify 基线快照会自动豁免」**
- deny 保留「本任务不可修改」的强保护语义（对真实核心文件），但不再被误用作「防交叉」

### 3.4 兼容

- **老任务无快照**（已 executed 的 workitem）：退化全仓收集 + warning「无基线快照，请人工确认范围」——不阻断（现状行为）
- 老 taskbook 里已有的 deny 交叉项：不动（基线豁免已覆盖），模板只影响新任务

## 4. 测试与验收（~15 用例）

- **快照**（~5）：execute 启动写快照（tracked M hash / untracked 清单）、无 git 仓容错、快照损坏降级
- **增量收集**（~6）：基线 M 未变豁免 / 基线 M 变更收集 / 基线 untracked 豁免 / 新增 untracked 收集 /
  HEAD 变化退化 + warning / 归一化匹配（目录 vs 文件）
- **回归**（~4）：fixture 模拟同仓并行（A 任务产物在 B verify 不被收）、deny 核心仍强保护、
  老任务无快照兼容、verify 报告 waived_scope 字段
- 验收：collab-flow 141 单测全绿 + 新用例全绿 + ruff 干净 + 真实队列冒烟（现有 done 任务不误报）

## 5. 任务拆分与排期

| 任务 | 内容 | 依赖 | 预估 |
|---|---|---|---|
| W-V1 | 基线快照（flow-task-core：execute 启动写快照 + 存储） | 无 | design 8min + execute 25min |
| W-V2 | 增量收集 + 智能豁免 + 模板更新（flow-core verify 改造） | W-V1 | design 8min + execute 25min |

- 排期：collab-flow 仓，与 chain-on-transition（22:20）/task-terminal-hooks（23:30）**同仓串行** →
  建议 **明天 18:00 晚间窗口**（今晚窗口已被同仓任务占满）
- 走 collab-flow 自身流程（decision→review→taskbook→translate→execute，复盘 checklist）

## 6. 关联

- 与 chain-on-transition（钩子）/task-terminal-hooks（超时收口）同属「链路畅通」体系：
  钩子解决「下一步没人触发」，本方案解决「verify 误报拖慢收尾」
- 落地后：今晚这类「手动补 allow 重验」不再需要——verify 自动豁免他任务产物

## 7. 细节优化（实现时全部纳入）

1. **快照与启动原子化**：快照在 runner 写 started_at 的同一处写（同一次 registry 写事务），
   避免「任务已启动但快照缺失」窗口；快照存独立文件 `.flow/tasks/<id>.snapshot.json`（registry 是并发写热点）
2. **快照成本优化**：untracked 只记清单不 hash（文件多但零成本）；仅 M 文件记 content-hash（数量少）；
   用 `git diff --name-only` + `git ls-files -o --exclude-standard` 一次拿全量，不逐文件 git 调用
3. **豁免报告可审计**：waived_scope 每项带原因（baseline_modified/baseline_untracked/prefix_match），
   出问题能查清为什么豁免；warning 列未被豁免的越界项
4. **多仓快照正确性**：快照在任务实际执行的工作目录拍（runner env 的 FLOW_WORKDIR 指向的仓，
   非 CLI 调用目录）——防跨仓拍错快照
5. **与 auto-advance 联动**：verify 修复后自动 verify（on_executed 钩子）不因交叉污染误打回 →
   自动重试环的风暴防护误触发率下降（间接收益）
6. **快照生命周期**：任务终态后快照保留（审计用），`flow task prune` 时随任务记录一起清
