# memory/ — 项目记忆（蒸馏版）

> 原始记录已归档至 `archive/memory-2026-08/`（ocr-final/low/taskbook，审计可查）。
> 本目录只留**蒸馏后的教训**——新教训直接追加到本文档。

## 蒸馏教训（2026-08-28 蒸馏）

1. **ocr 分级收口**（08-25 固化）：high/medium 必修完才 commit；low 落盘 `memory/ocr-low-<日期>.md` 不阻塞；禁止为 low 无限循环修复。全量审模式对大批量代码不收敛（每轮出新 finding），收口标准 = high/medium 清零 + 用户拍板硬停。
2. **kill reasonix 必须连子进程**（08-25）：kill bash 包装 pid 只杀外层，子进程存活继续跑（peak 时段白烧钱）。正确：`pkill -f "reasonix run"`。
3. **长任务关键产物别放 /tmp**（08-25 重启教训）：/tmp 是 tmpfs 重启即清。关键中间产物放 workspace，/tmp 只放可再生成物。
4. **大文件搬迁先查 mock.patch 字符串路径**（08-28）：代码 import 改了，测试里 `mock.patch("intel.collectors.x")` 字符串不会跟着改——批量替换漏一处就 81 errors。

## 归档索引

| 归档文件 | 内容 |
|---|---|
| `archive/memory-2026-08/task-ocr-medium-fixes.md` | 08-27 ocr medium 三修任务书（已完成 cd21c8d） |
| `archive/memory-2026-08/ocr-final-2026-08-25.md/.json` | 08-25 ocr review 结果（原始） |
| `archive/memory-2026-08/ocr-low-2026-08-25.md` | 08-25 ocr low 留档 |
| `archive/memory-2026-08/ocr-low-2026-08-27.md` | 08-27 ocr low 留档（含 08-28 追加：tieba 正则挂账） |

---

*原始全在 archive/，这里只留"下次不再踩"的。*
5. **execute 前必须走完 review→translate（状态机门禁，08-24 复盘 + 08-29 重演）**：design 后直接入队 execute 必秒退 exit 2（guard 要求 translated + taskbook.md 非空）。流程：`transition reviewed`（decision pass 前置）→ 生成 taskbook.md → `transition translated` → 才入队 execute。**taskbook 不生成的两个原因**：① design.md 缺「改动文件清单」声明（extract_change_list fail-closed → auto_translate 拒生成）② 章节标题不匹配机械审查正则（`^#+\s*(?:\d+[.、])*\d*\s*(测试策略|测试|测试方案)` 等——中文数字/「单元测试」都不匹配）。先例优先：新流程先翻 `.flow/archive/<同类>/events.jsonl`。失败先看 `flow task status <id> --json` 的 failure_tail，别翻 log。
