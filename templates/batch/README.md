# 批处理任务模板库（script/local 层，2026-08-27）

> 用途：批量/后台任务的**任务书模板**——直接复制填内容即可。
> 执行器路由：含 `command:` 行 → script（零 LLM 秒级）；`model: qwen35-9b` → local（本地模型零 token）。
> 门禁：script/local 只放行 small/medium（large 拒绝）。

## 模板清单

| 模板 | 执行器 | 场景 |
|---|---|---|
| 翻译.md | local（think:false）| 批量翻译（中英/英中）|
| 格式化.md | script | 代码/文件格式化（ruff/black 等）|
| 数据清洗.md | script | CSV/JSON 清洗、去重、转换 |
| 图片处理.md | script | 批量缩放/转换（webp 等）|
| 摘要.md | local | 长文本/日志摘要 |

## 用法

```bash
cp templates/batch/翻译.md <项目>/taskbook.md
# 编辑 taskbook.md 填内容 → flow execute（自动路由 script/local）
```
