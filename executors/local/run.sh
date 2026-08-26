#!/usr/bin/env bash
# local executor —— 本地模型批处理执行器(ollama /api/chat,local-executor 新增)
#
# 契约(与 executors/reasonix/wrapper.sh 同构,flow-core.py run_executor 唯一调用面):
#   <wrapper> --taskbook <path> --workdir <dir> --out <dir> [--timeout N] [--think true|false] [--model NAME]
# 职责: 模型决策(--model > 任务书 model: 声明(注册表任意名) > 默认 qwen35-9b)
#       → ollama /api/chat → result.md(模型回复) + result.json(元数据)
# 退出码: 0 成功 / 1 用法/前置错误 / 2 ollama 不可达(curl 失败或超时,可重试)
#         / 3 模型回复为空 / 124 外层 guard 超时(防 wrapper 挂起)
# 红线: 日志只记录 model/耗时/token 数,绝不打印任务书内容与模型回复;
#       任务书内容仅经请求体发往本机 ollama,不落日志、不回显。
set -uo pipefail
TMP_ARTIFACTS=()
cleanup() { rm -f "${TMP_ARTIFACTS[@]}" 2>/dev/null; }
trap cleanup EXIT   # 不 set -e;不 set -x(防内容入日志)

TASKBOOK=""; WORKDIR=""; OUT=""; TIMEOUT=""; THINK=""; MODEL=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --taskbook) TASKBOOK="$2"; shift 2 ;;
    --workdir)  WORKDIR="$2"; shift 2 ;;
    --out)      OUT="$2"; shift 2 ;;
    --timeout)  TIMEOUT="$2"; shift 2 ;;
    --think)    THINK="$2"; shift 2 ;;
    --model)    MODEL="$2"; shift 2 ;;
    *) shift ;;
  esac
done

# ── 前置校验(fail-closed):缺参/不可读 → 用法错 exit 1 ──
[[ -n "$TASKBOOK" && -n "$WORKDIR" && -n "$OUT" ]] \
  || { echo "错误: 缺少 --taskbook/--workdir/--out" >&2; exit 1; }
[[ -f "$TASKBOOK" ]] || { echo "错误: taskbook 不存在: $TASKBOOK" >&2; exit 1; }
OUT="$(readlink -f "$OUT")"
mkdir -p "$OUT" || { echo "错误: 无法创建输出目录: $OUT" >&2; exit 1; }
TASKBOOK_CONTENT="$(cat "$TASKBOOK")" || { echo "错误: 读取 taskbook 失败" >&2; exit 1; }
[[ -n "$TASKBOOK_CONTENT" ]] || { echo "错误: taskbook 为空" >&2; exit 1; }

# ── 模型决策: --model(CLI) > 任务书 model: 声明(注册表任意名) > 默认 qwen35-9b ──
# 注册表名 → ollama 模型名(本地小映射;未知名原样当 ollama 模型名,
# 注册表校验由 flow-core resolve_model 在调用前 fail-closed 完成)
local_model_name() {
  case "$1" in
    qwen35-9b) printf '%s' "qwen3.5:9b" ;;
    qwen3-8b)  printf '%s' "qwen3:8b" ;;
    *)         printf '%s' "$1" ;;
  esac
}
TB_MODEL="$(printf '%s\n' "$TASKBOOK_CONTENT" \
  | sed -nE 's/^[[:space:]]*model:[[:space:]]*([A-Za-z0-9._-]+)[[:space:]]*(#.*)?$/\1/p' \
  | head -1)"
if [[ -n "$MODEL" ]]; then
  MODEL_NAME="$MODEL"
elif [[ -n "$TB_MODEL" ]]; then
  MODEL_NAME="$TB_MODEL"
else
  MODEL_NAME="${LOCAL_MODEL_DEFAULT:-qwen35-9b}"
fi
OLLAMA_MODEL="$(local_model_name "$MODEL_NAME")"

# ── think 决策: --think(CLI) > 任务书 think: true|false > 默认 true(可关:翻译/机械任务) ──
# CLI 值非法 → 用法错(fail-closed);任务书声明非法值 → 忽略回默认(与 wrapper 阈值回退同风格)
TB_THINK="$(printf '%s\n' "$TASKBOOK_CONTENT" \
  | sed -nE 's/^[[:space:]]*think:[[:space:]]*(true|false)[[:space:]]*(#.*)?$/\1/p' \
  | head -1)"
THINK_VALUE="true"
if [[ -n "$THINK" ]]; then
  case "$THINK" in
    true|True|TRUE|1)  THINK_VALUE="true" ;;
    false|False|FALSE|0) THINK_VALUE="false" ;;
    *) echo "错误: --think 必须是 true|false,实际: $THINK" >&2; exit 1 ;;
  esac
elif [[ -n "$TB_THINK" ]]; then
  THINK_VALUE="$TB_THINK"
fi

# ── 超时: 外层 guard 防 curl 挂起(curl --max-time 已限内层,guard 兜底) ──
TIMEOUT="${TIMEOUT:-1200}"
if ! [[ "$TIMEOUT" =~ ^[0-9]+$ ]] || (( TIMEOUT < 1 )); then
  echo "错误: --timeout 必须是正整数: $TIMEOUT" >&2; exit 1
fi
GRACE="${LOCAL_WRAPPER_GRACE_S:-30}"
if ! [[ "$GRACE" =~ ^[0-9]+$ ]] || (( GRACE < 1 || GRACE > 50 )); then
  GRACE=30
fi
GUARD=$(( TIMEOUT + GRACE ))

OLLAMA_URL="${OLLAMA_URL:-http://127.0.0.1:11434}"
CURL_BIN="${LOCAL_CURL_BIN:-curl}"

# ── 请求体: python3 json 构造(任务书先落临时文件,防 shell 转义/命令行泄露) ──
TB_FILE="$OUT/.taskbook.txt"
printf '%s' "$TASKBOOK_CONTENT" > "$TB_FILE"
TMP_ARTIFACTS+=("$TB_FILE")
PAYLOAD="$OUT/.payload.json"
TMP_ARTIFACTS+=("$PAYLOAD")
python3 - "$OLLAMA_MODEL" "$THINK_VALUE" "$TB_FILE" > "$PAYLOAD" <<'PY' || { echo "错误: 请求体构造失败" >&2; exit 1; }
import json, sys
model, think, tb_file = sys.argv[1], sys.argv[2], sys.argv[3]
body = {
    "model": model,
    "messages": [{"role": "user", "content": open(tb_file, encoding="utf-8").read()}],
    # think 是 ollama 顶层参数（Qwen3 思考模式开关），放 options 内不生效（2026-08-26 实测 5.75 t/s）
    "think": think == "true",
    "options": {"num_predict": 4096, "num_ctx": 8192},
    "keep_alive": "30m",
}
json.dump(body, sys.stdout)
PY

STARTED_AT="$(date +"%Y-%m-%dT%H:%M:%S%:z")"
before=$(date +%s)
# -sS: 静默但保留错误;--max-time 超时 → curl rc=28 → 按不可达处理(exit 2)
timeout "$GUARD" "$CURL_BIN" -sS --max-time "$TIMEOUT" \
  -H "Content-Type: application/json" \
  -d@"$PAYLOAD" "$OLLAMA_URL/api/chat" > "$OUT/.run.out" 2> "$OUT/.run.err"
  TMP_ARTIFACTS+=("$OUT/.run.out" "$OUT/.run.err")  # ocr low：原始响应临时文件随清理（红线：模型回复不留盘）
rc=$?
DUR=$(awk -v b="$before" -v n="$(date +%s)" 'BEGIN { printf "%.1f", n - b }')
FINISHED_AT="$(date +"%Y-%m-%dT%H:%M:%S%:z")"

# ── 状态判定 ──
STATUS="failed"; EXIT_CODE="$rc"; REDACTED_LOGS=""
if [[ $rc -eq 124 ]]; then
  STATUS="timeout"      # 外层 guard 超时(防挂起;exit 124 与 reasonix 同语义)
  EXIT_CODE=124
  REDACTED_LOGS="(ollama 调用超时)"
elif [[ $rc -ne 0 ]]; then
  STATUS="failed"; EXIT_CODE=2   # curl 失败/超时 → ollama 不可达,可重试
  REDACTED_LOGS="$(tail -n 5 "$OUT/.run.err" 2>/dev/null | head -c 500)"
  [[ -n "$REDACTED_LOGS" ]] || REDACTED_LOGS="(ollama 不可达,无错误输出)"
fi

# ── 响应解析: content + token 计数(curl 成功时);result.md 由 python 直写原始内容 ──
CONTENT=""; TOKENS=0; PROMPT_TOKENS=0
if [[ $rc -eq 0 ]]; then
  # python 输出三行: content 的 JSON 编码(单行) / eval_count / prompt_eval_count;
  # content 为空 → 输出 __EMPTY__(不写 result.md);解析失败 → __BAD_JSON__ → 不可达(exit 2)
  mapfile -t PARSE < <(
    python3 - "$OUT" "$OUT/.run.out" <<'PY' || echo "__BAD_JSON__"
import json, os, sys
out, resp = sys.argv[1], sys.argv[2]
try:
    d = json.load(open(resp, encoding="utf-8"))
    content = (d.get("message") or {}).get("content") or ""
    if content:
        with open(os.path.join(out, "result.md"), "w", encoding="utf-8") as f:
            f.write(content + "\n")
        print(json.dumps(content))
    else:
        print("__EMPTY__")
    print(int(d.get("eval_count", 0) or 0))
    print(int(d.get("prompt_eval_count", 0) or 0))
except Exception:
    print("__BAD_JSON__")
PY
  )
  if [[ "${PARSE[0]:-}" == "__BAD_JSON__" || ${#PARSE[@]} -lt 3 ]]; then
    STATUS="failed"; EXIT_CODE=2; REDACTED_LOGS="(ollama 响应非预期 JSON,不可重试判定为不可达)"
    rc=2
  elif [[ "${PARSE[0]:-}" == "__EMPTY__" ]]; then
    STATUS="failed"; EXIT_CODE=3; REDACTED_LOGS="(模型回复为空)"
    rc=3
  else
    CONTENT="${PARSE[0]}"; TOKENS="${PARSE[1]}"; PROMPT_TOKENS="${PARSE[2]}"
  fi
fi

# ── 落盘: result.md + result.json(成功/空/失败均写,供 flow-core 与诊断) ──
# (生成空已在上方判定:status=failed exit=3;成功路径 result.md 已由 python 写入)
if [[ $rc -eq 0 && -n "$CONTENT" ]]; then
  STATUS="ok"; EXIT_CODE=0
fi

# diff.patch 占位(local 不产出代码 diff;空文件满足 flow-core execute st==ok 契约检查)
: > "$OUT/diff.patch"

python3 - "$OUT" "$STATUS" "$EXIT_CODE" "$DUR" "$STARTED_AT" "$FINISHED_AT" \
         "$OLLAMA_MODEL" "$MODEL_NAME" "$THINK_VALUE" "$TOKENS" "$PROMPT_TOKENS" \
         "$REDACTED_LOGS" "$TASKBOOK" <<'PY'
import json, os, sys
out, status, code, dur, started, finished = sys.argv[1:7]
model, model_name, think, tokens, prompt_tokens, logs, taskbook = sys.argv[7:14]
rec = {
    "schema_version": 1,
    "executor": "local",
    "status": status,
    "exit_code": int(code),
    "model": model,               # ollama 模型名(实际调用)
    "model_name": model_name,     # 注册表名/CLI 名(决策输入)
    "think": think == "true",
    "tokens": int(tokens),        # 生成 token 数(eval_count)
    "prompt_tokens": int(prompt_tokens),
    "duration_s": float(dur),
    "taskbook": os.path.basename(taskbook),
    "diff": {"files_changed": 0, "insertions": 0, "deletions": 0,
             "changed_files": [], "untracked_files": [],
             "patch": "executor/diff.patch"},
    "redacted_logs": logs,
    "started_at": started,
    "finished_at": finished,
}
with open(os.path.join(out, "result.json"), "w", encoding="utf-8") as f:
    json.dump(rec, f, ensure_ascii=False, separators=(",", ":"))
PY

# 日志: 只记录 model/耗时/token 数(零个人信息)
echo "local executor: model=$OLLAMA_MODEL duration_s=$DUR tokens=$TOKENS status=$STATUS" >&2
exit "$EXIT_CODE"
