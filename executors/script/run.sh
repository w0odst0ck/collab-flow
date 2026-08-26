#!/usr/bin/env bash
# script-executor：确定性命令执行器（零 LLM，秒级，批处理首选）
#
# 用法:
#   <wrapper> --taskbook <路径> --workdir <目录> --out <目录> [--timeout N]
#
# 任务书格式（frontmatter 或正文首行）:
#   command: <shell 命令>          # 必填：要执行的确定性命令
#   timeout: 60                    # 可选：覆盖默认超时
#
# 安全（fail-closed）:
#   - 黑名单危险命令 → 拒绝（exit 4），不执行
#   - 仅 workdir 内执行（cd workdir）
#   - 超时兜底（默认 300s；--timeout/任务书 timeout 可覆盖）
#   - 输出临时文件随清理（不留盘）
#
# 产物（与 local/reasonix 同契约）:
#   result.md   —— 命令 stdout
#   result.json —— {schema_version, executor, status, exit_code, command, duration_s, ...}
set -u

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"

TASKBOOK=""; WORKDIR=""; OUT=""; TIMEOUT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --taskbook) TASKBOOK="$2"; shift 2 ;;
    --workdir)  WORKDIR="$2"; shift 2 ;;
    --out)      OUT="$2"; shift 2 ;;
    --timeout)  TIMEOUT="$2"; shift 2 ;;
    *) echo "错误: 未知参数 $1" >&2; exit 1 ;;
  esac
done
[[ -n "$TASKBOOK" && -n "$WORKDIR" && -n "$OUT" ]] \
  || { echo "错误: 缺少 --taskbook/--workdir/--out" >&2; exit 1; }
[[ -f "$TASKBOOK" ]] || { echo "错误: 任务书不存在: $TASKBOOK" >&2; exit 1; }
OUT="$(readlink -f "$OUT")"
mkdir -p "$OUT" || { echo "错误: 无法创建输出目录: $OUT" >&2; exit 1; }
WORKDIR="$(readlink -f "$WORKDIR")"
[[ -d "$WORKDIR" ]] || { echo "错误: workdir 不存在: $WORKDIR" >&2; exit 1; }

TMP_ARTIFACTS=()
cleanup() { rm -f "${TMP_ARTIFACTS[@]}" 2>/dev/null; }
trap cleanup EXIT

# ── 解析任务书: command 行 + timeout 覆盖 ──
TB_CONTENT="$(cat "$TASKBOOK")"
TB_CMD="$(printf '%s\n' "$TB_CONTENT" | sed -nE 's/^[[:space:]]*command:[[:space:]]*(.+)$/\1/p' | head -1)"
TB_TIMEOUT="$(printf '%s\n' "$TB_CONTENT" | sed -nE 's/^[[:space:]]*timeout:[[:space:]]*([0-9]+)[[:space:]]*(#.*)?$/\1/p' | head -1)"
[[ -n "$TB_CMD" ]] || { echo "错误: 任务书缺少 command: 行" >&2; exit 1; }

TIMEOUT="${TIMEOUT:-${TB_TIMEOUT:-300}}"
if ! [[ "$TIMEOUT" =~ ^[0-9]+$ ]] || (( TIMEOUT < 1 )); then
  echo "错误: --timeout 必须是正整数: $TIMEOUT" >&2; exit 1
fi

# ── 黑名单检查（fail-closed）──
if printf '%s' "$TB_CMD" | grep -qE 'rm[[:space:]]+-rf|mkfs\.|dd[[:space:]]+.*of=/dev/|sudo|su[[:space:]]+|chown[[:space:]]+-R[[:space:]]+/|:[[:space:]]*\(\)[[:space:]]*\{|shutdown|reboot|>[/ ]dev/sd|mkfs'; then
  echo "错误: 命令命中危险黑名单，已拒绝（exit 4）: $TB_CMD" >&2
  exit 4
fi

# ── 执行（workdir 内 + 超时兜底）──
STARTED_AT="$(date +"%Y-%m-%dT%H:%M:%S%:z")"
before=$(date +%s)
( cd "$WORKDIR" && timeout "$TIMEOUT" bash -c "$TB_CMD" ) > "$OUT/.run.out" 2> "$OUT/.run.err"
rc=$?
TMP_ARTIFACTS+=("$OUT/.run.out" "$OUT/.run.err")
DUR="$(awk -v b="$before" -v n="$(date +%s)" 'BEGIN { printf "%.1f", n - b }')"
FINISHED_AT="$(date +"%Y-%m-%dT%H:%M:%S%:z")"

# ── 状态判定 ──
STATUS="ok"; EXIT_CODE="$rc"; REDACTED_LOGS=""
if [[ $rc -eq 124 ]]; then
  STATUS="timeout"; EXIT_CODE=124; REDACTED_LOGS="(命令执行超时 ${TIMEOUT}s)"
elif [[ $rc -ne 0 ]]; then
  STATUS="failed"; EXIT_CODE=$rc
  REDACTED_LOGS="$(tail -n 3 "$OUT/.run.err" 2>/dev/null | head -c 300)"
fi

# ── 产物 ──
cp "$OUT/.run.out" "$OUT/result.md" 2>/dev/null || : > "$OUT/result.md"
python3 - "$OUT" "$TB_CMD" "$STATUS" "$EXIT_CODE" "$DUR" "$STARTED_AT" "$FINISHED_AT" "$REDACTED_LOGS" <<'PY' > "$OUT/result.json"
import json, os, sys
out, cmd, status, code, dur, st, ft, err = sys.argv[1:9]
out = os.path.basename(out.rstrip("/"))
rec = {
    "schema_version": 1,
    "executor": "script",
    "status": status,
    "exit_code": int(code),
    "command": cmd[:500],          # 脱敏截断（命令可能含路径）
    "duration_s": float(dur),
    "started_at": st,
    "finished_at": ft,
    "output_file": "result.md",
    "workdir": "(workdir)",
    "error": err or "",            # 失败/超时时的错误摘要（ocr low：REDACTED_LOGS 落盘）
}
json.dump(rec, sys.stdout, ensure_ascii=False, indent=2)
PY

# 清理 .run.err 残留（失败时已摘录进日志）
rm -f "$OUT/.run.err"
echo "script executor: status=$STATUS exit_code=$EXIT_CODE duration_s=${DUR}s"
[[ "$STATUS" == "ok" ]] && exit 0 || exit 2
