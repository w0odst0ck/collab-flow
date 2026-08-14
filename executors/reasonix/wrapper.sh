#!/usr/bin/env bash
# reasonix executor wrapper —— collab-flow 默认执行器适配器(经 rx,§P3 §3.3)
#
# 契约(与 flow-core.py run_executor 唯一调用面):
#   <wrapper> --taskbook <path> --workdir <dir> --out <dir> [--timeout N]
# 职责: cd workdir → timeout rx run -y <taskbook> → redact sk-* → git diff → result.json
# 红线: key 不读不传不落盘(由执行节点本地 ~/.reasonix/.env 自读);日志过 sk- deny-list 防御性 redact。
set -uo pipefail
# 中间产物(.run.out/.run.err/.diff.*/.status)用完即清,不残留到 executor/ 工件目录
TMP_ARTIFACTS=()
cleanup() { [[ ${#TMP_ARTIFACTS[@]} -gt 0 ]] && rm -f "${TMP_ARTIFACTS[@]}"; }
trap cleanup EXIT   # 不 set -e;不 set -x(防 key)

TASKBOOK=""; WORKDIR=""; OUT=""; TIMEOUT="1800"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --taskbook) TASKBOOK="$2"; shift 2 ;;
    --workdir)  WORKDIR="$2"; shift 2 ;;
    --out)      OUT="$2"; shift 2 ;;
    --timeout)  TIMEOUT="$2"; shift 2 ;;
    *) shift ;;
  esac
done

[[ -n "$WORKDIR" && -n "$OUT" ]] || { echo "错误: 缺少 --workdir/--out" >&2; exit 2; }
[[ -f "$TASKBOOK" ]] || { echo "错误: taskbook 不存在: $TASKBOOK" >&2; exit 2; }

WORKDIR="$(readlink -f "$WORKDIR")"
OUT="$(readlink -f "$OUT")"
mkdir -p "$OUT" || { echo "错误: 无法创建输出目录: $OUT" >&2; exit 2; }

TASKBOOK_CONTENT="$(cat "$TASKBOOK")" || { echo "错误: 读取 taskbook 失败" >&2; exit 2; }
[[ -n "$TASKBOOK_CONTENT" ]] || { echo "错误: taskbook 为空" >&2; exit 2; }

cd "$WORKDIR" || { echo "错误: 无法进入 workdir: $WORKDIR" >&2; exit 2; }

# redact: 对文本过 sk- deny-list,命中替换 [REDACTED](无 python3 整段占位,绝不回退原文)
redact() {
  if command -v python3 >/dev/null 2>&1; then
    printf '%s' "$1" | python3 -c 'import sys,re; sys.stdout.write(re.sub(r"sk-[A-Za-z0-9]{10}", "[REDACTED]", sys.stdin.read()))'
  else
    printf '%s' "[REDACTED]"
  fi
}

STARTED_AT="$(date +"%Y-%m-%dT%H:%M:%S%:z")"
before=$(date +%s)
timeout "$TIMEOUT" rx run -y "$TASKBOOK_CONTENT" >"$OUT/.run.out" 2>"$OUT/.run.err"
rc=$?
DUR=$(( $(date +%s) - before ))
FINISHED_AT="$(date +"%Y-%m-%dT%H:%M:%S%:z")"

REDACTED_LOGS="$(redact "$(tail -c 2000 "$OUT/.run.err" 2>/dev/null)")"

# git diff(HEAD vs 工作树)+ --stat/--name-only + untracked
git diff HEAD >"$OUT/diff.patch" 2>/dev/null || true
git diff --stat >"$OUT/.diff.stat" 2>/dev/null || true
git diff --name-only >"$OUT/.diff.names" 2>/dev/null || true
git status --porcelain >"$OUT/.status" 2>/dev/null || true

STATUS="ok"
[[ $rc -eq 1 ]] && STATUS="failed"
[[ $rc -eq 124 ]] && STATUS="timeout"

python3 - "$OUT" "$STATUS" "$rc" "$DUR" "$STARTED_AT" "$FINISHED_AT" "$REDACTED_LOGS" <<'PY'
import json, sys, os, re
out, status, rc, dur, started, finished, logs = sys.argv[1:8]
changed = []
if os.path.isfile(os.path.join(out, ".diff.names")):
    changed = [l for l in open(os.path.join(out, ".diff.names")).read().splitlines() if l.strip()]
untracked = []
if os.path.isfile(os.path.join(out, ".status")):
    for l in open(os.path.join(out, ".status")).read().splitlines():
        if l[:2] == "??":
            untracked.append(l[3:])
ins = 0
dels = 0
if os.path.isfile(os.path.join(out, ".diff.stat")):
    for l in open(os.path.join(out, ".diff.stat")).read().splitlines():
        m = re.search(r"(\d+) insertion", l)
        if m:
            ins = int(m.group(1))
        m = re.search(r"(\d+) deletion", l)
        if m:
            dels = int(m.group(1))
for _p in (out + "/.run.out", out + "/.run.err", out + "/.diff.patch",
               out + "/.diff.stat", out + "/.diff.names", out + "/.status"):
    if os.path.isfile(_p):
        TMP_ARTIFACTS.append(_p)
rec = {"schema_version": 1, "executor": "reasonix", "status": status, "exit_code": int(rc),
       "duration_s": int(dur),
       "diff": {"files_changed": len(changed), "insertions": ins, "deletions": dels,
                "changed_files": changed, "untracked_files": untracked,
                "patch": "executor/diff.patch"},
       "test_command": None, "cost": None, "redacted_logs": logs,
       "started_at": started, "finished_at": finished}
with open(os.path.join(out, "result.json"), "w") as f:
    json.dump(rec, f, ensure_ascii=False, separators=(",", ":"))
PY

exit $rc
