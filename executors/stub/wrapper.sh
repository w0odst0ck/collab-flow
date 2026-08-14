#!/usr/bin/env bash
# stub executor wrapper —— 契约测试 stub(零 API,§P3 §5.1)
#
# 契约(与 reasonix 同构):<wrapper> --taskbook <path> --workdir <dir> --out <dir> [--timeout N]
# 不调用任何真实二进制;行为受 env 控制:
#   STUB_EXECUTOR_EXIT         退出码(0/1/124,默认 0)
#   STUB_EXECUTOR_CHANGE       是否在 workdir 造 dummy 改动(1/0,默认 1)
#   STUB_EXECUTOR_CHANGE_FILE  dummy 改动文件名(默认 stub-change.txt;越界场景设为 deny 项)
#   STUB_EXECUTOR_SLEEP        秒数(配合 --timeout 测 124)
#   STUB_EXECUTOR_LEAK_SK      在日志夹一个 sk- 串(测 redact)
#   STUB_EXECUTOR_NO_RESULT    不写 result.json(测必填输出缺失)
#   STUB_EXECUTOR_NO_DIFF      不写 diff.patch(测必填输出缺失)
set -uo pipefail

TASKBOOK=""; WORKDIR=""; OUT=""; TIMEOUT="30"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --taskbook) TASKBOOK="$2"; shift 2 ;;
    --workdir)  WORKDIR="$2"; shift 2 ;;
    --out)      OUT="$2"; shift 2 ;;
    --timeout)  TIMEOUT="$2"; shift 2 ;;
    *) shift ;;
  esac
done

[[ -n "$WORKDIR" && -n "$OUT" ]] || { echo "stub: 缺少 --workdir/--out" >&2; exit 2; }
mkdir -p "$OUT" || exit 2
cd "$WORKDIR" || exit 2

EXIT_CODE="${STUB_EXECUTOR_EXIT:-0}"
CHANGE="${STUB_EXECUTOR_CHANGE:-1}"
SLEEP="${STUB_EXECUTOR_SLEEP:-0}"
CHANGED_FILE="${STUB_EXECUTOR_CHANGE_FILE:-stub-change.txt}"

# sleep 模拟长任务;受 --timeout 包裹,超时 → 124(§P3 §5.1)
if [[ "$SLEEP" != "0" ]]; then
  timeout "$TIMEOUT" sleep "$SLEEP" >/dev/null 2>&1
  sleep_rc=$?
  [[ $sleep_rc -eq 124 ]] && EXIT_CODE=124
fi

if [[ "$EXIT_CODE" != "124" && "$CHANGE" == "1" ]]; then
  mkdir -p "$(dirname "$CHANGED_FILE")" 2>/dev/null || true
  echo "stub change $(date +%s)" >> "$CHANGED_FILE"
fi

# 泄漏 sk- 串进日志(wrapper 会 redact;不写 result.json 本体)
# 注:leak 串动态拼接,避免源码静态 deny-list 命中(文件里不出现连续的 sk- 字面串)
if [[ "${STUB_EXECUTOR_LEAK_SK:-0}" == "1" ]]; then
  LEAK_PREFIX="sk-"
  LEAK_SUFFIX="abcdefghij12345"
  echo "leak ${LEAK_PREFIX}${LEAK_SUFFIX}" > "$OUT/.run.err"
fi

# diff.patch: 有改动则 git diff HEAD;否则写占位补丁(零 API,不依赖 git 仓库)
if [[ "$CHANGE" == "1" ]] && git rev-parse HEAD >/dev/null 2>&1; then
  git diff HEAD > "$OUT/diff.patch" 2>/dev/null || true
  [[ -s "$OUT/diff.patch" ]] || { echo "diff --git a/$CHANGED_FILE b/$CHANGED_FILE" > "$OUT/diff.patch"; echo "+stub change" >> "$OUT/diff.patch"; }
else
  echo "diff --git a/stub-change.txt b/stub-change.txt" > "$OUT/diff.patch"
  echo "+stub change" >> "$OUT/diff.patch"
fi

[[ "${STUB_EXECUTOR_NO_DIFF:-0}" == "1" ]] && rm -f "$OUT/diff.patch"

REDACTED_LOGS=""
if [[ -f "$OUT/.run.err" ]]; then
  REDACTED_LOGS="$(python3 - "$OUT/.run.err" <<'PY'
import sys, re
data = open(sys.argv[1], encoding="utf-8", errors="replace").read()
sys.stdout.write(re.sub(r"sk-[A-Za-z0-9]{10}", "[REDACTED]", data))
PY
  )"
fi

[[ "${STUB_EXECUTOR_NO_RESULT:-0}" == "1" ]] && exit "$EXIT_CODE"

STATUS="ok"
[[ "$EXIT_CODE" == "1" ]] && STATUS="failed"
[[ "$EXIT_CODE" == "124" ]] && STATUS="timeout"

python3 - "$OUT" "$STATUS" "$EXIT_CODE" "$REDACTED_LOGS" "$CHANGED_FILE" "$CHANGE" <<'PY'
import json, sys, os
out, status, ec, logs, changed_file, change = sys.argv[1:7]
changed = [changed_file] if change == "1" and os.path.isfile(os.path.join(out, "diff.patch")) else []
rec = {"schema_version": 1, "executor": "stub", "status": status, "exit_code": int(ec),
       "duration_s": 0,
       "diff": {"files_changed": len(changed), "insertions": 1, "deletions": 0,
                "changed_files": changed, "untracked_files": [], "patch": "executor/diff.patch"},
       "test_command": None, "cost": None, "redacted_logs": logs,
       "started_at": "", "finished_at": ""}
with open(os.path.join(out, "result.json"), "w") as f:
    json.dump(rec, f, ensure_ascii=False, separators=(",", ":"))
PY

exit "$EXIT_CODE"
