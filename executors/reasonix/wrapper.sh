#!/usr/bin/env bash
# reasonix executor wrapper —— collab-flow 默认执行器适配器(经 rx,§P3 §3.3;超时韧性:design reasonix-executor-robustness)
#
# 契约(与 flow-core.py run_executor 唯一调用面):
#   <wrapper> --taskbook <path> --workdir <dir> --out <dir> [--timeout N] [--model NAME]
# 职责: cd workdir → 模型决策 → timeout(rx) run -y <taskbook> → redact sk-* → git diff → result.json
# 超时韧性:
#   - RX_TIMEOUT 跟随外层 --timeout N(单超时源,根治 rx 内层 600s 硬切)
#   - 外层 guard N+GRACE(--preserve-status): 124=内层 rx 超时(source=rx) / 143→exit 125=外层 guard(source=wrapper)
#   - 内层超时且 git diff 非空 → status=partial-complete(exit 仍 124)
#   - 失败/超时抓 .run.err+.run.out 最后 30 行过 deny-list 入 redacted_logs
# 红线: key 不读不传不落盘(由执行节点本地 ~/.reasonix/.env 自读);日志过 sk- deny-list 防御性 redact。
set -uo pipefail
# 中间产物(.run.out/.run.err/.diff.*/.status)用完即清,不残留到 executor/ 工件目录
TMP_ARTIFACTS=()
cleanup() { rm -f "${TMP_ARTIFACTS[@]}" 2>/dev/null; }
trap cleanup EXIT   # 不 set -e;不 set -x(防 key)

TASKBOOK=""; WORKDIR=""; OUT=""; TIMEOUT="1800"; MODEL=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --taskbook) TASKBOOK="$2"; shift 2 ;;
    --workdir)  WORKDIR="$2"; shift 2 ;;
    --out)      OUT="$2"; shift 2 ;;
    --timeout)  TIMEOUT="$2"; shift 2 ;;
    --model)    MODEL="$2"; shift 2 ;;
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
# 收紧 {10,}: 长于 10 的 key 尾部一并抹除({10,} 是 cmd_execute DENY_RE {10} 的超集)
redact() {
  if command -v python3 >/dev/null 2>&1; then
    printf '%s' "$1" | python3 -c 'import sys,re; sys.stdout.write(re.sub(r"sk-[A-Za-z0-9]{10,}", "[REDACTED]", sys.stdin.read()))'
  else
    printf '%s' "[REDACTED]"
  fi
}

# ---- 模型决策: --model(CLI,最优先) > 任务书字节数 >= 阈值 → pro > 默认 flash ----
RX_MODEL_DEFAULT="${RX_MODEL_DEFAULT:-deepseek-v4-flash}"
RX_MODEL_PRO="${RX_MODEL_PRO:-deepseek-v4-pro}"
RX_MODEL_PRO_THRESHOLD_BYTES="${RX_MODEL_PRO_THRESHOLD_BYTES:-8000}"
# 阈值 env 非法(非正整数)→ fail-closed 回退默认 8000
if ! [[ "$RX_MODEL_PRO_THRESHOLD_BYTES" =~ ^[0-9]+$ ]]; then
  RX_MODEL_PRO_THRESHOLD_BYTES=8000
fi
TASKBOOK_BYTES="$(wc -c < "$TASKBOOK")"
if [[ -n "$MODEL" ]]; then
  RX_MODEL="$MODEL"
elif (( TASKBOOK_BYTES >= RX_MODEL_PRO_THRESHOLD_BYTES )); then
  RX_MODEL="$RX_MODEL_PRO"
else
  RX_MODEL="$RX_MODEL_DEFAULT"
fi
export RX_MODEL

# ---- 单超时源: RX_TIMEOUT 跟随外层 N;guard N+GRACE 只防 rx 包装自身挂起 ----
export RX_TIMEOUT="$TIMEOUT"
GRACE="${RX_WRAPPER_GRACE_S:-30}"
# GRACE 越界 fail-closed:回到安全默认(G=0 → rc 双义;G>=60 → flow-core 的 N+60 先杀 wrapper 丢 result.json)
if ! [[ "$GRACE" =~ ^[0-9]+$ ]] || (( GRACE < 1 || GRACE > 50 )); then
  GRACE=30
fi
GUARD=$(( TIMEOUT + GRACE ))

# --preserve-status 探测:无该选项的 timeout(BusyBox 等)回退 timing 阈值判定(E13 边界);
# 支持时叠加 -k KILL_AFTER,防 rx 忽略 SIGTERM 时挂死(KILL_AFTER=5 使 guard 总时长 ≤ N+55 < flow-core 的 N+60)
PS_FLAG=""; KILL_AFTER=""
if timeout --preserve-status true >/dev/null 2>&1; then
  PS_FLAG="--preserve-status"
  if timeout --preserve-status -k 1 true >/dev/null 2>&1; then
    KILL_AFTER="-k 5"
  fi
fi

STARTED_AT="$(date +"%Y-%m-%dT%H:%M:%S%:z")"
before=$(date +%s)
# rx 接口: rx [--pro] <项目目录> "任务书"(rx 内部自行 cd + 调 reasonix run -y);
# 不能用 `rx run -y`(会把 run 当项目目录)。pro 经 --pro 标志(rx 不读 RX_MODEL 的规避);
# RX_BIN 可注入 stub(测试零 API,同 DSH_BIN 模式)。
RX_ARGS=()
[[ "$RX_MODEL" == "$RX_MODEL_PRO" ]] && RX_ARGS+=(--pro)
timeout $PS_FLAG $KILL_AFTER "$GUARD" "${RX_BIN:-rx}" "${RX_ARGS[@]}" "$WORKDIR" "$TASKBOOK_CONTENT" \
        >"$OUT/.run.out" 2>"$OUT/.run.err"
rc=$?
DUR=$(( $(date +%s) - before ))
FINISHED_AT="$(date +"%Y-%m-%dT%H:%M:%S%:z")"

# 无 --preserve-status 回退: elapsed >= GUARD → 判外层 guard(143),否则内层 rx
if [[ -z "$PS_FLAG" && $rc -eq 124 && $DUR -ge $GUARD ]]; then
  rc=143
fi

# ---- 退出码区分: 0 ok / 1 failed / 124 内层 rx 超时 / 143→exit 125 外层 guard / * 透传 ----
STATUS="ok"; EXIT=0; TIMEOUT_SOURCE=""
case $rc in
  0)   STATUS="ok" ;;
  1)   STATUS="failed"; EXIT=1 ;;
  124) STATUS="timeout"; TIMEOUT_SOURCE="rx"; EXIT=124 ;;
  143) STATUS="timeout"; TIMEOUT_SOURCE="wrapper"; EXIT=125 ;;
  *)   STATUS="failed"; EXIT=$rc ;;
esac

# git diff(HEAD vs 工作树)+ --stat/--name-only + untracked
# 独立 git 仓判定: rev-parse --show-toplevel 必须恰好等于 workdir(不得向上匹配,
# 否则 workspace 树内项目会误判为仓导致 diff 污染 workspace)。非仓时按目录时间戳扫描产出物。
TOPLVL="$(git -C "$WORKDIR" rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -n "$TOPLVL" && "$(readlink -f "$TOPLVL")" == "$WORKDIR" ]]; then
  git -C "$WORKDIR" diff HEAD >"$OUT/diff.patch" 2>/dev/null || true
  git -C "$WORKDIR" diff --stat >"$OUT/.diff.stat" 2>/dev/null || true
  git -C "$WORKDIR" diff --name-only >"$OUT/.diff.names" 2>/dev/null || true
  git -C "$WORKDIR" status --porcelain >"$OUT/.status" 2>/dev/null || true
else
  : >"$OUT/diff.patch"; : >"$OUT/.diff.stat"; : >"$OUT/.status"
  find "$WORKDIR/intel" -type f \( -name '*.py' -o -name '*.md' -o -name '*.json' -o -name '*.jsonl' -o -name '*.toml' \) 2>/dev/null | sed "s|$WORKDIR/||" | sort >"$OUT/.diff.names" || : >"$OUT/.diff.names"
fi

# ---- 超时抢救: 仅「内层超时(rc=124)且有产出」→ partial-complete(exit 仍 124) ----
# 产出判定: git 模式看 diff.patch 非空;非 git 模式看 .diff.names 非空(intel 目录时间戳扫描)
# (非 git 模式 diff.patch 恒空文件,若只看它 partial-complete 永不触发——ocr 2026-08-21)
PARTIAL_COMPLETE="false"
if [[ "$STATUS" == "timeout" && "$TIMEOUT_SOURCE" == "rx" ]] \
   && { [[ -s "$OUT/diff.patch" ]] || [[ -s "$OUT/.diff.names" ]]; }; then
  STATUS="partial-complete"
  PARTIAL_COMPLETE="true"
fi

# ---- 诊断抓取: 失败/超时抓 .run.err+.run.out 最后 30 行过 deny-list;无输出回退占位 ----
REDACTED_LOGS=""
if [[ "$STATUS" != "ok" ]]; then
  RAW_LOGS="$( { tail -n 30 "$OUT/.run.err" 2>/dev/null; tail -n 30 "$OUT/.run.out" 2>/dev/null; } )"
  REDACTED_LOGS="$(redact "$RAW_LOGS")"
  [[ -n "$REDACTED_LOGS" ]] || REDACTED_LOGS="(no rx output)"
fi

python3 - "$OUT" "$STATUS" "$EXIT" "$DUR" "$STARTED_AT" "$FINISHED_AT" "$REDACTED_LOGS" \
         "$RX_MODEL" "${TIMEOUT_SOURCE:-}" "$TIMEOUT" "$PARTIAL_COMPLETE" <<'PY'
import json, sys, os, re
out, status, rc, dur, started, finished, logs = sys.argv[1:8]
model, timeout_source, rx_timeout_s, partial = sys.argv[8:12]
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
for _p in (out + "/.run.out", out + "/.run.err", out + "/.diff.stat", out + "/.diff.names", out + "/.status"):
    if os.path.isfile(_p):
        pass  # 中间产物由 bash cleanup 清理;此处不得引用 bash 数组(会 NameError)
rec = {"schema_version": 1, "executor": "reasonix", "status": status, "exit_code": int(rc),
       "timeout_source": (timeout_source or None), "model": model,
       "rx_timeout_s": int(rx_timeout_s), "partial_complete": (partial == "true"),
       "duration_s": int(dur),
       "diff": {"files_changed": len(changed), "insertions": ins, "deletions": dels,
                "changed_files": changed, "untracked_files": untracked,
                "patch": "executor/diff.patch"},
       "test_command": None, "cost": None, "redacted_logs": logs,
       "started_at": started, "finished_at": finished}
with open(os.path.join(out, "result.json"), "w") as f:
    json.dump(rec, f, ensure_ascii=False, separators=(",", ":"))
PY

exit $EXIT
