#!/usr/bin/env bash
# flow CLI 冒烟测试(C1-C18,stub 零 API;方案 §6.3)
# 用法: bash tests/run-flow-smoke.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
FLOW="$ROOT_DIR/scripts/flow"
PASS=0; FAIL=0; FAILED_CASES=()

ok()  { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad() { FAIL=$((FAIL+1)); FAILED_CASES+=("$1"); echo "  ❌ $1 — $2"; }

# ---------- 公共准备(隔离 HOME / 临时 FLOW_DATA_DIR) ----------
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/home"
DATA="$WORK/.flow"
BRIEF_SRC="$WORK/brief-src.md"
echo "# 简报内容 for w2" > "$BRIEF_SRC"

run() { # run [args...]  —— 统一注入隔离 env
  env HOME="$WORK/home" FLOW_DATA_DIR="$DATA" COLLABFLOW_CONFIG="$WORK/no-cfg.yaml" "$FLOW" "$@"
}

# 单行有效 JSON 校验
json_ok() {
  printf '%s' "$1" | python3 -c 'import sys,json; json.loads(sys.stdin.read())' 2>/dev/null
}

echo "== C1: new w1 --json =="
OUT="$(run workitem new w1 --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && json_ok "$OUT" >/dev/null \
   && [[ $(printf '%s' "$OUT" | grep -c .) -eq 1 ]] \
   && echo "$OUT" | grep -q '"state":"created"' \
   && [[ -d "$DATA/workitems/w1" && -f "$DATA/workitems/w1/brief.md" \
      && -f "$DATA/workitems/w1/status.yaml" && -f "$DATA/workitems/w1/events.jsonl" ]]; then
  ok "exit0+单行JSON+四工件+created"
else
  bad "C1" "rc=$RC out=$OUT"
fi

echo "== C2: new W1 / new ../x =="
OUT="$(run workitem new W1 --json 2>&1)"; RC=$?
if [[ $RC -eq 2 ]]; then ok "大写 id exit2"; else bad "C2a" "rc=$RC out=$OUT"; fi
OUT="$(run workitem new ../x --json 2>&1)"; RC=$?
if [[ $RC -eq 2 ]]; then ok "路径穿越 exit2"; else bad "C2b" "rc=$RC out=$OUT"; fi

echo "== C3: 重复 new w1(无 --force) =="
OUT="$(run workitem new w1 --json 2>&1)"; RC=$?
if [[ $RC -eq 2 ]]; then ok "重复 id exit2"; else bad "C3" "rc=$RC out=$OUT"; fi

echo "== C4: new w2 --brief FILE =="
OUT="$(run workitem new w2 --brief "$BRIEF_SRC" --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && grep -q "简报内容 for w2" "$DATA/workitems/w2/brief.md"; then
  ok "brief 内容复制"
else
  bad "C4" "rc=$RC brief=$(cat "$DATA/workitems/w2/brief.md" 2>/dev/null)"
fi

echo "== C5: status w1 --json =="
OUT="$(run workitem status w1 --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"state":"created"'; then
  ok "status state=created"
else
  bad "C5" "rc=$RC out=$OUT"
fi

echo "== C6: status nope =="
OUT="$(run workitem status nope --json 2>&1)"; RC=$?
if [[ $RC -eq 2 ]]; then ok "不存在 exit2"; else bad "C6" "rc=$RC out=$OUT"; fi

echo "== C7: transition designed(0) → reviewed(exit1) =="
OUT="$(run workitem transition w1 designed --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]]; then ok "designed exit0"; else bad "C7a" "rc=$RC out=$OUT"; fi
OUT="$(run workitem transition w1 reviewed --json 2>&1)"; RC=$?
if [[ $RC -eq 1 ]] && echo "$OUT" | grep -q "design_required"; then
  ok "reviewed exit1(design_required)"
else
  bad "C7b" "rc=$RC out=$OUT"
fi

echo "== C8: 写 design.md + decision pass → reviewed → translated =="
printf '# design\n' > "$DATA/workitems/w1/design.md"
OUT="$(run workitem decision w1 --verdict pass --json 2>&1)"; RC=$?
[[ $RC -eq 0 ]] || bad "C8a" "decision rc=$RC out=$OUT"
OUT="$(run workitem transition w1 reviewed --json 2>&1)"; RC=$?
[[ $RC -eq 0 ]] || bad "C8b" "reviewed rc=$RC out=$OUT"
OUT="$(run workitem transition w1 translated --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"to":"translated"'; then
  ok "质量门过 → translated"
else
  bad "C8c" "rc=$RC out=$OUT"
fi

echo "== C9: transition accepted(在 translated) =="
OUT="$(run workitem transition w1 accepted --json 2>&1)"; RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "illegal_transition"; then
  ok "illegal_transition exit2"
else
  bad "C9" "rc=$RC out=$OUT"
fi

echo "== C10: decision reject → transition translated 拒(exit1) =="
run workitem transition w2 designed --json >/dev/null 2>&1
printf '# design\n' > "$DATA/workitems/w2/design.md"
run workitem decision w2 --verdict reject --defect-type missing_scenario --json >/dev/null 2>&1
run workitem transition w2 reviewed --json >/dev/null 2>&1
OUT="$(run workitem transition w2 translated --json 2>&1)"; RC=$?
if [[ $RC -eq 1 ]] && echo "$OUT" | grep -q "quality_pass"; then
  ok "quality_pass 拒 exit1"
else
  bad "C10" "rc=$RC out=$OUT"
fi

echo "== C11: list --json =="
OUT="$(run workitem list --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"id":"w1"' && echo "$OUT" | grep -q '"id":"w2"'; then
  ok "items 含 w1/w2"
else
  bad "C11" "rc=$RC out=$OUT"
fi

echo "== C12: log w1 --json =="
OUT="$(run workitem log w1 --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"event":"create"' \
   && echo "$OUT" | grep -q '"event":"translate"'; then
  ok "events 序列正确"
else
  bad "C12" "rc=$RC out=$OUT"
fi

echo "== C13: lock --owner a → 他人 transition 拒 → unlock 后可转移 =="
printf '# taskbook\n' > "$DATA/workitems/w1/taskbook.md"
OUT="$(run workitem lock w1 --owner a --json 2>&1)"; RC=$?
[[ $RC -eq 0 ]] || bad "C13a" "lock rc=$RC out=$OUT"
OUT="$(run workitem transition w1 executed --json 2>&1)"; RC=$?
if [[ $RC -eq 2 ]]; then ok "锁被持有 → transition 拒 exit2"; else bad "C13b" "rc=$RC out=$OUT"; fi
OUT="$(run workitem unlock w1 --owner a --json 2>&1)"; RC=$?
[[ $RC -eq 0 ]] || bad "C13c" "unlock rc=$RC out=$OUT"
OUT="$(run workitem transition w1 executed --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]]; then ok "解锁后可转移"; else bad "C13d" "rc=$RC out=$OUT"; fi

echo "== C14: lock --ttl 0 → 过期自动释放 =="
run workitem verify w1 --json >/dev/null 2>&1
OUT="$(run workitem lock w1 --owner a --ttl 0 --json 2>&1)"; RC=$?
[[ $RC -eq 0 ]] || bad "C14a" "lock rc=$RC out=$OUT"
OUT="$(run workitem transition w1 verified --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]]; then ok "过期锁自动释放 → verified"; else bad "C14b" "rc=$RC out=$OUT"; fi

echo "== C15: --json stdout 单行 + 错误走 stderr =="
OUT="$(run workitem status w1 --json 2>/dev/null)"; RC=$?
if [[ $RC -eq 0 ]] && [[ $(printf '%s' "$OUT" | grep -c .) -eq 1 ]]; then
  ok "成功 stdout 单行"
else
  bad "C15a" "rc=$RC out=$OUT"
fi
STDOUT="$(run workitem transition w1 bogus --json 2>/dev/null)"; RC=$?
STDERR="$(run workitem transition w1 bogus --json 2>&1 >/dev/null)"
if [[ $RC -eq 2 ]] && [[ -z "$STDOUT" ]] && echo "$STDERR" | grep -q '"status":"failed"'; then
  ok "错误走 stderr,stdout 干净"
else
  bad "C15b" "rc=$RC stdout=$STDOUT stderr=$STDERR"
fi

echo "== C16: 新源码 deny-list 零命中 =="
if grep -En '/home/[^~]|sk-[A-Za-z0-9]{10}' "$ROOT_DIR/scripts/flow" "$ROOT_DIR/scripts/flow-core.py" "$ROOT_DIR/config/defaults.yaml" >/dev/null 2>&1; then
  bad "C16" "deny-list 命中"
else
  ok "deny-list 零命中"
fi

echo "== C17: 两进程并发 transition → 恰一个生效 =="
( run workitem transition w1 accepted --approve --json >"$WORK/c17a.out" 2>&1; echo $? >"$WORK/c17a.rc" ) &
( run workitem transition w1 accepted --approve --json >"$WORK/c17b.out" 2>&1; echo $? >"$WORK/c17b.rc" ) &
wait
RCA=$(cat "$WORK/c17a.rc"); RCB=$(cat "$WORK/c17b.rc")
STATE="$(run workitem status w1 --json 2>/dev/null | python3 -c 'import sys,json;print(json.loads(sys.stdin.read())["state"])' 2>/dev/null)"
if [[ "$STATE" == "accepted" ]] && { [[ $RCA -eq 0 && $RCB -ne 0 ]] || [[ $RCB -eq 0 && $RCA -ne 0 ]]; }; then
  ok "恰一个生效,status 未损坏(state=$STATE)"
else
  bad "C17" "rcA=$RCA rcB=$RCB state=$STATE"
fi

echo "== C18: 回归 run-smoke.sh + run-config-smoke.sh =="
bash "$ROOT_DIR/tests/run-smoke.sh" >"$WORK/reg1.log" 2>&1; RC1=$?
bash "$ROOT_DIR/tests/run-config-smoke.sh" >"$WORK/reg2.log" 2>&1; RC2=$?
if [[ $RC1 -eq 0 && $RC2 -eq 0 ]]; then
  ok "15+17 回归全绿"
else
  bad "C18" "run-smoke rc=$RC1 run-config-smoke rc=$RC2"
fi

# ---------- C19-C21: design --async / --check(stub dsh 零 API) ----------
FAKE_KEY="sk-""fake-key-$(date +%s)"
STUB_DSH="$WORK/stub-dsh.sh"
cat > "$STUB_DSH" << 'STUB'
#!/usr/bin/env bash
# stub dsh(async 冒烟):env DSH_STUB_SLEEP/DSH_STUB_EXIT
set -u
if [[ "${DSH_STUB_SLEEP:-0}" != "0" ]]; then sleep "$DSH_STUB_SLEEP"; fi
cat << 'MD'
# stub 方案

这是 stub 输出的假方案内容，用于 async 冒烟测试。
MD
exit "${DSH_STUB_EXIT:-0}"
STUB
chmod +x "$STUB_DSH"
mkdir -p "$WORK/dshhome"

run_design() { # run_design [args...] —— design 链路:注入 stub dsh 全套 env
  env HOME="$WORK/home" FLOW_DATA_DIR="$DATA" COLLABFLOW_CONFIG="$WORK/no-cfg.yaml" \
      DEEPSEEK_API_KEY="$FAKE_KEY" DSH_HOME="$WORK/dshhome" DSH_BIN="$STUB_DSH" \
      DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" \
      "$FLOW" "$@"
}

wait_async() { # wait_async <id> [timeout_s] —— 轮询 design-async-result.json 出现
  local id="$1" t="${2:-20}" i=0
  while [[ $i -lt $((t * 2)) ]]; do
    [[ -f "$DATA/workitems/$id/design-async-result.json" ]] && return 0
    sleep 0.5
    i=$((i + 1))
  done
  return 1
}

async_pid() { # async_pid <id> —— status --json 中的 async.pid
  run_design workitem status "$1" --json 2>/dev/null \
    | python3 -c 'import sys,json;print((json.loads(sys.stdin.read()).get("async") or {}).get("pid",""))' 2>/dev/null
}

echo "== C19: design --async → --check 端到端(designed) =="
run_design workitem new w3 --brief "$BRIEF_SRC" --json >/dev/null 2>&1
OUT="$(run_design workitem design w3 --async 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q "pid="; then
  ok "async 启动 exit0+输出 pid"
else
  bad "C19a" "rc=$RC out=$OUT"
fi
if wait_async w3; then
  OUT="$(run_design workitem design w3 --check 2>&1)"; RC=$?
  STATE="$(run_design workitem status w3 --json 2>/dev/null \
           | python3 -c 'import sys,json;print(json.loads(sys.stdin.read())["state"])' 2>/dev/null)"
  if [[ $RC -eq 0 ]] && [[ "$STATE" == "designed" ]] \
     && [[ -s "$DATA/workitems/w3/design.md" ]]; then
    ok "check exit0 → designed + design.md 非空"
  else
    bad "C19b" "rc=$RC state=$STATE"
  fi
else
  bad "C19c" "worker 未在超时内完成"
fi

echo "== C20: --check 幂等 + --async/--check 互斥 =="
OUT1="$(run_design workitem status w3 --json 2>/dev/null)"
OUT="$(run_design workitem design w3 --check 2>&1)"; RC=$?
OUT2="$(run_design workitem status w3 --json 2>/dev/null)"
if [[ $RC -eq 0 ]] && [[ "$OUT1" == "$OUT2" ]]; then
  ok "二次 --check exit0 幂等(状态不变,无重复事件)"
else
  bad "C20a" "rc=$RC"
fi
OUT="$(run_design workitem design w3 --async --check 2>&1)"; RC=$?
if [[ $RC -eq 2 ]]; then ok "--async --check 互斥 exit2"; else bad "C20b" "rc=$RC out=$OUT"; fi

echo "== C21: 超时报警(--expected 1 + DSH_STUB_SLEEP=3) =="
run_design workitem new w4 --brief "$BRIEF_SRC" --json >/dev/null 2>&1
OUT="$(DSH_STUB_SLEEP=2 run_design workitem design w4 --async --expected 1 --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]]; then
  OUT1="$(run_design workitem design w4 --check --json 2>&1)"; RC1=$?
  sleep 2
  OUT2="$(run_design workitem design w4 --check --json 2>&1)"; RC2=$?
  if [[ $RC1 -eq 3 ]] && [[ $RC2 -eq 124 ]] \
     && echo "$OUT2" | grep -q '"alarm":"timeout"'; then
    ok "exit3(running) → exit124(alarm:timeout)"
  else
    bad "C21" "rc1=$RC1 rc2=$RC2 out1=$OUT1 out2=$OUT2"
  fi
  WPID="$(async_pid w4)"
  [[ -n "$WPID" ]] && kill -9 "$WPID" 2>/dev/null   # 清理未结束的 worker
else
  bad "C21a" "async 启动 rc=$RC out=$OUT"
fi

echo
echo "========== 结果: $PASS 通过 / $FAIL 失败 =========="
if [[ $FAIL -gt 0 ]]; then
  printf '失败用例: %s\n' "${FAILED_CASES[*]}"
  exit 1
fi
exit 0
