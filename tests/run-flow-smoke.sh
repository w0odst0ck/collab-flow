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

run() { # run [args...]  —— 统一注入隔离 env(含 FLOW_TASK_DIR,绝不触碰真实 ~/.collabflow)
  env HOME="$WORK/home" FLOW_DATA_DIR="$DATA" FLOW_TASK_DIR="$WORK/task" \
      COLLABFLOW_CONFIG="$WORK/no-cfg.yaml" "$FLOW" "$@"
}

run_mp() { # run_mp <max-parallel> [args...] —— 注入 FLOW_TASK_MAX_PARALLEL(task 冒烟用)
  env HOME="$WORK/home" FLOW_DATA_DIR="$DATA" FLOW_TASK_DIR="$WORK/task" \
      FLOW_TASK_MAX_PARALLEL="$1" COLLABFLOW_CONFIG="$WORK/no-cfg.yaml" "$FLOW" "${@:2}"
}

task_id() { # task_id —— 从 add 的 --json 输出取 id
  printf '%s' "$1" | python3 -c 'import sys,json;print(json.loads(sys.stdin.read()).get("id",""))' 2>/dev/null
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

# ---------- C22-C29: flow task 后台任务队列(M1;对应设计文档 C19-C26) ----------
# 全程 stub(sleep / sh -c "exit N")零 API;FLOW_TASK_DIR 已由 run()/run_mp() 注入临时目录。

echo "== C22: task add+status(add 返回 queued,status id 一致) =="
OUT="$(run task add --command "sleep 1" --json 2>&1)"; RC=$?
TID="$(task_id "$OUT")"
if [[ $RC -eq 0 ]] && json_ok "$OUT" >/dev/null \
   && [[ $(printf '%s' "$OUT" | grep -c .) -eq 1 ]] \
   && echo "$OUT" | grep -q '"state":"queued"' \
   && [[ "$TID" =~ ^t-[0-9a-f]{12}$ ]]; then
  ok "add exit0+单行JSON+state=queued+id 形状"
else
  bad "C22a" "rc=$RC out=$OUT"
fi
OUT="$(run task status "$TID" --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q "\"id\":\"$TID\"" \
   && echo "$OUT" | grep -qE '"state":"(queued|running|done)"'; then
  ok "status id 一致+state 合法"
else
  bad "C22b" "rc=$RC out=$OUT"
fi

echo "== C23: 幂等拒绝(同 workitem 非终态 → exit2+duplicate_workitem) =="
OUT="$(run task add --workitem w1 --command "sleep 2" --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]]; then
  OUT="$(run task add --workitem w1 --command "sleep 1" --json 2>&1)"; RC=$?
  if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q '"error":"duplicate_workitem"'; then
    ok "重复 add exit2+duplicate_workitem"
  else
    bad "C23b" "rc=$RC out=$OUT"
  fi
  sleep 3   # 等 w1 的 sleep 2 任务结束,不占后续槽位
else
  bad "C23a" "add rc=$RC out=$OUT"
fi

echo "== C24: E2E 自动流转 done(add→done,exit_code=0,finished_at 非空) =="
OUT="$(run task add --command "sleep 2" --json 2>&1)"; RC=$?
TID="$(task_id "$OUT")"
sleep 3
OUT="$(run task status "$TID" --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"state":"done"' \
   && echo "$OUT" | grep -q '"exit_code":0' \
   && echo "$OUT" | grep -q '"finished_at":"[0-9]'; then
  ok "add→自动流转 done+exit_code=0+finished_at 非空"
else
  bad "C24a" "rc=$RC out=$OUT"
fi
OUT="$(run task list --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q "\"id\":\"$TID\""; then
  ok "list 含该任务"
else
  bad "C24b" "rc=$RC out=$OUT"
fi

echo "== C25: 并发排队(--max-parallel 2,running 不超限,最终全 done) =="
TIDS25=()
for i in 1 2 3 4; do
  OUT="$(run_mp 2 task add --command "sleep 1" --json 2>&1)"
  TIDS25+=("$(task_id "$OUT")")
done
OUT="$(run_mp 2 task list --state running --json 2>&1)"
RUNNING_N="$(printf '%s' "$OUT" | python3 -c 'import sys,json;print(json.loads(sys.stdin.read()).get("count",99))' 2>/dev/null)"
if [[ "$RUNNING_N" -le 2 ]]; then
  ok "running 数=$RUNNING_N ≤2"
else
  bad "C25a" "running=$RUNNING_N out=$OUT"
fi
sleep 4
ALL_DONE=1
for TID in "${TIDS25[@]}"; do
  OUT="$(run_mp 2 task status "$TID" --json 2>&1)"
  echo "$OUT" | grep -q '"state":"done"' || ALL_DONE=0
done
if [[ $ALL_DONE -eq 1 ]]; then
  ok "4 个任务最终全 done"
else
  bad "C25b" "out=$OUT"
fi

echo "== C26: 优先级排队(P0 先于 P2 出队,--max-parallel 1) =="
OUT="$(run_mp 1 task add --command "sleep 1" --json 2>&1)"
BLOCK_ID="$(task_id "$OUT")"
OUT="$(run_mp 1 task add --command "sleep 0.2" --priority P2 --json 2>&1)"
P2_ID="$(task_id "$OUT")"
OUT="$(run_mp 1 task add --command "sleep 0.2" --priority P0 --json 2>&1)"
P0_ID="$(task_id "$OUT")"
sleep 4
S_P0="$(run_mp 1 task status "$P0_ID" --json 2>/dev/null \
        | python3 -c 'import sys,json;print((json.loads(sys.stdin.read()).get("task") or {}).get("started_at") or "")' 2>/dev/null)"
S_P2="$(run_mp 1 task status "$P2_ID" --json 2>/dev/null \
        | python3 -c 'import sys,json;print((json.loads(sys.stdin.read()).get("task") or {}).get("started_at") or "")' 2>/dev/null)"
if [[ -n "$S_P0" && -n "$S_P2" && "$S_P0" < "$S_P2" ]]; then
  ok "P0 先于 P2 出队($S_P0 < $S_P2)"
else
  bad "C26" "p0=$S_P0 p2=$S_P2 block=$BLOCK_ID"
fi

echo "== C27: 超时熔断(--expected-seconds 1 + sleep 5 → timeout/124) =="
OUT="$(run task add --command "sleep 5" --expected-seconds 1 --json 2>&1)"; RC=$?
TID="$(task_id "$OUT")"
sleep 3
OUT="$(run task status "$TID" --json 2>&1)"
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"state":"timeout"' \
   && echo "$OUT" | grep -q '"exit_code":124'; then
  ok "终态 timeout+exit_code=124"
else
  bad "C27" "rc=$RC out=$OUT"
fi

echo "== C28: 失败诊断(failed+failure_tail 含 boom) =="
OUT="$(run task add --command "sh -c 'echo boom; exit 3'" --json 2>&1)"; RC=$?
TID="$(task_id "$OUT")"
sleep 2
OUT="$(run task status "$TID" --json 2>&1)"
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"state":"failed"' \
   && echo "$OUT" | grep -q '"exit_code":3' \
   && echo "$OUT" | grep -q 'boom'; then
  ok "failed+exit_code=3+failure_tail 含 boom"
else
  bad "C28" "rc=$RC out=$OUT"
fi

echo "== C29: log --tail 抽取(尾部 ≤20 字节且含结束标记) =="
OUT="$(run task log "$TID" --tail 20 2>&1)"; RC=$?
LEN="$(printf '%s' "$OUT" | wc -c | tr -d ' ')"
if [[ $RC -eq 0 ]] && [[ "$LEN" -le 20 ]] && echo "$OUT" | grep -q "==="; then
  ok "尾部 ${LEN} 字节 ≤20 + 含 runner 结束标记"
else
  bad "C29" "rc=$RC len=$LEN out=$OUT"
fi

echo
echo "========== 结果: $PASS 通过 / $FAIL 失败 =========="
if [[ $FAIL -gt 0 ]]; then
  printf '失败用例: %s\n' "${FAILED_CASES[*]}"
  exit 1
fi
exit 0
