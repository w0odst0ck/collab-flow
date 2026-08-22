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

# ── flow-task-ledger 门禁(§1.4,fail-closed) ─────────────────────────────
# task add 需 kind/priority/expected-seconds/why + 锚定(workitem 或项目路径);
# 自由命令不在模板白名单 → 显式 --force + --force-reason。既有自由命令用例经
# smoke_add/smoke_add_mp 统一注入(用例可后覆盖 priority/expected-seconds 等)。
smoke_add() { # smoke_add [add args...] —— 注入门禁参数后透传(默认 queued 语义不变)
  run task add --kind design --priority P2 --expected-seconds 30 --why smoke \
      --force --force-reason smoke "$@"
}
smoke_add_mp() { # smoke_add_mp <max-parallel> [add args...]
  run_mp "$1" task add --kind design --priority P2 --expected-seconds 30 --why smoke \
      --force --force-reason smoke "${@:2}"
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
  # FLOW_TASK_DIR 必注(M2 默认入队落临时目录,不触碰真实 ~/.collabflow;run/run_mp 已有,此处补齐)
  env HOME="$WORK/home" FLOW_DATA_DIR="$DATA" FLOW_TASK_DIR="$WORK/task" \
      COLLABFLOW_CONFIG="$WORK/no-cfg.yaml" \
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
OUT="$(smoke_add --command "FLOW_WORKDIR=/projects/smoke sleep 1" --json 2>&1)"; RC=$?
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
OUT="$(smoke_add --workitem w1 --command "FLOW_WORKDIR=/projects/smoke sleep 2" --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]]; then
  OUT="$(smoke_add --workitem w1 --command "FLOW_WORKDIR=/projects/smoke sleep 1" --json 2>&1)"; RC=$?
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
OUT="$(smoke_add --command "FLOW_WORKDIR=/projects/smoke sleep 2" --json 2>&1)"; RC=$?
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
  OUT="$(smoke_add_mp 2 --command "FLOW_WORKDIR=/projects/smoke sleep 1" --json 2>&1)"
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
OUT="$(smoke_add_mp 1 --command "FLOW_WORKDIR=/projects/smoke sleep 1" --json 2>&1)"
BLOCK_ID="$(task_id "$OUT")"
OUT="$(smoke_add_mp 1 --command "FLOW_WORKDIR=/projects/smoke sleep 0.2" --priority P2 --json 2>&1)"
P2_ID="$(task_id "$OUT")"
OUT="$(smoke_add_mp 1 --command "FLOW_WORKDIR=/projects/smoke sleep 0.2" --priority P0 --json 2>&1)"
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
OUT="$(smoke_add --command "FLOW_WORKDIR=/projects/smoke sleep 5" --expected-seconds 1 --json 2>&1)"; RC=$?
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
OUT="$(smoke_add --command "FLOW_WORKDIR=/projects/smoke sh -c 'echo boom; exit 3'" --json 2>&1)"; RC=$?
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

# ---------- C30-C39: M2 async-first 默认 + 事件层 + 宿主集成(stub 零 API) ----------
# stub rx(reasonix wrapper 经 RX_BIN 注入,C36 partial-complete 用):记录/行为受 env 控制
STUB_RX="$WORK/stub-rx.sh"
cat > "$STUB_RX" << 'RX'
#!/usr/bin/env bash
set -u
if [[ "${RX_STUB_DIFF:-0}" == "1" ]]; then
  echo "stub rx change $(date +%s)" >> README.md
fi
if [[ "${RX_STUB_SLEEP:-0}" != "0" ]]; then
  sleep "${RX_STUB_SLEEP}"
fi
exit "${RX_STUB_EXIT:-0}"
RX
chmod +x "$STUB_RX"

run_x() { # run_x [args...] —— execute 链路 env(FLOW_WORKDIR=临时 git 项目,防污染仓库根)
  env HOME="$WORK/home" FLOW_DATA_DIR="$DATA" FLOW_TASK_DIR="$WORK/task" \
      FLOW_WORKDIR="$PROJ" COLLABFLOW_CONFIG="$WORK/no-cfg.yaml" \
      DSH_BIN="$STUB_DSH" DEEPSEEK_API_KEY="$FAKE_KEY" \
      DSH_HOME="$WORK/dshhome" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" \
      "$FLOW" "$@"
}

task_id_of() { # task_id_of —— 从入队 JSON 输出取 task_id
  printf '%s' "$1" | python3 -c 'import sys,json;print(json.loads(sys.stdin.read()).get("task_id",""))' 2>/dev/null
}

wait_task() { # wait_task <tid> [timeout_s] —— 轮询 task status 至终态
  local tid="$1" t="${2:-30}" i=0 st=""
  while [[ $i -lt $((t * 2)) ]]; do
    st="$(run task status "$tid" --json 2>/dev/null \
          | python3 -c 'import sys,json;print((json.loads(sys.stdin.read()).get("task") or {}).get("state",""))' 2>/dev/null)"
    case "$st" in done|failed|timeout) return 0;; esac
    sleep 0.5
    i=$((i + 1))
  done
  return 1
}

ev_state() { # ev_state <evfile> —— 事件流最后一行 state
  python3 -c 'import json,sys
lines=[l for l in open(sys.argv[1],encoding="utf-8") if l.strip()]
print(json.loads(lines[-1]).get("state",""))' "$1" 2>/dev/null
}

echo "== C30: design 默认入队(queued+task_id+事件 queued/running) =="
run_design workitem new w5 --brief "$BRIEF_SRC" --json >/dev/null 2>&1
OUT="$(DSH_STUB_SLEEP=2 run_design workitem design w5 --json 2>&1)"; RC=$?
TID30="$(task_id_of "$OUT")"
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"queued":true' \
   && echo "$OUT" | grep -q '"status":"ok"' && [[ "$TID30" =~ ^t-[0-9a-f]{12}$ ]]; then
  ok "默认入队 exit0+queued+task_id"
else
  bad "C30a" "rc=$RC out=$OUT"
fi
EV30="$WORK/task/events/$TID30.jsonl"
if [[ -f "$EV30" ]] && grep -q '"state":"queued"' "$EV30" && grep -q '"state":"running"' "$EV30"; then
  ok "事件流含 queued/running"
else
  bad "C30b" "events 缺失或事件不全: $EV30"
fi

echo "== C34: design 重复入队(在途 → exit2+duplicate_workitem) =="
OUT="$(run_design workitem design w5 --json 2>&1)"; RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q 'duplicate_workitem'; then
  ok "在途重复入队 exit2+duplicate_workitem"
else
  bad "C34" "rc=$RC out=$OUT"
fi

echo "== C31: design --sync 同步(立即 designed,无新增任务) =="
run_design workitem new w6 --brief "$BRIEF_SRC" --json >/dev/null 2>&1
N_BEFORE="$(run task list --json 2>/dev/null \
            | python3 -c 'import sys,json;print(json.loads(sys.stdin.read()).get("count",-1))' 2>/dev/null)"
OUT="$(run_design workitem design w6 --sync --json 2>&1)"; RC=$?
N_AFTER="$(run task list --json 2>/dev/null \
           | python3 -c 'import sys,json;print(json.loads(sys.stdin.read()).get("count",-1))' 2>/dev/null)"
ST6="$(run_design workitem status w6 --json 2>/dev/null \
       | python3 -c 'import sys,json;print(json.loads(sys.stdin.read())["state"])' 2>/dev/null)"
if [[ $RC -eq 0 ]] && [[ "$ST6" == "designed" ]] \
   && [[ -s "$DATA/workitems/w6/design.md" ]] && [[ "$N_BEFORE" == "$N_AFTER" ]]; then
  ok "--sync 同步 designed + design.md 非空 + 无新增任务"
else
  bad "C31" "rc=$RC state=$ST6 nbefore=$N_BEFORE nafter=$N_AFTER"
fi

echo "== C32: design 入队 E2E 联动(任务 done → workitem designed + 事件 done) =="
if wait_task "$TID30"; then
  ST5="$(run_design workitem status w5 --json 2>/dev/null \
         | python3 -c 'import sys,json;print(json.loads(sys.stdin.read())["state"])' 2>/dev/null)"
  TST30="$(run task status "$TID30" --json 2>/dev/null \
           | python3 -c 'import sys,json;print((json.loads(sys.stdin.read()).get("task") or {}).get("state",""))' 2>/dev/null)"
  EST30="$(ev_state "$EV30")"
  if [[ "$ST5" == "designed" && "$TST30" == "done" && "$EST30" == "done" ]]; then
    ok "task done + workitem designed + 事件 done(联动)"
  else
    bad "C32" "st=$ST5 tst=$TST30 est=$EST30"
  fi
else
  bad "C32" "w5 任务未在超时内完成"
fi

echo "== C33: 事件可重放(逐行 JSON + seq 单调 + 终态含 diagnostic 键) =="
if python3 - "$EV30" << 'PY'
import json, sys
seqs = []
with open(sys.argv[1], encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)          # 坏行会抛 → 测试失败
        assert rec.get("seq") is not None
        seqs.append(rec["seq"])
assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs), seqs
last = json.loads([l for l in open(sys.argv[1], encoding="utf-8") if l.strip()][-1])
assert "diagnostic" in last, last
PY
then
  ok "重放成功,seq 单调,终态含 diagnostic 键"
else
  bad "C33" "重放失败: $EV30"
fi

echo "== C35: execute 默认入队 E2E(stub executor → executed + 事件 done) =="
PROJ="$WORK/proj"
mkdir -p "$PROJ"
git -C "$PROJ" init -q
git -C "$PROJ" config user.email t@example.com
git -C "$PROJ" config user.name tester
echo "# init" > "$PROJ/README.md"
git -C "$PROJ" add README.md
git -C "$PROJ" commit -q -m init
run_x workitem new w7 --brief "$BRIEF_SRC" --json >/dev/null 2>&1
run_x workitem design w7 --sync --json >/dev/null 2>&1
run_x workitem decision w7 --verdict pass --json >/dev/null 2>&1
run_x workitem transition w7 reviewed --json >/dev/null 2>&1
run_x workitem transition w7 translated --json >/dev/null 2>&1
cat > "$DATA/workitems/w7/taskbook.md" << 'TASKBOOK'
# taskbook
```flow
test_command: /bin/true
diff_scope:
  allow:
    - README.md
  deny: []
```
TASKBOOK
OUT="$(run_x workitem execute w7 --executor stub --json 2>&1)"; RC=$?
TID35="$(task_id_of "$OUT")"
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"queued":true' && [[ "$TID35" =~ ^t-[0-9a-f]{12}$ ]]; then
  ok "execute 默认入队 exit0+queued+task_id"
else
  bad "C35a" "rc=$RC out=$OUT"
fi
if wait_task "$TID35"; then
  ST7="$(run_x workitem status w7 --json 2>/dev/null \
         | python3 -c 'import sys,json;print(json.loads(sys.stdin.read())["state"])' 2>/dev/null)"
  TST35="$(run task status "$TID35" --json 2>/dev/null \
           | python3 -c 'import sys,json;print((json.loads(sys.stdin.read()).get("task") or {}).get("state",""))' 2>/dev/null)"
  EST35="$(ev_state "$WORK/task/events/$TID35.jsonl")"
  if [[ "$ST7" == "executed" && "$TST35" == "done" && "$EST35" == "done" ]]; then
    ok "E2E: workitem executed + task done + 事件 done"
  else
    bad "C35b" "st=$ST7 tst=$TST35 est=$EST35"
  fi
else
  bad "C35c" "w7 任务未在超时内完成"
fi

echo "== C36: execute partial-complete 事件(内层超时 + 产出非空) =="
run_x workitem new w8 --brief "$BRIEF_SRC" --json >/dev/null 2>&1
run_x workitem design w8 --sync --json >/dev/null 2>&1
run_x workitem decision w8 --verdict pass --json >/dev/null 2>&1
run_x workitem transition w8 reviewed --json >/dev/null 2>&1
run_x workitem transition w8 translated --json >/dev/null 2>&1
cat > "$DATA/workitems/w8/taskbook.md" << 'TASKBOOK'
# taskbook
```flow
test_command: /bin/true
diff_scope:
  allow:
    - README.md
  deny: []
```
TASKBOOK
export RX_BIN="$STUB_RX" RX_STUB_EXIT=124 RX_STUB_DIFF=1 RX_STUB_SLEEP=0 RX_STUB_LEAK=0
OUT="$(run_x workitem execute w8 --executor reasonix --timeout 1 --json 2>&1)"; RC=$?
TID36="$(task_id_of "$OUT")"
unset RX_BIN RX_STUB_EXIT RX_STUB_DIFF RX_STUB_SLEEP RX_STUB_LEAK
if [[ $RC -eq 0 ]] && [[ "$TID36" =~ ^t-[0-9a-f]{12}$ ]] && wait_task "$TID36"; then
  if python3 - "$WORK/task/events/$TID36.jsonl" << 'PY'
import json, sys
lines = [l for l in open(sys.argv[1], encoding="utf-8") if l.strip()]
rec = json.loads(lines[-1])
assert rec["state"] == "partial-complete", rec
assert rec["partial_complete"] is True, rec
assert rec.get("diagnostic"), rec
PY
  then
    ok "事件 state=partial-complete + partial_complete:true + diagnostic 非空"
  else
    bad "C36b" "事件内容不符: $(cat "$WORK/task/events/$TID36.jsonl" 2>/dev/null)"
  fi
else
  bad "C36a" "rc=$RC tid=$TID36"
fi

echo "== C37: 事件失败诊断(failed + diagnostic 含 boom) =="
OUT="$(smoke_add --command "FLOW_WORKDIR=/projects/smoke sh -c 'echo boom; exit 3'" --json 2>&1)"; RC=$?
TID37="$(task_id "$OUT")"
sleep 2
if python3 - "$WORK/task/events/$TID37.jsonl" << 'PY'
import json, sys
lines = [l for l in open(sys.argv[1], encoding="utf-8") if l.strip()]
rec = json.loads(lines[-1])
assert rec["state"] == "failed", rec
assert "boom" in (rec.get("diagnostic") or ""), rec
PY
then
  ok "事件 failed + diagnostic 含 boom"
else
  bad "C37" "rc=$RC events=$(cat "$WORK/task/events/$TID37.jsonl" 2>/dev/null)"
fi

echo "== C38: wake-text 自包含(含 task_id/state/下一步/日志路径) =="
OUT="$(run task wake-text "$TID37" 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q "$TID37" \
   && echo "$OUT" | grep -q "下一步" && echo "$OUT" | grep -q "logs/$TID37.log"; then
  ok "wake-text 自包含文本"
else
  bad "C38" "rc=$RC out=$OUT"
fi

echo "== C39: notify 触发(配置时 stdin 事件;未配置零调用) =="
NOTIFY_RECORD="$WORK/notify-record.json"
if [[ -e "$NOTIFY_RECORD" ]]; then
  bad "C39a" "未配置 notify 却有调用记录"
else
  ok "未配置 host.notify → 零调用"
fi
STUB_NOTIFY="$WORK/stub-notify.sh"
cat > "$STUB_NOTIFY" << 'NOTIFY'
#!/usr/bin/env bash
cat > "$1"
NOTIFY
chmod +x "$STUB_NOTIFY"
cat > "$WORK/notify-cfg.yaml" << EOF
host:
  notify: $STUB_NOTIFY $NOTIFY_RECORD
EOF
OUT="$(env HOME="$WORK/home" FLOW_DATA_DIR="$DATA" FLOW_TASK_DIR="$WORK/task" \
    COLLABFLOW_CONFIG="$WORK/notify-cfg.yaml" "$FLOW" \
    task add --command "FLOW_WORKDIR=/projects/smoke echo notify-test" \
    --kind design --priority P2 --expected-seconds 30 --why smoke \
    --force --force-reason smoke --json 2>&1)"; RC=$?
TID39="$(task_id "$OUT")"
sleep 2
if [[ $RC -eq 0 ]] && [[ -f "$NOTIFY_RECORD" ]] && grep -q "$TID39" "$NOTIFY_RECORD"; then
  ok "配置 notify → 终态事件经 stdin 到达 stub"
else
  bad "C39b" "rc=$RC record=$(test -f "$NOTIFY_RECORD" && cat "$NOTIFY_RECORD" || echo missing)"
fi

echo "== C40: 全量回归(run-smoke + run-config-smoke + run-executor-smoke + unittest) =="
if [[ "${FLOW_SMOKE_NESTED:-0}" == "1" ]]; then
  ok "嵌套调用(FLOW_SMOKE_NESTED=1),跳过 C40 防递归"
else
  bash "$ROOT_DIR/tests/run-smoke.sh" >"$WORK/c40-1.log" 2>&1; RC1=$?
  bash "$ROOT_DIR/tests/run-config-smoke.sh" >"$WORK/c40-2.log" 2>&1; RC2=$?
  export FLOW_SMOKE_NESTED=1
  bash "$ROOT_DIR/tests/run-executor-smoke.sh" >"$WORK/c40-3.log" 2>&1; RC3=$?
  unset FLOW_SMOKE_NESTED
  ( cd "$ROOT_DIR" && python3 -m unittest discover tests >"$WORK/c40-4.log" 2>&1 ); RC4=$?
  if [[ $RC1 -eq 0 && $RC2 -eq 0 && $RC3 -eq 0 && $RC4 -eq 0 ]]; then
    ok "四个回归门全绿"
  else
    bad "C40" "run-smoke=$RC1 run-config=$RC2 run-executor=$RC3 unittest=$RC4"
  fi
fi

# ---------- C41-C47: flow-task-ledger 门禁/scheduled/pump/reschedule/cost/--force(§2.2) ----------
FUTURE_AT="$(python3 -c 'from datetime import datetime,timedelta,timezone; \
print((datetime.now(timezone.utc)+timedelta(hours=6)).isoformat(timespec="seconds"))')"
FUTURE_AT2="$(python3 -c 'from datetime import datetime,timedelta,timezone; \
print((datetime.now(timezone.utc)+timedelta(hours=10)).isoformat(timespec="seconds"))')"

echo "== C41: 门禁拒绝(kind 白名单 + why 必填) =="
OUT="$(run task add --command "flow workitem design x --sync --json" --workitem x \
    --kind reminder --priority P2 --expected-seconds 480 --why test --json 2>&1)"; RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "提醒请走 cron"; then
  ok "kind=reminder 拒绝 + 文案含「提醒请走 cron」"
else
  bad "C41a" "rc=$RC out=$OUT"
fi
OUT="$(run task add --command "FLOW_WORKDIR=/projects/smoke sleep 1" \
    --kind design --priority P2 --expected-seconds 30 \
    --force --force-reason smoke --json 2>&1)"; RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "拒绝"; then
  ok "缺 --why 拒绝(exit 2)"
else
  bad "C41b" "rc=$RC out=$OUT"
fi

echo "== C42: 合法入队(kind/why/audit 落账) =="
run workitem new x --json >/dev/null 2>&1
OUT="$(run task add --command "flow workitem design x --sync --json" --workitem x \
    --kind design --priority P2 --expected-seconds 480 --why test --json 2>&1)"; RC=$?
TID42="$(task_id "$OUT")"
if [[ $RC -eq 0 ]] && [[ "$TID42" =~ ^t-[0-9a-f]{12}$ ]]; then
  LIST42="$(run task list --json 2>&1)"
  if echo "$LIST42" | grep -q '"kind":"design"' && echo "$LIST42" | grep -q '"why":"test"' \
     && echo "$LIST42" | grep -q '"audit"'; then
    ok "list --json 含 kind/why/audit"
  else
    bad "C42b" "list 缺字段: $(echo "$LIST42" | head -c 300)"
  fi
else
  bad "C42a" "rc=$RC out=$OUT"
fi

echo "== C43: scheduled 入队(--at → scheduled + 不 auto-dispatch) =="
wait_task "$TID42" >/dev/null 2>&1   # C42 的 workitem x 任务需先终态(幂等 gate 不阻塞)
OUT="$(run task add --command "flow workitem design x --sync --json" --workitem x \
    --kind design --priority P2 --expected-seconds 480 --why test \
    --at "$FUTURE_AT" --json 2>&1)"; RC=$?
TID43="$(task_id "$OUT")"
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"state":"scheduled"' \
   && echo "$OUT" | grep -q '"scheduled_at"'; then
  ok "add --at → state=scheduled + scheduled_at 落账"
else
  bad "C43a" "rc=$RC out=$OUT"
fi
ST43="$(run task status "$TID43" --json 2>/dev/null \
        | python3 -c 'import sys,json;print((json.loads(sys.stdin.read()).get("task") or {}).get("state",""))' 2>/dev/null)"
if [[ "$ST43" == "scheduled" ]]; then
  ok "不 auto-dispatch(仍 scheduled)"
else
  bad "C43b" "state=$ST43(应为 scheduled)"
fi

echo "== C44: pump E2E(到点 scheduled → running → done + pump.json 心跳) =="
OUT="$(smoke_add --command "FLOW_WORKDIR=/projects/smoke sleep 0.5" \
    --at "$FUTURE_AT" --json 2>&1)"; RC=$?
TID44="$(task_id "$OUT")"
if [[ $RC -eq 0 ]]; then
  # 注入已到点(直接改注册表;pump 无窗口硬门控,D4,到点即升)
  python3 - "$WORK/task/tasks.json" "$TID44" << 'PY'
import json, os, sys
from datetime import datetime, timedelta, timezone
path, tid = sys.argv[1], sys.argv[2]
reg = json.load(open(path, encoding="utf-8"))
past = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat(timespec="seconds")
reg["tasks"][tid]["scheduled_at"] = past
# 原子写(temp + os.replace),防中断截断注册表
tmp = path + f".tmp{os.getpid()}"
with open(tmp, "w", encoding="utf-8") as f:
    json.dump(reg, f, ensure_ascii=False, indent=2)
os.replace(tmp, path)
PY
  OUT="$(run task pump --json 2>&1)"; RC=$?
  if [[ $RC -eq 0 ]] && wait_task "$TID44"; then
    T44="$(run task status "$TID44" --json 2>/dev/null)"
    if echo "$T44" | grep -q '"state":"done"' && echo "$T44" | grep -q '"exit_code":0'; then
      ok "pump 提升 → done+exit_code=0"
    else
      bad "C44b" "pump 后非 done: $T44"
    fi
  else
    bad "C44a" "rc=$RC out=$OUT"
  fi
else
  bad "C44c" "add rc=$RC out=$OUT"
fi
if [[ -f "$WORK/task/pump.json" ]] && grep -q '"heartbeat_at"' "$WORK/task/pump.json"; then
  ok "pump.json 心跳落盘"
else
  bad "C44d" "pump.json 缺失或无心跳"
fi

echo "== C45: reschedule(scheduled 改期成功;非 scheduled 改期拒) =="
OUT="$(run task reschedule "$TID43" --at "$FUTURE_AT2" --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"scheduled_at"'; then
  ok "scheduled 改期成功"
else
  bad "C45a" "rc=$RC out=$OUT"
fi
OUT="$(run task reschedule "$TID42" --at "$FUTURE_AT2" --json 2>&1)"; RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "仅 scheduled 可改期"; then
  ok "非 scheduled 改期拒绝"
else
  bad "C45b" "rc=$RC out=$OUT"
fi

echo "== C46: cost --json(list 含 cost_usd/scheduled_at/audit) =="
LIST46="$(run task list --json 2>&1)"
if echo "$LIST46" | grep -q '"cost_usd"' && echo "$LIST46" | grep -q '"scheduled_at"' \
   && echo "$LIST46" | grep -q '"audit"'; then
  ok "list --json 含 cost_usd/scheduled_at/audit"
else
  bad "C46" "list 缺字段: $(echo "$LIST46" | head -c 300)"
fi

echo "== C47: --force(自由命令 + --force-reason → audit.force_reason 落账) =="
OUT="$(run task add --command "FLOW_WORKDIR=/projects/smoke true" \
    --kind design --priority P2 --expected-seconds 30 --why test \
    --force --force-reason r47 --json 2>&1)"; RC=$?
TID47="$(task_id "$OUT")"
if [[ $RC -eq 0 ]] && [[ "$TID47" =~ ^t-[0-9a-f]{12}$ ]]; then
  if run task status "$TID47" --json 2>/dev/null | grep -q '"audit":{"force_reason":"r47"}'; then
    ok "audit.force_reason=r47 落账"
  else
    bad "C47b" "audit 缺失: $(run task status "$TID47" --json 2>&1 | head -c 300)"
  fi
else
  bad "C47a" "rc=$RC out=$OUT"
fi

# ── ENV1: 跨仓 workitem 执行(任务书 §4/§6:workdir 指向独立仓时 workitem 可解析) ──
echo "== ENV1: 跨仓 workitem 执行(workdir 独立仓可解析,产物落独立仓) =="
INDEP="$WORK/indep-repo"
mkdir -p "$INDEP/.flow/workitems/cross"
printf 'state: translated\n' > "$INDEP/.flow/workitems/cross/status.yaml"
printf '# cross taskbook\n验证跨仓解析\n' > "$INDEP/.flow/workitems/cross/taskbook.md"
( cd "$INDEP" && git init -q && git add . \
  && git -c user.email=smoke@t -c user.name=smoke commit -qm init ) 2>/dev/null
# add 进程 FLOW_DATA_DIR 指向独立仓(gate 锚定/模板白名单可见);workdir 记录独立仓;
# runner 子进程经 spawn_runner env 注入 FLOW_DATA_DIR/FLOW_WORKDIR(任务书 §4)后解析落到独立仓。
OUT="$(env HOME="$WORK/home" FLOW_DATA_DIR="$INDEP/.flow" FLOW_TASK_DIR="$WORK/task" \
    COLLABFLOW_CONFIG="$WORK/no-cfg.yaml" "$FLOW" \
    task add --command "flow workitem execute cross --sync --executor stub --timeout 3 --json" \
    --workitem cross --workdir "$INDEP" --kind execute --priority P2 \
    --expected-seconds 120 --why smoke --json 2>&1)"; RC=$?
TIDENV="$(task_id "$OUT")"
if [[ $RC -eq 0 ]] && [[ "$TIDENV" =~ ^t-[0-9a-f]{12}$ ]] && wait_task "$TIDENV"; then
  if [[ -f "$INDEP/.flow/workitems/cross/executor/result.json" ]] \
     && ! [[ -f "$DATA/workitems/cross/executor/result.json" ]]; then
    ok "跨仓 workitem 可解析,result.json 落独立仓(默认仓无产物)"
  else
    bad "ENV1b" "独立仓 result.json 缺失或默认仓误产: $(run task status "$TIDENV" --json 2>/dev/null | head -c 300)"
  fi
else
  bad "ENV1a" "rc=$RC out=$OUT"
fi

echo
echo "========== 结果: $PASS 通过 / $FAIL 失败 =========="
if [[ $FAIL -gt 0 ]]; then
  printf '失败用例: %s\n' "${FAILED_CASES[*]}"
  exit 1
fi
exit 0
