#!/usr/bin/env bash
# flow P3 CLI 冒烟(C19-C26,stub 零 API;方案 §6.2)
# 用法: bash tests/run-executor-smoke.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
FLOW="$ROOT_DIR/scripts/flow"
PASS=0; FAIL=0; FAILED_CASES=()

ok()  { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad() { FAIL=$((FAIL+1)); FAILED_CASES+=("$1"); echo "  ❌ $1 — $2"; }

# ---------- 公共准备(隔离 HOME / 临时 FLOW_DATA_DIR / git workdir) ----------
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/home"
DATA="$WORK/.flow"
PROJ="$WORK/proj"
mkdir -p "$PROJ"

git -C "$PROJ" init -q
git -C "$PROJ" config user.email t@example.com
git -C "$PROJ" config user.name tester
echo "# init" > "$PROJ/README.md"
git -C "$PROJ" add README.md
git -C "$PROJ" commit -q -m init

# stub dsh:输出含错误处理表的 markdown(供 verify --auto 错误表核对)
STUB_DSH="$WORK/stub-dsh.sh"
cat > "$STUB_DSH" << 'DSH'
#!/usr/bin/env bash
set -u
cat << 'MD'
# stub 方案

## 错误处理表
| # | 错误场景 | 处理 | 测试覆盖 |
|---|---|---|---|
| E1 | 场景1 | 处理1 | E1 |
MD
exit 0
DSH
chmod +x "$STUB_DSH"

FAKE_KEY="sk-fake-test-key-$(date +%s)"

run() { # run [args...] —— 统一注入隔离 env + stub designer/executor
  env HOME="$WORK/home" FLOW_DATA_DIR="$DATA" FLOW_WORKDIR="$PROJ" \
      COLLABFLOW_CONFIG="$WORK/no-cfg.yaml" \
      DSH_BIN="$STUB_DSH" DEEPSEEK_API_KEY="$FAKE_KEY" \
      DSH_HOME="$WORK/dshhome" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" \
      STUB_EXECUTOR_CHANGE_FILE="${STUB_EXECUTOR_CHANGE_FILE:-scripts/flow-core.py}" \
      "$FLOW" "$@"
}

json_ok() {
  printf '%s' "$1" | python3 -c 'import sys,json; json.loads(sys.stdin.read())' 2>/dev/null
}

# make_executed <id> [change_file] —— 全链路 new→design→review→translate→execute(stub)
make_executed() {
  local id="$1" cf="${2:-scripts/flow-core.py}"
  local brief="$WORK/brief-$id.md"
  echo "# brief $id" > "$brief"
  run workitem new "$id" --brief "$brief" >/dev/null 2>&1 || { echo "make_executed:new fail"; return 1; }
  run workitem design "$id" >/dev/null 2>&1 || { echo "make_executed:design fail"; return 1; }
  run workitem decision "$id" --verdict pass >/dev/null 2>&1 || { echo "make_executed:decision fail"; return 1; }
  run workitem transition "$id" reviewed >/dev/null 2>&1 || { echo "make_executed:reviewed fail"; return 1; }
  run workitem transition "$id" translated >/dev/null 2>&1 || { echo "make_executed:translated fail"; return 1; }
  cat > "$DATA/workitems/$id/taskbook.md" << 'TASKBOOK'
# taskbook
```flow
test_command: /bin/true
diff_scope:
  allow:
    - scripts/flow-core.py
  deny:
    - scripts/dsh-design
```
TASKBOOK
  STUB_EXECUTOR_CHANGE_FILE="$cf" run workitem execute "$id" --executor stub >/dev/null 2>&1 \
    || { echo "make_executed:execute fail"; return 1; }
}

state_of() { run workitem status "$1" --json 2>/dev/null | python3 -c 'import sys,json;print(json.loads(sys.stdin.read())["state"])' 2>/dev/null; }

echo "== C19: 全链路 new→design→review→translate→execute(stub) =="
if make_executed w1; then
  ST="$(state_of w1)"
  if [[ "$ST" == "executed" ]] \
     && [[ -f "$DATA/workitems/w1/executor/result.json" ]] \
     && [[ -s "$DATA/workitems/w1/executor/diff.patch" ]]; then
    ok "到 executed + result.json/diff.patch 落盘"
  else
    bad "C19" "state=$ST result=$(test -f "$DATA/workitems/w1/executor/result.json" && echo y || echo n)"
  fi
else
  bad "C19" "make_executed 失败"
fi

echo "== C20: verify --auto(全真 stub) → verified =="
OUT="$(run workitem verify w1 --auto --json 2>&1)"; RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"status":"ok"' \
   && [[ "$(state_of w1)" == "verified" ]] \
   && [[ -f "$DATA/workitems/w1/verify.md" ]]; then
  VJ="$DATA/workitems/w1/executor/verify.json"
  if python3 -c 'import json;v=json.load(open("'"$VJ"'",encoding="utf-8"));assert v["tests_pass"] and v["diff_match"] and v["error_table_match"]' 2>/dev/null; then
    ok "verified + verify.json 三真 + verify.md"
  else
    bad "C20" "verify.json 三真失败"
  fi
else
  bad "C20" "rc=$RC out=$OUT state=$(state_of w1)"
fi

echo "== C21: verify --auto(测试 fail) → executed + route=impl =="
make_executed w2 >/dev/null 2>&1
OUT="$(run workitem verify w2 --auto --test-command false --json 2>/dev/null)"; RC=$?
if [[ $RC -eq 1 ]] && [[ "$(state_of w2)" == "executed" ]]; then
  ROUTE="$(python3 -c 'import json;print(json.load(open("'"$DATA/workitems/w2/executor/verify.json"'",encoding="utf-8"))["route"])' 2>/dev/null)"
  if [[ "$ROUTE" == "impl" ]]; then ok "exit1 + executed + route=impl"; else bad "C21" "route=$ROUTE"; fi
else
  bad "C21" "rc=$RC state=$(state_of w2)"
fi

echo "== C22: verify --auto(diff 越界) → executed + out_of_scope =="
make_executed w3 scripts/dsh-design >/dev/null 2>&1
OUT="$(run workitem verify w3 --auto --json 2>/dev/null)"; RC=$?
if [[ $RC -eq 1 ]] && [[ "$(state_of w3)" == "executed" ]]; then
  OOS="$(python3 -c 'import json;v=json.load(open("'"$DATA/workitems/w3/executor/verify.json"'",encoding="utf-8"));print(v["details"]["diff"]["out_of_scope"])' 2>/dev/null)"
  if echo "$OOS" | grep -q "dsh-design"; then ok "exit1 + executed + out_of_scope"; else bad "C22" "oos=$OOS"; fi
else
  bad "C22" "rc=$RC state=$(state_of w3)"
fi

echo "== C23: verify --auto(错误表缺失) → designed + route=design =="
make_executed w4 >/dev/null 2>&1
printf '# design(无错误表)\n' > "$DATA/workitems/w4/design.md"
OUT="$(run workitem verify w4 --auto --json 2>/dev/null)"; RC=$?
if [[ $RC -eq 1 ]] && [[ "$(state_of w4)" == "designed" ]]; then
  ROUTE="$(python3 -c 'import json;print(json.load(open("'"$DATA/workitems/w4/executor/verify.json"'",encoding="utf-8"))["route"])' 2>/dev/null)"
  if [[ "$ROUTE" == "design" ]]; then ok "exit1 + designed + route=design"; else bad "C23" "route=$ROUTE"; fi
else
  bad "C23" "rc=$RC state=$(state_of w4)"
fi

echo "== C24: design/execute/verify --auto 的 --json 契约 =="
make_executed w5 >/dev/null 2>&1
# design --json(新 workitem)
BRIEF="$WORK/brief-w6.md"; echo "# brief w6" > "$BRIEF"
run workitem new w6 --brief "$BRIEF" >/dev/null 2>&1
OUT="$(run workitem design w6 --json 2>/dev/null)"; RC=$?
if [[ $RC -eq 0 ]] && [[ $(printf '%s' "$OUT" | grep -c .) -eq 1 ]] && json_ok "$OUT" >/dev/null && echo "$OUT" | grep -q '"status":"ok"'; then
  ok "design --json 单行"
else
  bad "C24a" "rc=$RC out=$OUT"
fi
# execute --json(force 重跑 w5,已 executed)
OUT="$(run workitem execute w5 --executor stub --force --json 2>/dev/null)"; RC=$?
if [[ $RC -eq 0 ]] && [[ $(printf '%s' "$OUT" | grep -c .) -eq 1 ]] && json_ok "$OUT" >/dev/null; then
  ok "execute --json 单行"
else
  bad "C24b" "rc=$RC out=$OUT"
fi
# verify --auto --json(失败走 stderr)
STDOUT="$(run workitem verify w5 --auto --test-command false --json 2>/dev/null)"; RC=$?
STDERR="$(run workitem verify w5 --auto --test-command false --json 2>&1 >/dev/null)"
if [[ $RC -eq 1 ]] && [[ -z "$STDOUT" ]] && echo "$STDERR" | grep -q '"status":"failed"'; then
  ok "verify --auto 失败走 stderr 单行"
else
  bad "C24c" "rc=$RC stdout=$STDOUT stderr=$STDERR"
fi

echo "== C25: 新增文件 deny-list 零命中 =="
# deny-list 正则动态拼接(拆段变量),避免脚本自引用
D_H1='/hom'; D_H2='e/[^~]'; D_SK='|sk-[A-Za-z0-9]{10}'
DENY_PAT="${D_H1}${D_H2}${D_SK}"
if grep -En "$DENY_PAT" \
     "$ROOT_DIR/scripts/flow-core.py" "$ROOT_DIR/scripts/flow" \
     "$ROOT_DIR/executors/reasonix/spec.yaml" "$ROOT_DIR/executors/reasonix/wrapper.sh" \
     "$ROOT_DIR/executors/stub/spec.yaml" "$ROOT_DIR/executors/stub/wrapper.sh" \
     "$ROOT_DIR/tests/test-executor.py" "$ROOT_DIR/tests/test_executor.py" \
     "$ROOT_DIR/tests/run-executor-smoke.sh" "$ROOT_DIR/config/defaults.yaml" \
     >/dev/null 2>&1; then
  bad "C25" "deny-list 命中"
else
  ok "deny-list 零命中"
fi

echo "== C26: 回归 run-smoke(15) + run-config-smoke(17) + run-flow-smoke(18) =="
bash "$ROOT_DIR/tests/run-smoke.sh" >"$WORK/reg1.log" 2>&1; RC1=$?
bash "$ROOT_DIR/tests/run-config-smoke.sh" >"$WORK/reg2.log" 2>&1; RC2=$?
bash "$ROOT_DIR/tests/run-flow-smoke.sh" >"$WORK/reg3.log" 2>&1; RC3=$?
if [[ $RC1 -eq 0 && $RC2 -eq 0 && $RC3 -eq 0 ]]; then
  ok "15+17+18 回归全绿"
else
  bad "C26" "run-smoke rc=$RC1 run-config-smoke rc=$RC2 run-flow-smoke rc=$RC3"
fi

echo
echo "========== 结果: $PASS 通过 / $FAIL 失败 =========="
if [[ $FAIL -gt 0 ]]; then
  printf '失败用例: %s\n' "${FAILED_CASES[*]}"
  exit 1
fi
exit 0
