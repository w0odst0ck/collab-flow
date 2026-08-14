#!/usr/bin/env bash
# dsh-design 配置层冒烟测试（17 用例，stub 模式零 API 消耗；方案 §5.2 表格）
# 用法: bash tests/run-config-smoke.sh
# 覆盖:缺失回退/自定义 config 生效/env>config/CLI>config/非法配置 fail-closed/
#       不可读/defaults 损坏/deny-list/等价性;stub 记录收到的 DSH_DESIGN_* 到 $DSH_STUB_ENV_RECORD
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
FLOW_CONFIG="$ROOT_DIR/scripts/flow-config"
DSH_DESIGN="$ROOT_DIR/scripts/dsh-design"
PASS=0; FAIL=0; FAILED_CASES=()

ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); FAILED_CASES+=("$1"); echo "  ❌ $1 — $2"; }

# ---------- 公共准备（隔离 HOME / 临时工作区 / stub） ----------
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
FAKE_KEY="sk-fake-test-key-$(date +%s)"
STUB="$WORK/stub-dsh.sh"

cat > "$STUB" << 'STUB'
#!/usr/bin/env bash
# stub dsh：模拟 headless 输出。env: DSH_STUB_EXIT/DSH_STUB_EMPTY/DSH_STUB_SLEEP/
# DSH_STUB_RECORD/DSH_STUB_SESSION/DSH_STUB_MODEL/DSH_STUB_ENV_RECORD
set -u
if [[ -n "${DSH_STUB_ENV_RECORD:-}" ]]; then
  env | grep '^DSH_DESIGN_' | sort > "$DSH_STUB_ENV_RECORD"
fi
if [[ -n "${DSH_STUB_RECORD:-}" ]]; then
  { echo "ARGS: $*"; echo "PWD: $PWD"; echo "PROMPT: ${*: -1}"; } > "$DSH_STUB_RECORD"
fi
if [[ "${DSH_STUB_SLEEP:-0}" != "0" ]]; then sleep "$DSH_STUB_SLEEP"; fi
if [[ "${DSH_STUB_SESSION:-0}" == "1" ]]; then
  MODEL="${DSH_STUB_MODEL:-deepseek-v4-flash}"
  python3 - "$DSH_HOME" "$PWD" "$MODEL" << 'PY'
import json, os, subprocess, sys, time
home, cwd, model = sys.argv[1], sys.argv[2], sys.argv[3]
def project_key(cwd):
    readable=[]; run=False
    for ch in cwd:
        if ch in "/\\:":
            if not run: readable.append("-")
            run=True
        elif ch != "~" and ((ch.isascii() and ch.isalnum()) or ch in "._-"):
            readable.append(ch); run=False
        else:
            readable.append("~"+format(ord(ch),"04X")); run=False
    core=("".join(readable)).lstrip("-") or "root"
    return "--" + core[:251] + "--"
proj = os.path.join(home, "sessions", project_key(cwd))
os.makedirs(proj, exist_ok=True)
sid = "session-stub-" + str(int(time.time()))
d = os.path.join(proj, sid); os.makedirs(d, exist_ok=True)
events = [
  {"type":"session","version":0,"id":sid,"createdAt":int(time.time()*1000),"cwd":cwd,"delegationDepth":0},
  {"type":"request/header","data":{"header":{"config":{"model":model}}}},
  {"type":"assistant/message","data":{"usage":{"inputTokens":10,"outputTokens":8,"cacheReadTokens":5,"reasoningTokens":2}}},
  {"type":"assistant/chunk","data":{"chunk":{"usage":{"inputTokens":9999,"outputTokens":9999,"cacheReadTokens":9999,"reasoningTokens":9999}}}},
]
raw = os.path.join(d,"session.jsonl")
with open(raw,"w") as f:
    for e in events: f.write(json.dumps(e)+"\n")
out = raw + ".zstd"
subprocess.run(["zstd", "-f", "-o", out, raw], check=True, capture_output=True)
os.remove(raw)
PY
fi
if [[ "${DSH_STUB_EMPTY:-0}" == "1" ]]; then exit "${DSH_STUB_EXIT:-0}"; fi
cat << 'MD'
# stub 方案

这是 stub 输出的假方案内容，用于配置层冒烟测试。

## 结论
- 用例：配置注入与优先级
MD
exit "${DSH_STUB_EXIT:-0}"
STUB
chmod +x "$STUB"

# 隔离 HOME：flow-config 用户配置默认路径、defaults 的 ~ 展开均落在此
mkdir -p "$WORK/home"

flow_run() {
  # usage: flow_run <cfg_path|-> [--env K=V ...] [--no-key] [--] args...
  # cfg_path='-' 表示不注入 COLLABFLOW_CONFIG（缺失回退）
  local cfg="$1"; shift
  rm -f "$WORK/stub_env.txt"
  # 每个用例从「无 pro patch」开始:自愈路径可测,且不同用例的模型配置不互相污染
  rm -f "$WORK/home/.config/dsh-design/pro.patch.yml"
  local envs=(HOME="$WORK/home" FLOW_DSH_DESIGN="$DSH_DESIGN" DSH_BIN="$STUB" DSH_STUB_ENV_RECORD="$WORK/stub_env.txt")
  local no_key=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --env) envs+=("$2"); shift 2 ;;
      --no-key) no_key=1; shift ;;
      --) shift; break ;;
      *) break ;;
    esac
  done
  [[ $no_key -eq 0 ]] && envs+=(DEEPSEEK_API_KEY="$FAKE_KEY")
  if [[ "$cfg" == "-" ]]; then
    env "${envs[@]}" "$FLOW_CONFIG" "$@" 2>&1
  else
    env "${envs[@]}" COLLABFLOW_CONFIG="$cfg" "$FLOW_CONFIG" "$@" 2>&1
  fi
}

echo "== 用例 1: 无 user config → 静默回退 defaults =="
mkdir -p "$WORK/proj1"
OUT="$(flow_run - -- -d "$WORK/proj1" -o "$WORK/proj1/designs" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 0 ]] && ! echo "$OUT" | grep -q "错误:" \
   && grep -q '^DSH_DESIGN_PRO_MODEL=deepseek-v4-pro$' "$WORK/stub_env.txt" \
   && grep -q '^DSH_DESIGN_TIMEOUT=1800$' "$WORK/stub_env.txt" \
   && grep -q '^DSH_DESIGN_PERMISSION=read-only$' "$WORK/stub_env.txt"; then
  ok "exit0+默认值注入+无报错"; else bad "用例1" "rc=$RC out=$OUT env=$(cat "$WORK/stub_env.txt" 2>/dev/null | tr '\n' ' ')"; fi

echo "== 用例 2: 自定义 config timeout 生效（stub sleep 5 → 124） =="
CFG2="$WORK/cfg2.yaml"; cat > "$CFG2" <<'EOF'
version: 1
roles:
  designer:
    timeout_s: 1
EOF
mkdir -p "$WORK/proj2"
OUT="$(flow_run "$CFG2" --env DSH_STUB_SLEEP=5 -- -d "$WORK/proj2" -o "$WORK/proj2/designs" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 124 ]]; then ok "exit124（自定义 timeout=1 生效）"; else bad "用例2" "rc=$RC out=$OUT"; fi

echo "== 用例 3: 自定义 config model.pro 贯穿 patch 自愈+grep+回验 =="
CFG3="$WORK/cfg3.yaml"; cat > "$CFG3" <<'EOF'
version: 1
roles:
  designer:
    model:
      pro: deepseek-v4-custompro
EOF
mkdir -p "$WORK/proj3"
OUT="$(flow_run "$CFG3" --env DSH_STUB_SESSION=1 --env DSH_STUB_MODEL=deepseek-v4-custompro -- \
    -d "$WORK/proj3" -o "$WORK/proj3/designs" "任务" 2>&1)"
RC=$?
MANIFEST3="$WORK/proj3/designs/.dsh-design/manifest.jsonl"
if [[ $RC -eq 0 ]] && [[ -f "$MANIFEST3" ]] \
   && grep -q '"model": "deepseek-v4-custompro"' "$MANIFEST3"; then
  ok "exit0+manifest model=自定义值"; else bad "用例3" "rc=$RC manifest=$(cat "$MANIFEST3" 2>/dev/null || echo 无) out=$OUT"; fi

echo "== 用例 4: 自定义 config key.source（dotenv）生效 =="
echo "DEEPSEEK_API_KEY=sk-custom-key-$(date +%s)" > "$WORK/home/custom.env"
CFG4="$WORK/cfg4.yaml"; cat > "$CFG4" <<'EOF'
version: 1
roles:
  designer:
    key:
      source: ~/custom.env
EOF
mkdir -p "$WORK/proj4"
OUT="$(flow_run "$CFG4" --no-key -- -d "$WORK/proj4" -o "$WORK/proj4/designs" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q "方案:"; then
  ok "exit0（key 从自定义 dotenv 解析）"; else bad "用例4" "rc=$RC out=$OUT"; fi

echo "== 用例 5: 优先级 env > config =="
CFG5="$WORK/cfg5.yaml"; cat > "$CFG5" <<'EOF'
version: 1
roles:
  designer:
    timeout_s: 1800
EOF
mkdir -p "$WORK/proj5"
OUT="$(flow_run "$CFG5" --env DSH_STUB_SLEEP=5 --env DSH_DESIGN_TIMEOUT=1 -- \
    -d "$WORK/proj5" -o "$WORK/proj5/designs" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 124 ]]; then ok "env 覆盖 config（1s 超时生效）"; else bad "用例5" "rc=$RC out=$OUT"; fi

echo "== 用例 6: 优先级 CLI > config =="
mkdir -p "$WORK/proj6"
OUT="$(flow_run "$CFG5" --env DSH_STUB_SLEEP=5 -- -d "$WORK/proj6" -o "$WORK/proj6/designs" -t 1 "任务" 2>&1)"
RC=$?
if [[ $RC -eq 124 ]]; then ok "CLI -t 覆盖 config（1s 超时生效）"; else bad "用例6" "rc=$RC out=$OUT"; fi

echo "== 用例 7: 非法配置（明文 key）→ exit 2 =="
CFG7="$WORK/cfg7.yaml"; cat > "$CFG7" <<'EOF'
version: 1
roles:
  designer:
    key:
      env_ref: sk-abc123
EOF
OUT="$(flow_run "$CFG7" -- -d "$WORK/proj7" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "明文 key"; then
  ok "exit2+报明文 key"; else bad "用例7" "rc=$RC out=$OUT"; fi

echo "== 用例 8: 非法配置（key 绑定非 \${DEEPSEEK_API_KEY}）→ exit 2 =="
CFG8="$WORK/cfg8.yaml"; cat > "$CFG8" <<'EOF'
version: 1
roles:
  designer:
    key:
      env_ref: ${OTHER}
EOF
OUT="$(flow_run "$CFG8" -- -d "$WORK/proj8" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "P1 仅支持"; then
  ok "exit2+报 P1 仅支持"; else bad "用例8" "rc=$RC out=$OUT"; fi

echo "== 用例 9: 非法配置（permission）→ exit 2 =="
CFG9="$WORK/cfg9.yaml"; cat > "$CFG9" <<'EOF'
version: 1
roles:
  designer:
    permission: everything
EOF
OUT="$(flow_run "$CFG9" -- -d "$WORK/proj9" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "permission"; then
  ok "exit2+报 permission"; else bad "用例9" "rc=$RC out=$OUT"; fi

echo "== 用例 10: 非法配置（timeout）→ exit 2 =="
CFG10="$WORK/cfg10.yaml"; cat > "$CFG10" <<'EOF'
version: 1
roles:
  designer:
    timeout_s: abc
EOF
OUT="$(flow_run "$CFG10" -- -d "$WORK/proj10" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "timeout_s"; then
  ok "exit2+报 timeout_s"; else bad "用例10" "rc=$RC out=$OUT"; fi

echo "== 用例 11: 非法配置（YAML 缩进错误）→ exit 2 =="
CFG11="$WORK/cfg11.yaml"; cat > "$CFG11" <<'EOF'
version: 1
roles:
  designer:
    timeout_s: 1
   permission: read-only
EOF
OUT="$(flow_run "$CFG11" -- -d "$WORK/proj11" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "配置解析失败"; then
  ok "exit2+报解析失败"; else bad "用例11" "rc=$RC out=$OUT"; fi

echo "== 用例 12: 非法配置（不支持语法：锚点）→ exit 2 =="
CFG12="$WORK/cfg12.yaml"; cat > "$CFG12" <<'EOF'
version: 1
roles: &a
  designer:
    timeout_s: 1
EOF
OUT="$(flow_run "$CFG12" -- -d "$WORK/proj12" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "不支持"; then
  ok "exit2+报不支持"; else bad "用例12" "rc=$RC out=$OUT"; fi

echo "== 用例 13: 非法配置（注入字符）→ exit 2 =="
CFG13="$WORK/cfg13.yaml"; cat > "$CFG13" <<'EOF'
version: 1
roles:
  designer:
    model:
      pro: "x; echo pwned"
EOF
OUT="$(flow_run "$CFG13" -- -d "$WORK/proj13" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "非法" && [[ ! -f "$WORK/stub_env.txt" ]]; then
  ok "exit2+拒绝注入+dsh 未被调用"; else bad "用例13" "rc=$RC out=$OUT"; fi

echo "== 用例 14: user config 存在但不可读 → exit 2（fail-closed，区分缺失） =="
CFG14="$WORK/cfg14.yaml"; cat > "$CFG14" <<'EOF'
version: 1
roles:
  designer:
    timeout_s: 1
EOF
chmod 000 "$CFG14"
OUT="$(flow_run "$CFG14" -- -d "$WORK/proj14" "任务" 2>&1)"
RC=$?
chmod 644 "$CFG14"
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "配置不可读"; then
  ok "exit2+报配置不可读"; else bad "用例14" "rc=$RC out=$OUT"; fi

echo "== 用例 15: defaults 自身损坏 → exit 2 =="
BAD_DEFAULTS="$WORK/bad-defaults.yaml"; echo "version: [oops" > "$BAD_DEFAULTS"
OUT="$(flow_run - --env COLLABFLOW_DEFAULTS="$BAD_DEFAULTS" -- -d "$WORK/proj15" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "默认配置不可用"; then
  ok "exit2+报默认配置不可用"; else bad "用例15" "rc=$RC out=$OUT"; fi

echo "== 用例 16: defaults 零个人标识（deny-list 零命中） =="
if grep -nE '/home/[^~]|sk-|private-placeholder' "$ROOT_DIR/config/defaults.yaml" >/dev/null 2>&1; then
  bad "用例16" "deny-list 命中"; else ok "deny-list 零命中"; fi

echo "== 用例 17: 等价性（flow-config 无 user config vs 直调 dsh-design） =="
mkdir -p "$WORK/proj17a" "$WORK/proj17b"
OUT_A="$(env HOME="$WORK/home" DSH_HOME="$WORK/dshhome" DEEPSEEK_API_KEY="$FAKE_KEY" \
    DSH_BIN="$STUB" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" \
    DSH_STUB_SESSION=1 DSH_STUB_MODEL=deepseek-v4-pro \
    "$DSH_DESIGN" -d "$WORK/proj17a" -o "$WORK/proj17a/designs" "设计一个测试工具" 2>&1)"
RC_A=$?
OUT_B="$(flow_run - --env DSH_STUB_SESSION=1 --env DSH_STUB_MODEL=deepseek-v4-pro -- \
    -d "$WORK/proj17b" -o "$WORK/proj17b/designs" "设计一个测试工具" 2>&1)"
RC_B=$?
MD_A=$(find "$WORK/proj17a/designs" -name "*.md" 2>/dev/null | wc -l)
MD_B=$(find "$WORK/proj17b/designs" -name "*.md" 2>/dev/null | wc -l)
MODEL_A="$(echo "$OUT_A" | grep -o '模型: [^ ]*' | head -1)"
MODEL_B="$(echo "$OUT_B" | grep -o '模型: [^ ]*' | head -1)"
if [[ $RC_A -eq 0 && $RC_B -eq 0 && $MD_A -ge 1 && $MD_B -ge 1 && "$MODEL_A" == "$MODEL_B" ]]; then
  ok "等价：exit0+均落盘+模型行一致（$MODEL_A）"
else
  bad "用例17" "rcA=$RC_A rcB=$RC_B mdA=$MD_A mdB=$MD_B modelA=$MODEL_A modelB=$MODEL_B"
fi

echo
echo "========== 结果: $PASS 通过 / $FAIL 失败 =========="
if [[ $FAIL -gt 0 ]]; then
  printf '失败用例: %s\n' "${FAILED_CASES[*]}"
  exit 1
fi
exit 0
