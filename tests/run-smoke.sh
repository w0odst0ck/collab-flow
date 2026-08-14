#!/usr/bin/env bash
# dsh-design 冒烟测试（15 断言，stub 模式零 API 消耗）
# 用法: bash tests/run-smoke.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
DSH_DESIGN="$ROOT_DIR/scripts/dsh-design"
PASS=0; FAIL=0; FAILED_CASES=()

ok()   { PASS=$((PASS+1)); echo "  ✅ $1"; }
bad()  { FAIL=$((FAIL+1)); FAILED_CASES+=("$1"); echo "  ❌ $1 — $2"; }

# ---------- 公共准备 ----------
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
FAKE_KEY="sk-fake-test-key-$(date +%s)"
STUB="$WORK/stub-dsh.sh"

cat > "$STUB" << 'STUB'
#!/usr/bin/env bash
# stub dsh：模拟 headless 输出。env: DSH_STUB_EXIT/DSH_STUB_EMPTY/DSH_STUB_SLEEP/
# DSH_STUB_RECORD/DSH_STUB_SESSION/DSH_STUB_MODEL
set -u
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

这是 stub 输出的假方案内容，用于冒烟测试。

## 结论
- 用例 1：字符串任务成功落盘
- 用例 2：简报文件输入
MD
exit "${DSH_STUB_EXIT:-0}"
STUB
chmod +x "$STUB"

run_design() { # 传 env 前缀给 env 命令；其余参数是 dsh-design 的参数
  env HOME="$WORK/home" DSH_HOME="$WORK/dshhome" DEEPSEEK_API_KEY="$FAKE_KEY" \
      DSH_BIN="$STUB" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" \
      "$DSH_DESIGN" "$@" 2>&1
}

# 预建各用例项目目录
for n in 1 1c 2 3 4 5 6 7 8 9 10 12; do mkdir -p "$WORK/proj$n"; done

echo "== 用例 1: 字符串任务成功落盘（非 json，验证 DESIGN= 打印） =="
OUT="$(run_design -d "$WORK/proj1" -o "$WORK/proj1/designs" "设计一个测试工具" 2>&1)"
RC=$?
DESIGN_FILE=$(find "$WORK/proj1/designs" -name "*.md" 2>/dev/null | head -1)
if [[ $RC -eq 0 && -n "$DESIGN_FILE" && -s "$DESIGN_FILE" ]] \
   && echo "$OUT" | grep -q "DESIGN="; then ok "exit0+落盘+DESIGN打印"; else bad "用例1" "rc=$RC out=$OUT"; fi

echo "== 用例 1c: --json 单行输出 =="
OUT="$(run_design --json -d "$WORK/proj1c" -o "$WORK/proj1c/designs" "设计一个测试工具" 2>&1)"
RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q '"design":'; then ok "json 单行含 design 字段"; else bad "用例1c" "rc=$RC out=$OUT"; fi

echo "== 用例 2: 简报文件输入 =="
BRIEF_FILE="$WORK/brief.md"; echo "# 简报 测试任务" > "$BRIEF_FILE"
RECORD="$WORK/rec2.txt"
OUT="$(env HOME="$WORK/home" DSH_HOME="$WORK/dshhome" DEEPSEEK_API_KEY="$FAKE_KEY" \
       DSH_BIN="$STUB" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" DSH_STUB_RECORD="$RECORD" \
       "$DSH_DESIGN" -d "$WORK/proj2" "$BRIEF_FILE" 2>&1)"
RC=$?
if [[ $RC -eq 0 && -f "$RECORD" ]] && grep -q "简报 测试任务" "$RECORD"; then ok "简报内容进入 prompt"; else bad "用例2" "rc=$RC"; fi

echo "== 用例 3: 简报文件不存在 =="
OUT="$(run_design -d "$WORK/proj3" "$WORK/nope.md" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "不是普通文件\|不存在"; then ok "exit2+报错"; else bad "用例3" "rc=$RC out=$OUT"; fi

echo "== 用例 4: key 缺失 =="
OUT="$(env HOME="$WORK/home-nokey" DSH_HOME="$WORK/dshhome" DSH_BIN="$STUB" \
       DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" "$DSH_DESIGN" -d "$WORK/proj4" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && echo "$OUT" | grep -q "DEEPSEEK_API_KEY"; then ok "exit2+报缺key"; else bad "用例4" "rc=$RC out=$OUT"; fi
if echo "$OUT" | grep -q "$FAKE_KEY"; then bad "用例4b" "key 泄露!"; else ok "无 key 泄露"; fi

echo "== 用例 5: pro 补丁被改回 flash（fail-closed，不调 stub） =="
FLASH_PATCH="$WORK/flash.patch.yml"
printf -- "- id: agent-default-model\n  config:\n    provider: deepseek-official\n    model: deepseek-v4-flash\n" > "$FLASH_PATCH"
RECORD5="$WORK/rec5.txt"
OUT="$(env HOME="$WORK/home" DSH_HOME="$WORK/dshhome" DEEPSEEK_API_KEY="$FAKE_KEY" \
       DSH_BIN="$STUB" DSH_DESIGN_PRO_PATCH="$FLASH_PATCH" DSH_STUB_RECORD="$RECORD5" \
       "$DSH_DESIGN" -d "$WORK/proj5" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 2 ]] && [[ ! -f "$RECORD5" ]]; then ok "exit2+未调stub"; else bad "用例5" "rc=$RC stub被调用=$([[ -f "$RECORD5" ]] && echo yes || echo no)"; fi

echo "== 用例 6: 模型回验失败 fail-closed =="
OUT="$(env HOME="$WORK/home" DSH_HOME="$WORK/dshhome" DEEPSEEK_API_KEY="$FAKE_KEY" \
       DSH_BIN="$STUB" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" \
       DSH_STUB_SESSION=1 DSH_STUB_MODEL="deepseek-v4-flash" \
       "$DSH_DESIGN" -d "$WORK/proj6" -o "$WORK/proj6/designs" "任务" 2>&1)"
RC=$?
NEW_MD=$(find "$WORK/proj6/designs" -name "*.md" 2>/dev/null | wc -l)
if [[ $RC -eq 1 ]] && [[ "$NEW_MD" == "0" ]] && echo "$OUT" | grep -q "模型回验失败"; then ok "exit1+不落盘"; else bad "用例6" "rc=$RC md=$NEW_MD out=$OUT"; fi

echo "== 用例 7: 成本统计去重（只算 message，不算 chunk） =="
OUT="$(env HOME="$WORK/home" DSH_HOME="$WORK/dshhome" DEEPSEEK_API_KEY="$FAKE_KEY" \
       DSH_BIN="$STUB" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" \
       DSH_STUB_SESSION=1 DSH_STUB_MODEL="deepseek-v4-pro" \
       "$DSH_DESIGN" -d "$WORK/proj7" -o "$WORK/proj7/designs" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q "total=25"; then ok "total=25（10+8+5+2，chunk 9999 未计）"; else bad "用例7" "rc=$RC out=$OUT"; fi

echo "== 用例 8: dsh 失败不落盘 =="
OUT="$(env HOME="$WORK/home" DSH_HOME="$WORK/dshhome" DEEPSEEK_API_KEY="$FAKE_KEY" \
       DSH_BIN="$STUB" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" DSH_STUB_EXIT=1 \
       "$DSH_DESIGN" -d "$WORK/proj8" -o "$WORK/proj8/designs" "任务" 2>&1)"
RC=$?
NEW_MD=$(find "$WORK/proj8/designs" -name "*.md" 2>/dev/null | wc -l)
if [[ $RC -eq 1 ]] && [[ "$NEW_MD" == "0" ]]; then ok "exit1+不落盘"; else bad "用例8" "rc=$RC md=$NEW_MD"; fi
if [[ -f "$WORK/proj8/designs/.dsh-design/manifest.jsonl" ]] \
   && grep -q '"status": "failed"' "$WORK/proj8/designs/.dsh-design/manifest.jsonl"; then ok "manifest 记 failed"; else bad "用例8b" "manifest 缺失或未记失败"; fi

echo "== 用例 9: 空输出不写空 md =="
OUT="$(env HOME="$WORK/home" DSH_HOME="$WORK/dshhome" DEEPSEEK_API_KEY="$FAKE_KEY" \
       DSH_BIN="$STUB" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" DSH_STUB_EMPTY=1 \
       "$DSH_DESIGN" -d "$WORK/proj9" -o "$WORK/proj9/designs" "任务" 2>&1)"
RC=$?
NEW_MD=$(find "$WORK/proj9/designs" -name "*.md" 2>/dev/null | wc -l)
if [[ $RC -eq 1 ]] && [[ "$NEW_MD" == "0" ]]; then ok "exit1+不写空md"; else bad "用例9" "rc=$RC md=$NEW_MD"; fi

echo "== 用例 10: 超时 =="
OUT="$(env HOME="$WORK/home" DSH_HOME="$WORK/dshhome" DEEPSEEK_API_KEY="$FAKE_KEY" \
       DSH_BIN="$STUB" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" \
       DSH_STUB_SLEEP=5 DSH_DESIGN_TIMEOUT=1 \
       "$DSH_DESIGN" -d "$WORK/proj10" -o "$WORK/proj10/designs" "任务" 2>&1)"
RC=$?
if [[ $RC -eq 124 ]]; then ok "exit124"; else bad "用例10" "rc=$RC"; fi

echo "== 用例 11: 项目只读红线（除 designs/ 外零改动） =="
PROJ11="$WORK/proj11"; mkdir -p "$PROJ11"
echo "sentinel" > "$PROJ11/existing.txt"
SNAP_BEFORE="$(find "$PROJ11" -type f ! -path "*/designs/*" | sort | xargs -r md5sum 2>/dev/null)"
run_design -d "$PROJ11" -o "$PROJ11/designs" "任务" >/dev/null 2>&1
SNAP_AFTER="$(find "$PROJ11" -type f ! -path "*/designs/*" | sort | xargs -r md5sum 2>/dev/null)"
if [[ "$SNAP_BEFORE" == "$SNAP_AFTER" ]]; then ok "项目除 designs/ 外零改动"; else bad "用例11" "快照不一致"; fi

echo "== 用例 12: -d/-o 目录参数 =="
RECORD12="$WORK/rec12.txt"
OUT="$(env HOME="$WORK/home" DSH_HOME="$WORK/dshhome" DEEPSEEK_API_KEY="$FAKE_KEY" \
       DSH_BIN="$STUB" DSH_DESIGN_PRO_PATCH="$WORK/pro.patch.yml" DSH_STUB_RECORD="$RECORD12" \
       "$DSH_DESIGN" -d "$WORK/proj12" -o "$WORK/custom-out" "任务" 2>&1)"
RC=$?
DESIGN_FILE=$(find "$WORK/custom-out" -name "*.md" 2>/dev/null | head -1)
if [[ $RC -eq 0 && -n "$DESIGN_FILE" ]] && grep -q "PWD: $WORK/proj12" "$RECORD12"; then
  ok "输出到 -o 目录 + dsh 在 -d 目录运行"
else
  bad "用例12" "rc=$RC design=$DESIGN_FILE"
fi

echo
echo "========== 结果: $PASS 通过 / $FAIL 失败 =========="
if [[ $FAIL -gt 0 ]]; then
  printf '失败用例: %s\n' "${FAILED_CASES[*]}"
  exit 1
fi
exit 0
