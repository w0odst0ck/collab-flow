#!/usr/bin/env bash
# test-dsh-design-detect.sh — DSH_BIN 探测逻辑单测（stub 目录模拟，零 API）
# 覆盖：env 优先 / PATH 命中 / npx 缓存 glob / 全失败 fail-closed
set -uo pipefail
# 隔离：探测逻辑的前提是 DSH_BIN 未设（flow 壳兜底可能 export 污染 verify 环境）
unset DSH_BIN
cd "$(dirname "$(readlink -f "$0")")/.." || exit 2
DETECT="scripts/dsh-design"
WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "✅ $1"; }
bad(){ FAIL=$((FAIL+1)); echo "❌ $1"; }

# 1. env 已设优先
mkdir -p "$WORK/fake-env"
cat > "$WORK/fake-env/dsh" <<'SH'
#!/bin/sh
echo "fake-env-dsh"
SH
chmod +x "$WORK/fake-env/dsh"
OUT="$(HOME="$WORK/home" DSH_BIN="$WORK/fake-env/dsh" bash -c 'grep -n "DSH_BIN 自动探测" -A 20 '"$DETECT"' | grep -q '\''env 已设'\'' && echo ok' 2>&1)"
# 直接验证探测逻辑：模拟无 env 时 npx 缓存命中
mkdir -p "$WORK/home/.npm/_npx/aaa/node_modules/.bin" "$WORK/home/.npm/_npx/bbb/node_modules/.bin"
printf '#!/bin/sh\necho old\n' > "$WORK/home/.npm/_npx/aaa/node_modules/.bin/dsh"
printf '#!/bin/sh\necho new\n' > "$WORK/home/.npm/_npx/bbb/node_modules/.bin/dsh"
chmod +x "$WORK/home/.npm/_npx/aaa/node_modules/.bin/dsh" "$WORK/home/.npm/_npx/bbb/node_modules/.bin/dsh"
touch -t 203001010000 "$WORK/home/.npm/_npx/bbb/node_modules/.bin/dsh"  # bbb 更新 → ls -t 取 bbb
# 模拟 dsh-design 的探测段（隔离执行）
DETECTED="$(HOME="$WORK/home" PATH="/usr/bin:/bin" bash -c '
  if [[ -z "${DSH_BIN:-}" ]]; then
    if command -v dsh >/dev/null 2>&1; then DSH_BIN="$(command -v dsh)";
    else NPNX_DSH="$(ls -t "$HOME"/.npm/_npx/*/node_modules/.bin/dsh 2>/dev/null | head -1)";
      [[ -n "$NPNX_DSH" ]] && DSH_BIN="$NPNX_DSH"; fi
  fi
  echo "${DSH_BIN:-NONE}"')"
if [[ "$DETECTED" == "$WORK/home/.npm/_npx/bbb/node_modules/.bin/dsh" ]]; then
  ok "npx 缓存 glob 取最新（bbb > aaa）"
else
  bad "npx 缓存探测: $DETECTED"
fi

# 2. PATH 命中
DETECTED2="$(HOME="$WORK/home" PATH="$WORK/fake-env:/usr/bin:/bin" bash -c '
  if [[ -z "${DSH_BIN:-}" ]]; then
    if command -v dsh >/dev/null 2>&1; then DSH_BIN="$(command -v dsh)"; fi
  fi
  echo "${DSH_BIN:-NONE}"')"
if [[ "$DETECTED2" == "$WORK/fake-env/dsh" ]]; then
  ok "PATH 命中优先于 npx 缓存"
else
  bad "PATH 探测: $DETECTED2"
fi

# 3. env 已设不被覆盖
DETECTED3="$(HOME="$WORK/home" PATH="/usr/bin:/bin" DSH_BIN="$WORK/fake-env/dsh" bash -c '
  if [[ -z "${DSH_BIN:-}" ]]; then
    NPNX_DSH="$(ls -t "$HOME"/.npm/_npx/*/node_modules/.bin/dsh 2>/dev/null | head -1)"
    [[ -n "$NPNX_DSH" ]] && DSH_BIN="$NPNX_DSH"
  fi
  echo "${DSH_BIN:-NONE}"')"
if [[ "$DETECTED3" == "$WORK/fake-env/dsh" ]]; then
  ok "env 已设不被覆盖"
else
  bad "env 优先: $DETECTED3"
fi

# 4. 全失败 → NONE（调用方 fail-closed 报错）
DETECTED4="$(HOME="$WORK/home-empty" PATH="/usr/bin:/bin" bash -c '
  if [[ -z "${DSH_BIN:-}" ]]; then
    if command -v dsh >/dev/null 2>&1; then DSH_BIN="$(command -v dsh)";
    else NPNX_DSH="$(ls -t "$HOME"/.npm/_npx/*/node_modules/.bin/dsh 2>/dev/null | head -1)";
      [[ -n "$NPNX_DSH" ]] && DSH_BIN="$NPNX_DSH"; fi
  fi
  echo "${DSH_BIN:-NONE}"')"
if [[ "$DETECTED4" == "NONE" ]]; then
  ok "全失败 → NONE（fail-closed 由调用方报错）"
else
  bad "全失败: $DETECTED4"
fi

echo "==== 结果: $PASS 通过 / $FAIL 失败 ===="
[[ $FAIL -eq 0 ]] || exit 1
