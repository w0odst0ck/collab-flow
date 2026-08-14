#!/usr/bin/env python3
"""dsh-design-stats.py —— dsh-design 内部工具：token 统计 + projectKey 复刻 + 模型回验。

用法: dsh-design-stats.py <DSH_HOME> <project_cwd> <before_epoch>
输出: 单行 JSON
  - 成功: {"session":"session-…","model":"deepseek-v4-pro",
           "usage":{"inputTokens":…,"outputTokens":…,"cacheReadTokens":…,"reasoningTokens":…},
           "total":…,"turns":…}
  - 失败: {"error":"session_not_found" | "zstd_failed" | "bad_before" | "usage"}

设计要点（方案 §2.3 / §4）：
  1. projectKey 精确复刻 dsh 的 projectKey()（/ \\ : → '-', 连续折叠；安全字符保留；其余 ~XXXX；外层 --…--）。
  2. 只累加 assistant/message 的 data.usage（每轮权威值），显式忽略 assistant/chunk 的流式快照，防止同一轮重复计数。
  3. 模型取首个 request/header → data.header.config.model（实际生效模型，供主脚本 fail-closed 回验）。
  4. 会话定位：<DSH_HOME>/sessions/<projectKey(cwd)>/ 下 mtime >= before 的最新 session.jsonl.zstd。
  依赖 zstd + python3，不用 jq。
"""

import glob
import json
import os
import subprocess
import sys

def project_key(cwd):
    """复刻 dsh projectKey()：/ \\ : → '-'（连续折叠）；安全字符 [A-Za-z0-9._-] 保留；其余 ~XXXX；外层包裹 --…--。"""
    readable = []
    run = False
    for ch in cwd:
        if ch in "/\\:":
            if not run:
                readable.append("-")
            run = True
        elif ch != "~" and ((ch.isascii() and ch.isalnum()) or ch in "._-"):
            readable.append(ch)
            run = False
        else:
            readable.append("~" + format(ord(ch), "04X"))
            run = False
    core = ("".join(readable)).lstrip("-") or "root"
    return "--" + core[:251] + "--"


def main():
    if len(sys.argv) != 4:
        print(json.dumps({"error": "usage"}))
        return 2
    home, cwd, before = sys.argv[1], sys.argv[2], sys.argv[3]
    try:
        before = int(before)
    except ValueError:
        print(json.dumps({"error": "bad_before"}))
        return 2

    proj = os.path.join(home, "sessions", project_key(cwd))
    cands = []
    for p in glob.glob(os.path.join(proj, "*", "session.jsonl.zstd")):
        try:
            mt = os.path.getmtime(p)
        except OSError:
            continue
        if mt >= before:
            cands.append((mt, p))
    if not cands:
        print(json.dumps({"error": "session_not_found"}))
        return 0
    _, f = max(cands)

    model = None
    usage = dict(inputTokens=0, outputTokens=0, cacheReadTokens=0, reasoningTokens=0)
    turns = 0
    try:
        res = subprocess.run(["zstd", "-dc", f], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        print(json.dumps({"error": "zstd_failed"}))
        return 0
    if res.returncode != 0:
        print(json.dumps({"error": "zstd_failed"}))
        return 0

    for line in res.stdout.splitlines():
        if not line.strip():
            continue
        try:
            e = json.loads(line)
        except ValueError:
            continue
        t = e.get("type")
        if t == "request/header":
            # 实际生效模型（回验依据）
            model = ((e.get("data") or {}).get("header") or {}).get("config") or {}
            model = model.get("model")
        elif t == "assistant/message":
            # 只取权威每轮 usage；assistant/chunk 的流式快照必须忽略（防重复计数）
            u = (e.get("data") or {}).get("usage") or {}
            for k in usage:
                try:
                    usage[k] += int(u.get(k, 0) or 0)
                except (TypeError, ValueError):
                    pass
            turns += 1

    print(json.dumps({
        "session": os.path.basename(os.path.dirname(f)),
        "model": model,
        "usage": usage,
        "total": sum(usage.values()),
        "turns": turns,
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
