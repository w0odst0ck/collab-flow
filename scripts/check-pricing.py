#!/usr/bin/env python3
"""check-pricing.py —— DeepSeek 官方计费页巡检(与本地 config/pricing.yaml 对比)。

用途:DeepSeek 改计费模式(时段/工作日限定)时自动发现,配合 cron + 飞书推送,
避免人工发现滞后。零 LLM、纯 stdlib、只读网络。

行为:
  - 抓官方定价页 → 正则解析「高峰时段为北京时间周一至周五 9:00 - 12:00、14:00 - 18:00」
  - 与本地 config/pricing.yaml 的 peak 段对比
  - 一致 → exit 0;不一致 → 打印差异 + exit 1(cron 侧据 exit code 推送)
  - 网络/解析失败 → 打印原因 + exit 2(不误报:只提示「无法确认」)

用法:
  python3 scripts/check-pricing.py                # 默认读 ../config/pricing.yaml
  PRICING_CONFIG=/path/pricing.yaml python3 ...   # 覆盖 config 路径
  python3 scripts/check-pricing.py --json         # 机器可读输出

cron 建议(零 LLM,不受峰谷约束):工作日 09:05 每天一次(官方若改价多为工作日发布)
  openclaw cron add check-pricing --cron "5 9 * * 1-5" --command \
    "bash -lc 'python3 ~/.openclaw/workspace/projects/collab-flow/scripts/check-pricing.py || ~/.openclaw/workspace/scripts/feishu_push.py ...'"
"""

import argparse
import json
import os
import re
import sys
import urllib.request

_CFG_PATH = os.environ.get("PRICING_CONFIG") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "config", "pricing.yaml")
_URL = "https://api-docs.deepseek.com/zh-cn/quick_start/pricing/"
_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
# 官方脚注格式:「高峰时段为北京时间周一至周五 9:00 - 12:00、14:00 - 18:00(其余为空闲时段)」
_RE_SPAN = re.compile(r"高峰时段为北京时间(?:周一至周五|周一 至 周五|周一至周日)?\s*([\d\s:：\-、]+)", re.S)


def _fetch_official():
    """抓官方页 → 返回 (weekday_only, spans)。spans=[(h1,h2),...];失败抛异常。"""
    req = urllib.request.Request(_URL, headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=25) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"\s+", " ", text)
    m = _RE_SPAN.search(text)
    if not m:
        raise RuntimeError("官方页未匹配到高峰时段描述(页面结构可能变了)")
    seg = m.group(0)
    weekday_only = "周一至周五" in seg or "周一 至 周五" in seg
    # 提取所有 "H[:MM] - H[:MM]" 段,保留分钟精度(非整点边界也能检出)
    spans = []
    for h1, m1, h2, m2 in re.findall(r"(\d{1,2})(?::(\d{2}))?\s*[-~至]\s*(\d{1,2})(?::(\d{2}))?", seg):
        spans.append((int(h1) * 60 + int(m1 or 0), int(h2) * 60 + int(m2 or 0)))
    if not spans:
        raise RuntimeError(f"官方时段解析失败: {seg!r}")
    return weekday_only, spans


def _load_local():
    """读本地 config peak 段。失败抛异常(带路径)。"""
    try:
        import yaml
    except ImportError:
        raise RuntimeError("缺少 pyyaml,无法读本地 config")
    with open(_CFG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    peak = cfg["peak"]
    # 本地 config 为整数小时,统一转分钟与官方比较(非整点变化可检出)
    spans = tuple((int(a) * 60, int(b) * 60) for a, b in peak["spans"])
    return bool(peak.get("weekday_only", True)), spans


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", action="store_true", help="机器可读 JSON 输出")
    args = ap.parse_args()

    def out(msg, code):
        if args.json:
            print(json.dumps({"ok": code == 0, "code": code, "msg": msg},
                             ensure_ascii=False))
        else:
            print(msg)
        sys.exit(code)

    try:
        off_wo, off_spans = _fetch_official()
    except Exception as e:
        return out(f"⚠️ check-pricing: 官方页无法确认({e})", 2)
    try:
        loc_wo, loc_spans = _load_local()
    except Exception as e:
        return out(f"⚠️ check-pricing: 本地 config 读取失败({_CFG_PATH}): {e}", 2)

    if off_wo == loc_wo and sorted(off_spans) == sorted(loc_spans):
        return out(f"✅ check-pricing: 官方与本地一致 "
                   f"(weekday_only={loc_wo}, spans={loc_spans})", 0)

    diff = (f"⚠️ check-pricing: DeepSeek 计费规则已变化!\n"
            f"  官方: weekday_only={off_wo}, spans={off_spans}(分钟)\n"
            f"  本地: weekday_only={loc_wo}, spans={loc_spans}(分钟)\n"
            f"  修法: 改 {_CFG_PATH} 的 peak 段 → python3 scripts/window.py 自检")
    return out(diff, 1)


if __name__ == "__main__":
    main()
