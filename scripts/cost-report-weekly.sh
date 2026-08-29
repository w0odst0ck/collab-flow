#!/usr/bin/env bash
# cost-weekly-report 薄壳(cron: 周一 09:00 Asia/Shanghai 推上周周报)。
# 零 LLM:仅转发 python3 scripts/cost-report.py --push。
# 凭证零硬编码:feishu-notify.sh 由 cost-report 经 FEISHU_NOTIFY env 定位,
# 其自身从 FEISHU_APP_SECRET/~/.bashrc 兜底,本脚本不接触 secret。
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$DIR/cost-report.py" --push
