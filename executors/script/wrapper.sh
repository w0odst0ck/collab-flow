#!/usr/bin/env bash
# script executor 契约入口 —— flow-core 固定按 executors/<name>/wrapper.sh 找执行器,
# 此处转发到 run.sh(单一实现)。
exec "$(dirname "$(readlink -f "$0")")/run.sh" "$@"
