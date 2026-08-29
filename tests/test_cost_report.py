"""unittest discover 桥接:使 `python3 -m unittest discover tests` 能发现 test-cost-report.py。

unittest discover 仅识别合法 Python 模块名文件([_a-z]\\w*\\.py),test-cost-report.py 含
连字符会被静默跳过。故以本合法命名文件加载并重新暴露其测试类,不改动原测试文件。
"""

import importlib.util
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "tests.test_cost_report_impl", os.path.join(_HERE, "test-cost-report.py"))
_impl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_impl)

LoadRegistryTests = _impl.LoadRegistryTests
WeekWindowTests = _impl.WeekWindowTests
BucketTests = _impl.BucketTests
CostAggregationTests = _impl.CostAggregationTests
SavingsTests = _impl.SavingsTests
ConfigTests = _impl.ConfigTests
CliAndJsonTests = _impl.CliAndJsonTests
PushTests = _impl.PushTests
