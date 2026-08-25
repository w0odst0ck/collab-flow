"""unittest discover 桥接:使 `python3 -m unittest discover tests` 能发现 test-flow-task-wb.py。

镜像 tests/test_flow_core.py 模式:连字符文件名被 discover 静默跳过(非法模块名),
以本合法命名文件加载并重新暴露其测试类,不改动原测试文件。
"""

import importlib.util
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "tests.test_flow_task_wb_impl", os.path.join(_HERE, "test-flow-task-wb.py"))
_impl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_impl)

TerminalHookTests = _impl.TerminalHookTests
StaleEvolveTests = _impl.StaleEvolveTests
StaleForceTests = _impl.StaleForceTests
ArchiveTests = _impl.ArchiveTests
ResolveScanRootsTests = _impl.ResolveScanRootsTests   # ocr6-F3:扫描根解析
LearnExpectedTests = _impl.LearnExpectedTests
AuditTests = _impl.AuditTests
RedlineTests = _impl.RedlineTests
LastEventTsTests = _impl.LastEventTsTests   # ocr3 L2:last_event_ts 有界尾部读取
