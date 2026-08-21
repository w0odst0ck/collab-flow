"""unittest discover 桥接:使 `python3 -m unittest discover tests` 能发现 test-flow-task.py。

镜像 tests/test_flow_core.py 模式:连字符文件名被 discover 跳过,以本合法命名文件加载
并重新暴露其测试类,不改动原测试文件。
"""

import importlib.util
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "tests.test_flow_task_core_impl", os.path.join(_HERE, "test-flow-task.py"))
_impl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_impl)

TaskStateMachineTests = _impl.TaskStateMachineTests
TaskStoreTests = _impl.TaskStoreTests
TaskRunnerTests = _impl.TaskRunnerTests
