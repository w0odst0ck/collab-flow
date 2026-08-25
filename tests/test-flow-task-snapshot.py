#!/usr/bin/env python3
# noqa: N999
# 注:文件名连字符为项目惯例(与 test-flow-core.py 等旧文件一致)
"""flow-task-core.py W-V1 execute 基线快照单元测试(design verify-baseline-snapshot §3)。

用法: python3 -m unittest discover tests
零 API、全程 FLOW_TASK_DIR 临时目录隔离;git fixture 用 tempfile + git init,
绝不碰真实业务仓(R7 红线)。覆盖 §3 用例级:内容正确(M hash/untracked)、
非 git/git 缺失容错、干净仓、已删 tracked=null、workdir 回退、写读 roundtrip、
缺失/损坏/形状非法→None、写 OSError→False、DENY 拒写、dispatch 先快照后
registry、三入口(dispatch/run/pump)、prune 清理、cmd_snapshot CLI。
"""

import contextlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
_TASK_CORE = os.path.join(_HERE, "..", "scripts", "flow-task-core.py")

_spec = importlib.util.spec_from_file_location("flow_task_core_snap", _TASK_CORE)
tc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tc)

# 故意拼接,避免源码静态 deny-list 命中(sk-[A-Za-z0-9]{16,})
_FAKE_SECRET = "sk-" + "fakekey1234567890123456789"


def _cfg(**kw):
    base = {"task": {"schema_version": 1, "max_parallel": 2, "default_priority": "P2",
                     "log_tail_bytes": 2000, "kill_grace_s": 1, "queue_cap": 50,
                     "expected_seconds_seed": {}}}
    base["task"].update(kw)
    return base


class SnapshotIsoBase(unittest.TestCase):
    """FLOW_TASK_DIR 临时目录隔离 + tempfile git fixture(绝不碰真实业务仓)。"""

    def setUp(self):
        self._saved_cwd = os.getcwd()
        self._saved_env = os.environ.get("FLOW_TASK_DIR")
        self.tmp = tempfile.mkdtemp(prefix="flow-snap-")
        os.environ["FLOW_TASK_DIR"] = os.path.join(self.tmp, "task")
        # 脏状态 fixture 仓:commit a/b/c + .gitignore → 改 b → 删 c → 增 u1/ignored
        self.repo = os.path.join(self.tmp, "repo")
        self.clean_repo = os.path.join(self.tmp, "clean")
        os.makedirs(self.repo)
        os.makedirs(self.clean_repo)
        self._make_dirty_repo()
        self._make_clean_repo()

    def tearDown(self):
        if self._saved_env is None:
            os.environ.pop("FLOW_TASK_DIR", None)
        else:
            os.environ["FLOW_TASK_DIR"] = self._saved_env
        os.chdir(self._saved_cwd)
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- git fixture 构造 ---------------------------------------------------

    def _git(self, repo, *args):
        return subprocess.run(["git", "-C", repo, *args],
                              capture_output=True, text=True, check=False)

    def _commit(self, repo):
        r = self._git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t",
                      "commit", "-q", "-m", "init")
        self.assertEqual(r.returncode, 0, r.stderr)

    def _make_dirty_repo(self):
        for name, content in (("a.txt", "a\n"), ("b.txt", "b\n"), ("c.txt", "c\n")):
            with open(os.path.join(self.repo, name), "w", encoding="utf-8") as f:
                f.write(content)
        with open(os.path.join(self.repo, ".gitignore"), "w", encoding="utf-8") as f:
            f.write("ignored.txt\n")
        self._git(self.repo, "init", "-q")
        self._git(self.repo, "add", ".")
        self._commit(self.repo)
        # 制造 1 个 M(b.txt) + 1 个 D(c.txt) + 1 个 untracked(u1.txt) + 1 个 ignored
        with open(os.path.join(self.repo, "b.txt"), "w", encoding="utf-8") as f:
            f.write("b2\n")
        os.remove(os.path.join(self.repo, "c.txt"))
        with open(os.path.join(self.repo, "u1.txt"), "w", encoding="utf-8") as f:
            f.write("u1\n")
        with open(os.path.join(self.repo, "ignored.txt"), "w", encoding="utf-8") as f:
            f.write("ign\n")

    def _make_clean_repo(self):
        with open(os.path.join(self.clean_repo, "a.txt"), "w", encoding="utf-8") as f:
            f.write("a\n")
        self._git(self.clean_repo, "init", "-q")
        self._git(self.clean_repo, "add", ".")
        self._commit(self.clean_repo)

    def _head(self, repo):
        r = self._git(repo, "rev-parse", "HEAD")
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout.strip()

    def _hash(self, repo, path):
        r = self._git(repo, "hash-object", path)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout.strip()

    # -- 任务 seed ----------------------------------------------------------

    def _seed_task(self, tid="t-000000000001", state="queued", kind="execute",
                   workdir=None, scheduled_at=None, **over):
        reg = tc.load_registry()
        entry = {
            "id": tid, "workitem": None, "command": "true", "priority": "P2",
            "state": state, "kind": kind, "workdir": workdir or self.repo,
            "expected_seconds": None, "kill_on_timeout": False,
            "created_at": tc.now_iso(), "started_at": None, "finished_at": None,
            "exit_code": None, "failure_tail": None, "pid": None,
            "heartbeat_at": None, "scheduled_at": scheduled_at, "why": "test",
        }
        entry.update(over)
        reg["tasks"][tid] = entry
        tc.save_registry_atomic(reg)
        return tid

    def _valid_snap(self, tid="t-000000000001", **over):
        snap = {"schema_version": tc.SNAPSHOT_SCHEMA_VERSION, "task_id": tid,
                "captured_at": "2026-08-24T10:00:00.000+00:00",
                "workdir": self.repo, "git": True, "head": self._head(self.repo),
                "tracked_modified": {"b.txt": self._hash(self.repo, "b.txt"),
                                     "c.txt": None},
                "untracked": ["u1.txt"]}
        snap.update(over)
        return snap


# ---------------------------------------------------------------------------
# capture_git_snapshot 内容与容错(§3 用例级)
# ---------------------------------------------------------------------------

class CaptureSnapshotTests(SnapshotIsoBase):
    """快照内容正确性 + 非 git/git 缺失/干净仓/已删/回退。"""

    def test_snapshot_content_correct(self):
        snap = tc.capture_git_snapshot(self.repo)
        self.assertTrue(snap["git"])
        self.assertEqual(snap["head"], self._head(self.repo))
        # M 文件 content-hash == git hash-object(与 executor .diff.names 同源,D2)
        self.assertEqual(snap["tracked_modified"]["b.txt"], self._hash(self.repo, "b.txt"))
        # 已删 tracked → null(D4)
        self.assertIsNone(snap["tracked_modified"]["c.txt"])
        # untracked 文件级清单;ignored 不在其中(D3)
        self.assertIn("u1.txt", snap["untracked"])
        self.assertNotIn("ignored.txt", snap["untracked"])
        # 未改动文件不出现在 tracked_modified
        self.assertNotIn("a.txt", snap["tracked_modified"])
        self.assertEqual(snap["schema_version"], tc.SNAPSHOT_SCHEMA_VERSION)

    def test_snapshot_non_git_workdir(self):
        snap = tc.capture_git_snapshot(self.tmp)  # 无 .git
        self.assertFalse(snap["git"])
        self.assertEqual(snap["error"], "not_a_git_repo")

    def test_snapshot_git_missing(self):
        snap = tc.capture_git_snapshot(self.repo, _git="nonexistent-git-xyz")
        self.assertFalse(snap["git"])
        self.assertEqual(snap["error"], "git_missing")

    def test_snapshot_git_cmd_failed(self):
        # 注入一个"git"假装在仓库外:rev-parse --is-inside-work-tree 失败 → 降级
        fake = os.path.join(self.tmp, "fake-git")
        with open(fake, "w", encoding="utf-8") as f:
            f.write("#!/bin/sh\ncase \" $* \" in *\" rev-parse \"*) exit 128;; esac\nexit 0\n")
        os.chmod(fake, 0o755)
        snap = tc.capture_git_snapshot(self.repo, _git=fake)
        self.assertFalse(snap["git"])
        self.assertEqual(snap["error"], "not_a_git_repo")

    def test_snapshot_clean_repo(self):
        snap = tc.capture_git_snapshot(self.clean_repo)  # E11 干净仓 = 合法空快照
        self.assertTrue(snap["git"])
        self.assertEqual(snap["tracked_modified"], {})
        self.assertEqual(snap["untracked"], [])
        self.assertIsNotNone(snap)

    def test_snapshot_deleted_tracked_null(self):
        snap = tc.capture_git_snapshot(self.repo)
        self.assertIn("c.txt", snap["tracked_modified"])
        self.assertIsNone(snap["tracked_modified"]["c.txt"])

    def test_snapshot_workdir_fallback(self):
        # E10:workdir=None → 回退 os.getcwd()(chdir 到干净仓,不碰真实业务仓)
        os.chdir(self.clean_repo)
        snap = tc.capture_git_snapshot(None)
        self.assertIsNotNone(snap)
        self.assertTrue(snap["git"])
        self.assertEqual(snap["head"], self._head(self.clean_repo))

    def test_snapshot_unicode_paths(self):
        # 非 ASCII(UTF-8)路径不被 quotePath 转义 → hash 正确(跨平台)
        repo = os.path.join(self.tmp, "uni")
        os.makedirs(repo)
        with open(os.path.join(repo, "测试.txt"), "w", encoding="utf-8") as f:
            f.write("x\n")
        self._git(repo, "init", "-q")
        self._git(repo, "add", ".")
        self._commit(repo)
        with open(os.path.join(repo, "测试.txt"), "w", encoding="utf-8") as f:
            f.write("y\n")  # 制造 M:中文文件名
        snap = tc.capture_git_snapshot(repo)  # 不抛
        self.assertTrue(snap["git"])
        self.assertIn("测试.txt", snap["tracked_modified"])
        self.assertEqual(snap["tracked_modified"]["测试.txt"], self._hash(repo, "测试.txt"))

    @unittest.skipUnless(sys.platform.startswith("linux"),
                         "非 UTF-8 字节文件名依赖 POSIX byte passthrough(Linux)")
    def test_snapshot_non_utf8_paths_latin1(self):
        # ocr7-L4:非 UTF-8 文件名(latin-é 的 0xE9 原始字节)仅在 Linux/POSIX
        # byte passthrough 下可靠,其余平台文件系统可能拒绝/改写 → 条件跳过
        # (此前无条件依赖 ext4 字节透传,跨平台脆弱);_git_run errors="replace"
        # 解码 → U+FFFD 替换符,丢条目/表示变化即失败
        repo = os.path.join(self.tmp, "uni2")
        os.makedirs(repo)
        with open(os.path.join(repo, "a.txt"), "w", encoding="utf-8") as f:
            f.write("x\n")
        self._git(repo, "init", "-q")
        self._git(repo, "add", ".")
        self._commit(repo)
        with open(os.path.join(os.fsencode(repo), b"latin-\xe9.txt"), "wb") as f:
            f.write(b"z\n")  # untracked:非 UTF-8 字节文件名(此前会 UnicodeDecodeError)
        snap = tc.capture_git_snapshot(repo)  # 不抛
        self.assertTrue(snap["git"])
        # ocr F3:钉死非 UTF-8 文件名在 untracked 的表示——-c core.quotePath=false
        # 输出原始字节 + errors="replace" 解码,latin-\xe9 → 一个 U+FFFD 替换符
        self.assertIn("latin-\ufffd.txt", snap["untracked"])

    def test_snapshot_multi_modified_batched_hash(self):
        """ocr7-M2:多个 M 文件 → 批量 hash-object(--stdin-paths)一次 spawn,
        内容与逐文件 git hash-object 一致;已删文件批量失败 → 逐文件回退仍 null。"""
        repo = os.path.join(self.tmp, "multi")
        os.makedirs(repo)
        for n in ("f1.txt", "f2.txt", "f3.txt", "f4.txt", "gone.txt"):
            with open(os.path.join(repo, n), "w", encoding="utf-8") as f:
                f.write(n + "\n")
        self._git(repo, "init", "-q")
        self._git(repo, "add", ".")
        self._commit(repo)
        for n in ("f1.txt", "f2.txt", "f3.txt", "f4.txt"):
            with open(os.path.join(repo, n), "w", encoding="utf-8") as f:
                f.write(n + " changed\n")
        os.remove(os.path.join(repo, "gone.txt"))      # D:触发批量失败 → 逐文件回退

        hash_calls = []
        orig_run = subprocess.run

        def _counting_run(*a, **kw):
            argv = a[0] if a else []
            if "hash-object" in argv:
                hash_calls.append(argv)
            return orig_run(*a, **kw)

        with mock.patch.object(tc.subprocess, "run", side_effect=_counting_run):
            snap = tc.capture_git_snapshot(repo)
        self.assertTrue(snap["git"])
        for n in ("f1.txt", "f2.txt", "f3.txt", "f4.txt"):
            self.assertEqual(snap["tracked_modified"][n], self._hash(repo, n))
        # 已删文件 → null(E4 语义不变)
        self.assertIsNone(snap["tracked_modified"]["gone.txt"])
        # 批量路径:hash-object 只 spawn 一次(--stdin-paths);含已删 → 批量失败
        # 回退逐文件(≥1 次),但绝不为每个文件独立批量成功路径外的全量逐文件
        self.assertGreaterEqual(len(hash_calls), 1)
        first = hash_calls[0]
        self.assertIn("--stdin-paths", first)


# ---------------------------------------------------------------------------
# write/load roundtrip + 降级(E5-E9)
# ---------------------------------------------------------------------------

class SnapshotStoreTests(SnapshotIsoBase):
    """write/load 原子性 + 缺失/损坏/形状非法/OSError/DENY。"""

    def test_write_load_roundtrip(self):
        tid = "t-000000000001"
        snap = self._valid_snap(tid)
        self.assertTrue(tc.write_snapshot(tid, snap))
        loaded = tc.load_snapshot(tid)
        self.assertEqual(loaded, snap)
        self.assertEqual(loaded["head"], snap["head"])
        self.assertEqual(loaded["tracked_modified"]["b.txt"], snap["tracked_modified"]["b.txt"])
        self.assertEqual(loaded["untracked"], ["u1.txt"])
        # 原子性:无 .tmp. 残留
        for name in os.listdir(tc.snapshots_dir()):
            self.assertNotIn(".tmp.", name)

    def test_load_missing_none(self):
        self.assertIsNone(tc.load_snapshot("t-0000000000aa"))  # E5

    def test_load_corrupt_none(self):
        tid = "t-000000000001"
        path = tc.snapshot_path(tid)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write("{not json!!")
        self.assertIsNone(tc.load_snapshot(tid))  # E6 不抛

    def test_load_wrong_shape_none(self):
        tid = "t-000000000001"
        bads = [
            {"schema_version": 99, "git": True, "head": "x",
             "tracked_modified": {}, "untracked": []},          # E7 schema 错
            {"schema_version": 1, "git": True, "head": "",
             "tracked_modified": {}, "untracked": []},          # E7 head 空
            {"schema_version": 1, "git": True, "head": "x",
             "tracked_modified": [], "untracked": []},          # E7 tracked 非 dict
            {"schema_version": 1, "git": True, "head": "x",
             "tracked_modified": {}, "untracked": "oops"},      # E7 untracked 非 list
            {"schema_version": 1, "git": "yes", "head": "x"},   # E7 git 非 bool
            [1, 2, 3],                                           # E7 顶层非 dict
        ]
        for i, bad in enumerate(bads):
            tc.write_snapshot(tid, bad)  # 形状非法但 JSON 可序列化
            self.assertIsNone(tc.load_snapshot(tid), f"case {i} 应降级 None")
            os.remove(tc.snapshot_path(tid))

    def test_write_snapshot_oserror(self):
        tid = "t-000000000001"
        with mock.patch.object(os, "replace", side_effect=OSError("disk full")):
            ok = tc.write_snapshot(tid, self._valid_snap(tid))  # E8 不抛
        self.assertFalse(ok)
        self.assertFalse(os.path.exists(tc.snapshot_path(tid)))
        # 无 tmp 残留
        if os.path.isdir(tc.snapshots_dir()):
            self.assertEqual([n for n in os.listdir(tc.snapshots_dir()) if ".tmp." in n], [])

    def test_write_snapshot_deny(self):
        tid = "t-000000000001"
        snap = {"schema_version": tc.SNAPSHOT_SCHEMA_VERSION, "task_id": tid,
                "workdir": self.repo, "git": False, "error": f"boom {_FAKE_SECRET}"}
        ok = tc.write_snapshot(tid, snap)  # E9 拒写不抛
        self.assertFalse(ok)
        self.assertFalse(os.path.exists(tc.snapshot_path(tid)))

    def test_write_snapshot_git_false_roundtrip(self):
        tid = "t-000000000001"
        snap = {"schema_version": tc.SNAPSHOT_SCHEMA_VERSION, "task_id": tid,
                "captured_at": "2026-08-24T10:00:00.000+00:00",
                "workdir": self.tmp, "git": False, "error": "not_a_git_repo"}
        self.assertTrue(tc.write_snapshot(tid, snap))
        self.assertEqual(tc.load_snapshot(tid), snap)  # git:false 快照可正常读取


# ---------------------------------------------------------------------------
# 三入口挂钩(dispatch/run/pump)+ 原子化顺序(E12)
# ---------------------------------------------------------------------------

class PromoteHooksTests(SnapshotIsoBase):
    """dispatch/run/pump 提升路径均生成快照,且先于 registry running。"""

    def _noop_spawn(self):
        return mock.patch.object(tc, "spawn_runner", return_value=None)

    def test_dispatch_writes_snapshot_before_running(self):
        tid = self._seed_task(state="queued", kind="execute")
        calls = []
        orig_write, orig_save = tc.write_snapshot, tc.save_registry_atomic

        def _wr(t, snap):
            calls.append("snapshot")
            return orig_write(t, snap)

        def _sv(reg):
            calls.append("registry")
            return orig_save(reg)

        with mock.patch.object(tc, "write_snapshot", side_effect=_wr), \
                mock.patch.object(tc, "save_registry_atomic", side_effect=_sv), \
                self._noop_spawn():
            promoted = tc.dispatch(_cfg())
        self.assertEqual(promoted, [tid])
        # E12 不变式:快照写盘先于 registry 落盘
        self.assertEqual(calls, ["snapshot", "registry"])
        # registry 落盘 running 且快照文件存在
        t = tc.load_registry()["tasks"][tid]
        self.assertEqual(t["state"], "running")
        self.assertTrue(os.path.isfile(tc.snapshot_path(tid)))
        snap = tc.load_snapshot(tid)
        self.assertTrue(snap["git"])
        self.assertEqual(snap["workdir"], self.repo)

    def test_dispatch_skips_non_execute(self):
        tid = self._seed_task(state="queued", kind="design")
        with self._noop_spawn():
            promoted = tc.dispatch(_cfg())
        self.assertEqual(promoted, [tid])
        self.assertFalse(os.path.exists(tc.snapshot_path(tid)))  # D5:仅 execute 拍

    def test_run_writes_snapshot(self):
        tid = self._seed_task(state="queued", kind="execute")
        with self._noop_spawn():
            rc = tc.cmd_run([tid], _cfg())
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.isfile(tc.snapshot_path(tid)))
        self.assertEqual(tc.load_registry()["tasks"][tid]["state"], "running")

    def test_pump_writes_snapshot(self):
        tid = self._seed_task(state="scheduled", kind="execute",
                              scheduled_at="2020-01-01T00:00:00+00:00")
        with mock.patch.object(tc, "run_stale_gate", return_value=[]), \
                self._noop_spawn():
            rc = tc.cmd_pump([], _cfg())
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.isfile(tc.snapshot_path(tid)))
        self.assertEqual(tc.load_registry()["tasks"][tid]["state"], "running")


# ---------------------------------------------------------------------------
# 生命周期(prune 连带清理,E13)+ CLI(cmd_snapshot)
# ---------------------------------------------------------------------------

class SnapshotLifecycleTests(SnapshotIsoBase):
    """prune/auto_prune 清理快照;cmd_snapshot 读取接口。"""

    def test_prune_removes_snapshot(self):
        tid = "t-000000000001"
        self._seed_task(tid=tid, state="done", finished_at="2020-01-01T00:00:00+00:00")
        tc.write_snapshot(tid, self._valid_snap(tid))
        os.makedirs(tc.logs_dir(), exist_ok=True)
        with open(tc.log_path(tid), "w", encoding="utf-8") as f:
            f.write("x")
        removed = tc.prune_tasks(tc.load_registry())
        self.assertIn(tid, removed)
        self.assertFalse(os.path.exists(tc.snapshot_path(tid)))
        self.assertFalse(os.path.exists(tc.log_path(tid)))
        # 幂等:不存在快照再删不抛(E13)
        tc._remove_snapshot_best_effort(tid)

    def test_auto_prune_removes_snapshot(self):
        tid = "t-000000000001"
        self._seed_task(tid=tid, state="done", finished_at="2020-01-01T00:00:00+00:00")
        tc.write_snapshot(tid, self._valid_snap(tid))
        cfg = _cfg(prune_done_days=0)
        removed = tc.auto_prune(tc.load_registry(), cfg)
        self.assertIn(tid, removed)
        self.assertFalse(os.path.exists(tc.snapshot_path(tid)))

    def test_cmd_snapshot_cli(self):
        tid = "t-000000000001"
        cfg = _cfg()
        # 缺失 → exit2 snapshot_missing(E5)
        self.assertEqual(tc.cmd_snapshot([tid], cfg), 2)
        # 损坏 → exit2 snapshot_corrupt(E6)
        tc.write_snapshot(tid, self._valid_snap(tid))
        with open(tc.snapshot_path(tid), "w", encoding="utf-8") as f:
            f.write("{oops")
        self.assertEqual(tc.cmd_snapshot([tid], cfg), 2)
        # 非法 id → exit2
        self.assertEqual(tc.cmd_snapshot(["bad-id"], cfg), 2)
        # --json → status ok + head
        tc.write_snapshot(tid, self._valid_snap(tid))
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_snapshot([tid, "--json"], cfg)
        self.assertEqual(rc, 0)
        out = json.loads(buf.getvalue())
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["id"], tid)
        self.assertTrue(out["snapshot"]["git"])
        self.assertEqual(out["snapshot"]["head"], self._head(self.repo))
        # 非 json → 文本摘要含 head
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = tc.cmd_snapshot([tid], cfg)
        self.assertEqual(rc, 0)
        self.assertIn(self._head(self.repo), buf.getvalue())


if __name__ == "__main__":
    unittest.main()
