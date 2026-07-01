"""Phase-2 golden+fork orchestration tests (no real VMs).

Validates the concurrency-safe golden-VM management that a forking host (Linux/
KVM, or the cloud control plane) relies on, by stubbing `_smol` to record the
command stream. This is the part a macOS/HVF box can't exercise with real VMs
(it can't memfd-back a forkable golden), so we assert the orchestration directly:

  1. Under N concurrent start()s for one image, the golden is built exactly once
     and forked N times; teardown releases it and removes it after the last ref.
  2. When a forkable boot fails, the process latches to the fallback path
     (full per-trial create+start, no fork) for every trial.

Run with harbor's interpreter:
  .../tools/harbor/bin/python integrations/harbor/tests/test_fork_orchestration.py
"""

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from harbor_smolvm import environment as se


class _Cfg:
    def __init__(self, image):
        self.docker_image = image


class _Res:
    def __init__(self, code=0, out="", err=""):
        self.return_code, self.stdout, self.stderr = code, out, err


def _make_env(name, calls, fail_forkable=False):
    """A backend instance with a recording `_smol`, bypassing harbor's __init__."""
    env = object.__new__(se.SmolvmEnvironment)
    env._cloud = False
    env._name = name
    env._cloud_id = None
    env._forked = False
    env._golden_image = None
    env.task_env_config = _Cfg("alpine:latest")

    async def fake_smol(args, *, check=True, timeout_sec=None):
        calls.append(args)
        # Let sibling coroutines interleave so the golden lock is actually tested.
        await asyncio.sleep(0.005)
        if fail_forkable and args[:1] == ["start"] and "--forkable" in args:
            raise RuntimeError("krun_start_enter returned: -22")
        return _Res()

    env._smol = fake_smol
    return env


def _reset():
    se._goldens = {}
    se._fork_unavailable = False
    se._KEEP_GOLDEN = False
    # Force fork attempts regardless of host platform (auto is Linux-gated, and
    # these tests stub `_smol`, so the real host's fork capability is irrelevant).
    se._FORK_MODE = "on"


def _count(calls, *prefix):
    return sum(1 for c in calls if c[: len(prefix)] == list(prefix))


async def test_golden_once_and_forks():
    _reset()
    calls = []
    N = 5
    envs = [_make_env(f"hb-trial-{i}", calls) for i in range(N)]
    await asyncio.gather(*(e.start(force_build=False) for e in envs))

    golden = envs[0]._golden_name("alpine:latest")
    assert _count(calls, "create", "-n", golden) == 1, "golden created more than once"
    assert _count(calls, "start", "-n", golden, "--forkable") == 1, "golden started >1"
    assert _count(calls, "fork", "--golden", golden) == N, f"expected {N} forks"
    assert all(e._forked for e in envs), "all trials should be forked clones"
    assert se._goldens["alpine:latest"].refcount == N
    print(f"  [ok] {N} concurrent starts -> golden built once, {N} CoW forks")

    # Teardown: each clone removed; golden removed after the last ref drops.
    await asyncio.gather(*(e.stop(delete=True) for e in envs))
    assert _count(calls, "rm", "-n", golden) == 1, "golden not cleaned up once"
    for e in envs:
        assert _count(calls, "rm", "-n", e._name) == 1, "clone not removed"
    assert "alpine:latest" not in se._goldens, "golden state not released"
    print(f"  [ok] teardown removed {N} clones + the golden exactly once")


async def test_fallback_when_not_forkable():
    _reset()
    calls = []
    N = 3
    envs = [_make_env(f"hb-fb-{i}", calls, fail_forkable=True) for i in range(N)]
    await asyncio.gather(*(e.start(force_build=False) for e in envs))

    assert _count(calls, "fork") == 0, "must not fork when forkable boot fails"
    assert se._fork_unavailable is True, "fork-unavailable should latch on"
    assert not any(e._forked for e in envs), "no trial should be marked forked"
    # Every trial falls back to a full per-trial machine + dir provisioning.
    for e in envs:
        assert _count(calls, "create", "-n", e._name) == 1, "fallback create missing"
        assert _count(calls, "start", "-n", e._name) == 1, "fallback start missing"
    print(f"  [ok] forkable boot failed -> all {N} trials fell back to full create")


async def main():
    print("test_golden_once_and_forks:")
    await test_golden_once_and_forks()
    print("test_fallback_when_not_forkable:")
    await test_fallback_when_not_forkable()
    print("\nALL FORK-ORCHESTRATION TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
