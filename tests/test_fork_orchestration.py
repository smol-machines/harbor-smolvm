"""Golden+fork orchestration tests (no real VMs).

Validates the concurrency-safe golden-Machine management the fork fast path
relies on, by mocking the smolvm SDK ``Machine`` and recording calls. Fork is
opt-in (``SMOLVM_HARBOR_FORK=on``); these tests force it on and stub the SDK, so
the host's real fork capability is irrelevant.

  1. Under N concurrent start()s for one image, the golden is created exactly
     once (forkable=True) and forked N times; teardown deletes the clones and
     the golden after the last ref.
  2. When the forkable create fails, the process latches to the fallback path
     (a full per-trial machine, no fork) for every trial.

Run:  <venv-with-smolmachines>/bin/python tests/test_fork_orchestration.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from harbor_smolvm import environment as se


class _Cfg:
    def __init__(self, image):
        self.docker_image = image


class _Res:
    def __init__(self, code=0, out="", err=""):
        self.exit_code, self.stdout, self.stderr = code, out, err


class FakeMachine:
    """Records the SDK calls the backend makes; forks return more FakeMachines."""

    events: list = []
    fail_forkable = False

    def __init__(self, name):
        self.name = name

    @classmethod
    def create(cls, config, conn=None):
        cls.events.append(("create", config.name, config.forkable))
        if cls.fail_forkable and config.forkable:
            raise RuntimeError("krun_start_enter returned: -22")
        return cls(config.name)

    def fork(self, name, ports=None):
        FakeMachine.events.append(("fork", self.name, name))
        return FakeMachine(name)

    def exec(self, argv, opts=None):
        return _Res()

    def write_file(self, path, data, mode=None):
        pass

    def stop(self):
        FakeMachine.events.append(("stop", self.name))

    def delete(self):
        FakeMachine.events.append(("delete", self.name))


def _make_env(name, image="alpine:latest"):
    env = object.__new__(se.SmolvmEnvironment)
    env._cloud = False
    env._name = name
    env._machine = None
    env._forked = False
    env._golden_image = None
    env.task_env_config = _Cfg(image)
    return env


def _reset(fail_forkable=False):
    se.Machine = FakeMachine
    se.MachineConfig = _FakeConfig
    se.ResourceSpec = lambda **k: None
    se._goldens = {}
    se._fork_unavailable = False
    se._KEEP_GOLDEN = False
    se._FORK_MODE = "on"  # force fork attempts regardless of host
    FakeMachine.events = []
    FakeMachine.fail_forkable = fail_forkable


class _FakeConfig:
    def __init__(self, name=None, image=None, resources=None, persistent=False,
                 forkable=False, **kw):
        self.name = name
        self.image = image
        self.forkable = forkable


def _count(kind):
    return sum(1 for e in FakeMachine.events if e[0] == kind)


async def test_golden_once_and_forks():
    _reset()
    N = 5
    envs = [_make_env(f"hb-trial-{i}") for i in range(N)]
    await asyncio.gather(*(e.start(force_build=False) for e in envs))

    golden = envs[0]._golden_name("alpine:latest")
    creates = [e for e in FakeMachine.events if e[0] == "create"]
    assert creates == [("create", golden, True)], f"golden not created once: {creates}"
    assert _count("fork") == N, f"expected {N} forks, got {_count('fork')}"
    assert all(e._forked for e in envs)
    assert se._goldens["alpine:latest"].refcount == N
    print(f"  [ok] {N} concurrent starts -> golden created once (forkable), {N} forks")

    await asyncio.gather(*(e.stop(delete=True) for e in envs))
    deletes = {e[1] for e in FakeMachine.events if e[0] == "delete"}
    assert golden in deletes, "golden not deleted after last ref"
    for e in envs:
        assert e._name in deletes, f"clone {e._name} not deleted"
    assert "alpine:latest" not in se._goldens
    print(f"  [ok] teardown deleted {N} clones + the golden")


async def test_fallback_when_forkable_fails():
    _reset(fail_forkable=True)
    N = 3
    envs = [_make_env(f"hb-fb-{i}") for i in range(N)]
    await asyncio.gather(*(e.start(force_build=False) for e in envs))

    assert _count("fork") == 0, "must not fork when the forkable create fails"
    assert se._fork_unavailable is True, "fork-unavailable should latch"
    assert not any(e._forked for e in envs)
    # every trial fell back to a full (non-forkable) create of its own machine
    fallbacks = {e[1] for e in FakeMachine.events if e[0] == "create" and e[2] is False}
    for e in envs:
        assert e._name in fallbacks, f"no fallback create for {e._name}"
    print(f"  [ok] forkable failed -> all {N} trials fell back to full create")


async def main():
    print("test_golden_once_and_forks:")
    await test_golden_once_and_forks()
    print("test_fallback_when_forkable_fails:")
    await test_fallback_when_forkable_fails()
    print("\nALL FORK-ORCHESTRATION TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
