import subprocess
from types import SimpleNamespace

import pytest

from bench.harbor_fanout import prepare_docker_task, validate_result


def result(*, rewards: list[float], errors: int = 0, return_code: int = 0):
    return SimpleNamespace(
        provider="smol-branch",
        rewards=rewards,
        errors=errors,
        return_code=return_code,
    )


def test_oracle_reward_must_meet_minimum() -> None:
    with pytest.raises(RuntimeError, match=r"observed=0\.000\.\.1\.000"):
        validate_result(
            result(rewards=[1.0, 0.0]),
            attempts=2,
            install_only=False,
            minimum_reward=1.0,
        )


def test_install_only_does_not_require_rewards() -> None:
    validate_result(
        result(rewards=[]),
        attempts=2,
        install_only=True,
        minimum_reward=1.0,
    )


def test_matched_docker_preparation_copies_task_and_rewrites_image(
    tmp_path, monkeypatch
) -> None:
    task = tmp_path / "real-task"
    task.mkdir()
    original = '[environment]\ndocker_image = "example/base:1"\n'
    (task / "task.toml").write_text(original)
    (task / "instruction.md").write_text("do the work\n")
    script = tmp_path / "prepare.sh"
    script.write_text("set -e\necho warmed > /warm\n")
    observed = {}

    monkeypatch.setattr("bench.harbor_fanout.docker_preflight", lambda: None)

    def fake_run(command, **kwargs):
        if command[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(command, 1)
        observed["dockerfile"] = kwargs["input"]
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr("bench.harbor_fanout.subprocess.run", fake_run)

    prepared, _, built, image = prepare_docker_task(task, script, tmp_path / "cache")

    assert built
    assert image in (prepared / "task.toml").read_text()
    assert "example/base:1" in observed["dockerfile"]
    assert "echo warmed" in observed["dockerfile"]
    assert (task / "task.toml").read_text() == original
