#!/usr/bin/env python3
"""Run reproducible Harbor fan-out comparisons on public benchmark tasks."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time
import tomllib
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any


DEFAULT_DATASET = "terminal-bench-sample@2.0"
DEFAULT_TASK = "regex-log"
PROVIDERS = ("smol-branch", "smol-cold", "docker")


def package_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def mem_available_bytes() -> int | None:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except (FileNotFoundError, OSError, ValueError):
        return None
    return None


class MemorySampler:
    def __init__(self, interval_s: float = 0.1) -> None:
        self.interval_s = interval_s
        self.samples: list[int] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            value = mem_available_bytes()
            if value is not None:
                self.samples.append(value)

    def __enter__(self) -> "MemorySampler":
        initial = mem_available_bytes()
        if initial is not None:
            self.samples.append(initial)
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join(timeout=max(1.0, self.interval_s * 4))
        final = mem_available_bytes()
        if final is not None:
            self.samples.append(final)

    @property
    def peak_delta_bytes(self) -> int | None:
        if not self.samples:
            return None
        return max(self.samples[0] - min(self.samples), 0)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def elapsed(interval: dict[str, str] | None) -> float | None:
    if (
        not interval
        or not interval.get("started_at")
        or not interval.get("finished_at")
    ):
        return None
    return (
        parse_time(interval["finished_at"]) - parse_time(interval["started_at"])
    ).total_seconds()


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def distribution(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    return {
        "min": min(values),
        "p50": statistics.median(values),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values),
    }


@dataclass
class RunResult:
    repetition: int
    provider: str
    dataset: str
    task: str
    attempts: int
    concurrency: int
    agent: str
    install_only: bool
    checkpoint_mode: str
    checkpoint_prepare_seconds: float | None
    return_code: int
    wall_seconds: float
    harbor_seconds: float | None
    completed: int
    errors: int
    rewards: list[float]
    environment_setup_seconds: dict[str, float] | None
    agent_execution_seconds: dict[str, float] | None
    verifier_seconds: dict[str, float] | None
    approximate_peak_host_memory_bytes: int | None
    result_path: str
    host: dict[str, Any]
    software: dict[str, str | None]
    provider_prepare_seconds: float | None = None
    provider_prepare_built: bool | None = None
    checkpoint_prepare_phases: dict[str, float] | None = None
    correctness_error: str | None = None


def ensure_dataset(harbor: str, dataset: str, cache_dir: Path) -> Path:
    name = dataset.split("@", 1)[0]
    path = cache_dir / name
    if path.is_dir() and any(path.iterdir()):
        return path
    cache_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [harbor, "dataset", "download", dataset, "--output-dir", str(cache_dir)],
        check=True,
    )
    if not path.is_dir():
        raise RuntimeError(f"Harbor downloaded {dataset}, but {path} was not created")
    return path


def provider_args(provider: str, checkpoint_spec: dict[str, Any] | None) -> list[str]:
    if provider == "smol-branch":
        args = ["--env", "smol.harbor:SmolEnvironment"]
        if checkpoint_spec is not None:
            args.extend(
                [
                    "--ek",
                    "checkpoints=" + json.dumps(checkpoint_spec, separators=(",", ":")),
                ]
            )
        return args
    if provider == "smol-cold":
        return [
            "--env",
            "smol.harbor:SmolEnvironment",
            "--ek",
            "auto_checkpoint=false",
        ]
    if provider == "docker":
        return ["--env", "docker"]
    raise ValueError(f"unknown provider {provider!r}")


def docker_preflight() -> None:
    for command in (["docker", "info"], ["docker", "compose", "version"]):
        result = subprocess.run(
            command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        if result.returncode:
            raise RuntimeError(
                "Docker baseline requested, but `docker info` or "
                "`docker compose version` failed"
            )


def prepare_docker_task(
    task_path: Path,
    prepare_script: Path,
    cache_dir: Path,
) -> tuple[Path, float, bool, str]:
    """Build a Docker task with the same preparation applied to the Smol golden."""
    docker_preflight()
    task = tomllib.loads((task_path / "task.toml").read_text())
    image = (task.get("environment") or {}).get("docker_image")
    if not isinstance(image, str) or not image:
        raise RuntimeError("matched Docker preparation requires docker_image")

    script = prepare_script.read_text()
    digest = hashlib.sha256()
    digest.update(image.encode())
    digest.update(script.encode())
    for path in sorted(path for path in task_path.rglob("*") if path.is_file()):
        digest.update(str(path.relative_to(task_path)).encode())
        digest.update(path.read_bytes())
    fingerprint = digest.hexdigest()[:12]
    task_slug = re.sub(r"[^a-z0-9]+", "-", task_path.name.lower()).strip("-")
    prepared_image = f"smol-bench/harbor-{task_slug}:{fingerprint}"

    inspect = subprocess.run(
        ["docker", "image", "inspect", prepared_image],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    built = inspect.returncode != 0
    build_seconds = 0.0
    if built:
        dockerfile = f"FROM {image}\nRUN {json.dumps(['/bin/bash', '-lc', script])}\n"
        started = time.perf_counter()
        result = subprocess.run(
            ["docker", "build", "--tag", prepared_image, "-"],
            input=dockerfile,
            text=True,
            capture_output=True,
            timeout=1200,
        )
        build_seconds = time.perf_counter() - started
        if result.returncode != 0:
            raise RuntimeError(
                f"matched Docker image failed to build ({result.returncode})\n"
                f"stdout:\n{result.stdout[-4000:]}\n"
                f"stderr:\n{result.stderr[-4000:]}"
            )

    prepared_task = cache_dir / "prepared-tasks" / fingerprint
    if not (prepared_task / "task.toml").is_file():
        prepared_task.parent.mkdir(parents=True, exist_ok=True)
        temporary = prepared_task.with_name(f".{fingerprint}-{uuid.uuid4().hex[:6]}")
        shutil.copytree(task_path, temporary)
        definition = (temporary / "task.toml").read_text()
        pattern = r'(?m)^(\s*docker_image\s*=\s*)(["\']).*?\2\s*$'
        definition, count = re.subn(
            pattern,
            lambda match: match.group(1) + json.dumps(prepared_image),
            definition,
            count=1,
        )
        if count != 1:
            shutil.rmtree(temporary)
            raise RuntimeError("could not replace docker_image in copied task.toml")
        (temporary / "task.toml").write_text(definition)
        try:
            temporary.rename(prepared_task)
        except FileExistsError:
            shutil.rmtree(temporary)
    return prepared_task, build_seconds, built, prepared_image


def summarize_job(
    *,
    provider: str,
    repetition: int,
    dataset: str,
    task: str,
    attempts: int,
    concurrency: int,
    agent: str,
    install_only: bool,
    checkpoint_mode: str,
    checkpoint_prepare_seconds: float | None,
    return_code: int,
    wall_seconds: float,
    peak_memory: int | None,
    job_dir: Path,
    provider_prepare_seconds: float | None,
    provider_prepare_built: bool | None,
    checkpoint_prepare_phases: dict[str, float] | None,
) -> RunResult:
    result_path = job_dir / "result.json"
    if not result_path.is_file():
        raise RuntimeError(f"Harbor did not write {result_path}")
    job = json.loads(result_path.read_text())
    trial_results = []
    for path in sorted(job_dir.glob("*/result.json")):
        trial_results.append(json.loads(path.read_text()))

    rewards: list[float] = []
    setup: list[float] = []
    execution: list[float] = []
    verifier: list[float] = []
    for trial in trial_results:
        reward = (trial.get("verifier_result") or {}).get("rewards", {}).get("reward")
        if isinstance(reward, (int, float)):
            rewards.append(float(reward))
        for target, key in (
            (setup, "environment_setup"),
            (execution, "agent_execution"),
            (verifier, "verifier"),
        ):
            value = elapsed(trial.get(key))
            if value is not None:
                target.append(value)

    started = job.get("started_at")
    finished = job.get("finished_at")
    harbor_seconds = None
    if started and finished:
        harbor_seconds = (parse_time(finished) - parse_time(started)).total_seconds()

    stats = job.get("stats") or {}
    errors = int(stats.get("n_errored_trials", 0))
    return RunResult(
        repetition=repetition,
        provider=provider,
        dataset=dataset,
        task=task,
        attempts=attempts,
        concurrency=concurrency,
        agent=agent,
        install_only=install_only,
        checkpoint_mode=checkpoint_mode,
        checkpoint_prepare_seconds=checkpoint_prepare_seconds,
        return_code=return_code,
        wall_seconds=wall_seconds,
        harbor_seconds=harbor_seconds,
        completed=max(len(trial_results) - errors, 0),
        errors=errors,
        rewards=rewards,
        environment_setup_seconds=distribution(setup),
        agent_execution_seconds=distribution(execution),
        verifier_seconds=distribution(verifier),
        approximate_peak_host_memory_bytes=peak_memory,
        result_path=str(result_path),
        host={
            "platform": platform.platform(),
            "machine": platform.machine(),
            "logical_cpus": os.cpu_count(),
        },
        software={
            "python": platform.python_version(),
            "harbor": package_version("harbor"),
            "smolmachines": package_version("smolmachines"),
        },
        provider_prepare_seconds=provider_prepare_seconds,
        provider_prepare_built=provider_prepare_built,
        checkpoint_prepare_phases=checkpoint_prepare_phases,
    )


def correctness_error(
    result: RunResult,
    *,
    attempts: int,
    install_only: bool,
    minimum_reward: float | None,
) -> str | None:
    rewards_missing = not install_only and len(result.rewards) != attempts
    rewards_below_minimum = (
        not install_only
        and minimum_reward is not None
        and any(reward < minimum_reward for reward in result.rewards)
    )
    if result.return_code or result.errors or rewards_missing or rewards_below_minimum:
        observed = (
            f"{min(result.rewards):.3f}..{max(result.rewards):.3f}"
            if result.rewards
            else "none"
        )
        return (
            f"{result.provider} failed correctness gate: rc={result.return_code}, "
            f"errors={result.errors}, rewards={len(result.rewards)}/{attempts}, "
            f"observed={observed}, required_minimum={minimum_reward}"
        )
    return None


def validate_result(
    result: RunResult,
    *,
    attempts: int,
    install_only: bool,
    minimum_reward: float | None,
) -> None:
    failure = correctness_error(
        result,
        attempts=attempts,
        install_only=install_only,
        minimum_reward=minimum_reward,
    )
    result.correctness_error = failure
    if failure is not None:
        raise RuntimeError(failure)


def run_one(
    *,
    harbor: str,
    provider: str,
    dataset: str,
    task_path: Path,
    task: str,
    attempts: int,
    concurrency: int,
    agent: str,
    jobs_dir: Path,
    label: str,
    repetition: int,
    install_only: bool,
    checkpoint_spec: dict[str, Any] | None,
    checkpoint_prepare_seconds: float | None,
    provider_prepare_seconds: float | None,
    provider_prepare_built: bool | None,
    checkpoint_prepare_phases: dict[str, float] | None,
) -> RunResult:
    if provider == "docker":
        docker_preflight()
    job_name = f"{label}-{provider}"
    job_dir = jobs_dir / job_name
    if job_dir.exists():
        raise RuntimeError(f"refusing to overwrite existing job directory {job_dir}")
    command = [
        harbor,
        "run",
        "--path",
        str(task_path),
        "--agent",
        agent,
        "--jobs-dir",
        str(jobs_dir),
        "--job-name",
        job_name,
        "--n-attempts",
        str(attempts),
        "--n-concurrent",
        str(concurrency),
        *provider_args(
            provider, checkpoint_spec if provider == "smol-branch" else None
        ),
    ]
    if install_only:
        command.append("--install-only")
    print(f"\n[{provider}] {' '.join(command)}", flush=True)
    started = time.perf_counter()
    with MemorySampler() as memory:
        return_code = subprocess.run(command).returncode
    wall_seconds = time.perf_counter() - started
    result = summarize_job(
        provider=provider,
        repetition=repetition,
        dataset=dataset,
        task=task,
        attempts=attempts,
        concurrency=concurrency,
        agent=agent,
        install_only=install_only,
        checkpoint_mode="prepared" if checkpoint_spec is not None else "auto",
        checkpoint_prepare_seconds=(
            checkpoint_prepare_seconds if provider == "smol-branch" else None
        ),
        return_code=return_code,
        wall_seconds=wall_seconds,
        peak_memory=memory.peak_delta_bytes,
        job_dir=job_dir,
        provider_prepare_seconds=provider_prepare_seconds,
        provider_prepare_built=provider_prepare_built,
        checkpoint_prepare_phases=checkpoint_prepare_phases,
    )
    return result


def prepare_checkpoint(task_path: Path, label: str, prepare_script: Path | None):
    """Create one live checkpoint reused by every branched benchmark repetition."""
    from smol import ExecOptions, Machine, MachineConfig, ResourceSpec

    task = tomllib.loads((task_path / "task.toml").read_text())
    environment = task.get("environment") or {}
    image = environment.get("docker_image")
    if not isinstance(image, str) or not image:
        raise RuntimeError("prepared checkpoint mode requires environment.docker_image")
    network_mode = environment.get("network_mode", "public")
    if network_mode != "public":
        raise RuntimeError(
            "the benchmark checkpoint helper currently supports public network tasks"
        )
    name = f"harbor-bench-{label}-{uuid.uuid4().hex[:6]}"
    resources = ResourceSpec(
        cpus=environment.get("cpus"),
        memory_mb=environment.get("memory_mb"),
        storage_gb=(
            math.ceil(environment["storage_mb"] / 1024)
            if environment.get("storage_mb")
            else None
        ),
        network=True,
    )
    started = time.perf_counter()
    machine = Machine.create(
        MachineConfig(
            name=name,
            image=image,
            resources=resources,
            persistent=True,
            checkpoint=True,
            env=environment.get("env") or None,
            workdir=environment.get("workdir"),
        )
    )
    created = time.perf_counter()
    if prepare_script is not None:
        result = machine.exec(
            ["/bin/bash", "-lc", prepare_script.read_text()],
            ExecOptions(timeout=900, workdir="/"),
        )
        if result.exit_code != 0:
            machine.delete()
            raise RuntimeError(
                f"checkpoint preparation script failed ({result.exit_code})\n"
                f"stdout:\n{result.stdout[-4000:]}\n"
                f"stderr:\n{result.stderr[-4000:]}"
            )
    finished = time.perf_counter()
    prepare_seconds = finished - started
    phases = {
        "machine_create_seconds": created - started,
        "prepare_script_seconds": finished - created,
    }
    declared_resources = {
        key: environment[key]
        for key in ("cpus", "memory_mb", "storage_mb", "gpus")
        if environment.get(key) is not None
    }
    checkpoint: dict[str, Any] = {
        "machine": name,
        "network_mode": network_mode,
    }
    if declared_resources:
        checkpoint["resources"] = declared_resources
    spec = {image: checkpoint}
    return machine, spec, prepare_seconds, phases


def delete_checkpoint(machine: Any) -> None:
    try:
        machine.stop()
    except Exception:
        pass
    try:
        machine.delete()
    except Exception:
        pass


def write_summary(results: list[RunResult], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps([asdict(result) for result in results], indent=2) + "\n"
    )
    csv_path = output.with_suffix(".csv")
    with csv_path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            [
                "provider",
                "wall_seconds",
                "harbor_seconds",
                "setup_p50_seconds",
                "setup_p99_seconds",
                "errors",
                "mean_reward",
                "correctness_error",
                "approx_peak_host_memory_mib",
            ]
        )
        for result in results:
            setup = result.environment_setup_seconds or {}
            memory = result.approximate_peak_host_memory_bytes
            writer.writerow(
                [
                    result.provider,
                    f"{result.wall_seconds:.6f}",
                    f"{result.harbor_seconds:.6f}" if result.harbor_seconds else "",
                    setup.get("p50", ""),
                    setup.get("p99", ""),
                    result.errors,
                    statistics.mean(result.rewards) if result.rewards else "",
                    result.correctness_error or "",
                    f"{memory / 2**20:.3f}" if memory is not None else "",
                ]
            )
    print(f"\nWrote {output} and {csv_path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--task", default=DEFAULT_TASK)
    parser.add_argument("--task-label", help="result label for --task-path")
    parser.add_argument(
        "--task-path",
        type=Path,
        help="run one local task directory instead of downloading --dataset",
    )
    parser.add_argument("--attempts", type=int, default=16)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--agent", default="oracle")
    parser.add_argument(
        "--minimum-reward",
        type=float,
        help="fail if any trial scores below this value (defaults to 1 for oracle)",
    )
    parser.add_argument("--install-only", action="store_true")
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="run remaining providers and repetitions after a correctness failure",
    )
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument(
        "--checkpoint-mode", choices=("prepared", "auto"), default="prepared"
    )
    parser.add_argument(
        "--providers",
        nargs="+",
        choices=PROVIDERS,
        default=["smol-branch", "smol-cold"],
    )
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/harbor"))
    parser.add_argument("--jobs-dir", type=Path, default=Path("results/raw"))
    parser.add_argument(
        "--prepare-script",
        type=Path,
        help="run this script once inside the reusable checkpoint before fan-out",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.attempts < 1 or args.concurrency < 1 or args.repetitions < 1:
        parser.error("attempts, concurrency, and repetitions must be positive")
    if args.concurrency > args.attempts:
        parser.error("concurrency cannot exceed attempts")
    if args.prepare_script is not None and not args.prepare_script.is_file():
        parser.error(f"prepare script does not exist: {args.prepare_script}")
    if args.prepare_script is not None and args.checkpoint_mode != "prepared":
        parser.error("--prepare-script requires --checkpoint-mode=prepared")
    minimum_reward = args.minimum_reward
    if minimum_reward is None and args.agent == "oracle":
        minimum_reward = 1.0

    harbor = shutil.which("harbor")
    if not harbor:
        raise RuntimeError("Harbor is not installed; run `uv sync --extra dev`")
    if args.task_path is not None:
        task_path = args.task_path.resolve()
        task_name = args.task_label or task_path.name
    else:
        dataset_dir = ensure_dataset(harbor, args.dataset, args.cache_dir.resolve())
        task_path = dataset_dir / args.task
        task_name = args.task
    if not (task_path / "task.toml").is_file():
        raise RuntimeError(f"task.toml was not found under {task_path}")

    label = datetime.now().strftime("%Y%m%d-%H%M%S")
    output = args.output or Path("results") / f"{label}-{task_name}.json"
    jobs_dir = args.jobs_dir.resolve()
    jobs_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_machine = None
    checkpoint_spec = None
    checkpoint_prepare_seconds = None
    checkpoint_prepare_phases = None
    docker_task_path = task_path
    docker_prepare_seconds = None
    docker_prepare_built = None
    docker_prepared_image = None
    if (
        args.prepare_script is not None
        and "docker" in args.providers
        and not args.install_only
    ):
        print("\nPreparing an equivalently warmed Docker image...", flush=True)
        (
            docker_task_path,
            docker_prepare_seconds,
            docker_prepare_built,
            docker_prepared_image,
        ) = prepare_docker_task(
            task_path, args.prepare_script, args.cache_dir.resolve()
        )
        status = "built" if docker_prepare_built else "reused"
        print(
            f"Docker image {docker_prepared_image} {status} in "
            f"{docker_prepare_seconds:.3f}s.",
            flush=True,
        )
    if args.checkpoint_mode == "prepared" and "smol-branch" in args.providers:
        print("\nPreparing one reusable live checkpoint...", flush=True)
        (
            checkpoint_machine,
            checkpoint_spec,
            checkpoint_prepare_seconds,
            checkpoint_prepare_phases,
        ) = prepare_checkpoint(task_path, label, args.prepare_script)
        print(
            f"Checkpoint ready in {checkpoint_prepare_seconds:.3f}s; "
            "its cost is reported separately from branch readiness.",
            flush=True,
        )

    results: list[RunResult] = []
    failures: list[str] = []
    try:
        for repetition in range(1, args.repetitions + 1):
            providers = list(args.providers)
            if repetition % 2 == 0:
                providers.reverse()
            for provider in providers:
                result = run_one(
                    harbor=harbor,
                    provider=provider,
                    dataset=args.dataset,
                    task_path=(docker_task_path if provider == "docker" else task_path),
                    task=task_name,
                    attempts=args.attempts,
                    concurrency=args.concurrency,
                    agent=args.agent,
                    jobs_dir=jobs_dir,
                    label=f"{label}-r{repetition}",
                    repetition=repetition,
                    install_only=args.install_only,
                    checkpoint_spec=checkpoint_spec,
                    checkpoint_prepare_seconds=checkpoint_prepare_seconds,
                    provider_prepare_seconds=(
                        docker_prepare_seconds if provider == "docker" else None
                    ),
                    provider_prepare_built=(
                        docker_prepare_built if provider == "docker" else None
                    ),
                    checkpoint_prepare_phases=(
                        checkpoint_prepare_phases if provider == "smol-branch" else None
                    ),
                )
                results.append(result)
                try:
                    validate_result(
                        result,
                        attempts=args.attempts,
                        install_only=args.install_only,
                        minimum_reward=minimum_reward,
                    )
                except RuntimeError as error:
                    failures.append(str(error))
                    print(f"\nERROR: {error}", file=sys.stderr, flush=True)
                    if not args.keep_going:
                        raise
    finally:
        if checkpoint_machine is not None:
            delete_checkpoint(checkpoint_machine)
        if results:
            write_summary(results, output)
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130) from None
