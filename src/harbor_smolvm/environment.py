"""Harbor environment backend that runs task environments on smolvm / smolfleet.

Use it as a custom Harbor environment via its import path (after
``pip install harbor-smolvm smolmachines``):

    # local microVM (this host)
    harbor run ... --env harbor_smolvm:SmolvmEnvironment
    # hosted smolfleet cloud (set SMOL_CLOUD_TOKEN, or ~/.config/smolvm)
    harbor run ... --env harbor_smolvm:SmolvmCloudEnvironment

A Harbor task's environment is an OCI image; smolvm runs that image as a real
microVM (locally, cross-platform) or smolfleet runs it on the cloud. This backend
drives the smolvm **Python SDK** (``smolmachines``) — one ``Machine`` API over
both targets — so there is no local CLI to install and no subprocess plumbing:

    start(force_build) -> Machine.create(image=..., persistent=True[, forkable])
                          / golden.fork(name)             (CoW clone per trial)
    exec(command)      -> Machine.exec(...)               (persists root)
    upload/download    -> Machine.write_file / read_file  (shares exec's fs)
    stop(delete)       -> Machine.stop() / delete()

The SDK's ``exec`` persists writes to ``/`` across calls, and ``write_file`` /
``read_file`` share that same filesystem, so the agent-writes-then-verifier-reads
flow just works (no persistent-overlay or ``/workspace`` staging needed).

Env: SMOLVM_HARBOR_FORK=auto|on|off (CoW-fork clones per trial, default auto),
SMOLVM_HARBOR_KEEP_GOLDEN=1 (keep goldens warm across runs),
SMOL_CLOUD_TOKEN (smolfleet api key for the cloud subclass).
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import os
import shlex
import tarfile
import tempfile
import uuid
from pathlib import Path

from harbor.environments.base import BaseEnvironment, ExecResult
from harbor.environments.capabilities import (
    EnvironmentCapabilities,
    EnvironmentResourceCapabilities,
)
from harbor.models.task.config import EnvironmentConfig, TaskOS
from harbor.models.trial.paths import TrialPaths

from smol import ConnectOptions, ExecOptions, Machine, MachineConfig, ResourceSpec


# ---- Phase 2: golden-VM + CoW fork fast path -------------------------------
# Harbor's `--n-concurrent` runs trials as asyncio tasks in ONE process, so a
# module-level golden registry guarded by an asyncio.Lock is the right primitive.
# Per task image we build ONE forkable golden Machine, then `golden.fork()` a
# copy-on-write clone per trial. Falls back to a full per-trial machine where a
# forkable boot isn't available.
_FORK_MODE = os.environ.get("SMOLVM_HARBOR_FORK", "auto").strip().lower()
_KEEP_GOLDEN = os.environ.get("SMOLVM_HARBOR_KEEP_GOLDEN", "0").strip().lower() in (
    "1",
    "true",
    "on",
    "yes",
)


class _ForkUnavailable(Exception):
    """Raised when a forkable golden can't be brought up on this host."""


class _GoldenState:
    """Shared per-image golden Machine: built once, ref-counted by live clones."""

    __slots__ = ("machine", "name", "refcount", "ready")

    def __init__(self, name: str):
        self.machine: Machine | None = None
        self.name = name
        self.refcount = 0
        self.ready = False


_goldens: dict[str, _GoldenState] = {}
_golden_lock = asyncio.Lock()
_fork_unavailable = False


class _SmolEnvBase(BaseEnvironment):
    """Shared smolvm-SDK driver; subclasses set ``_cloud``."""

    _cloud: bool = False

    def __init__(
        self,
        environment_dir: Path,
        environment_name: str,
        session_id: str,
        trial_paths: TrialPaths,
        task_env_config: EnvironmentConfig,
        *args,
        **kwargs,
    ):
        super().__init__(
            environment_dir=environment_dir,
            environment_name=environment_name,
            session_id=session_id,
            trial_paths=trial_paths,
            task_env_config=task_env_config,
            **kwargs,
        )
        # smolvm machine name: stable + DNS-safe, unique per trial. Collapse runs
        # of non-alnum to a single hyphen and trim.
        import re

        slug = re.sub(r"[^a-z0-9]+", "-", session_id.lower()).strip("-")[:40]
        self._name = ("hb-" + slug).rstrip("-")
        self._machine: Machine | None = None
        self._forked: bool = False
        self._golden_image: str | None = None

    # ---- capabilities / metadata ----
    @staticmethod
    def type() -> str:
        return "smolvm"

    @classmethod
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
        return EnvironmentResourceCapabilities(cpu_limit=True, memory_limit=True)

    @property
    def capabilities(self) -> EnvironmentCapabilities:
        # File transfer is via the SDK, not a host bind-mount.
        return EnvironmentCapabilities(mounted=False)

    @property
    def os(self) -> TaskOS:
        return TaskOS.LINUX

    def _validate_definition(self):
        if not self.task_env_config.docker_image:
            raise RuntimeError(
                "SmolvmEnvironment requires task.toml [environment].docker_image "
                "(a prebuilt OCI image ref). Dockerfile-only tasks need a "
                "docker-build-and-import bridge (not yet wired)."
            )

    def _conn(self) -> ConnectOptions | None:
        if not self._cloud:
            return None  # local embedded engine
        # api_key from SMOL_CLOUD_TOKEN (else the SDK's own config discovery).
        token = os.environ.get("SMOL_CLOUD_TOKEN") or None
        return ConnectOptions(target="cloud", api_key=token)

    # Canonical guest dirs Harbor redirects script stdout into and uploads
    # solution/tests to. A bare OCI image has none, so the first
    # `(script) > /logs/agent/oracle.txt` redirect would fail before the script.
    _HARBOR_DIRS = (
        "/logs/agent",
        "/logs/verifier",
        "/logs/artifacts",
        "/tests",
        "/solution",
        "/harbor/skills",
    )
    # Agents emit bash-only syntax (`&>`, `set -o pipefail`), but debian's /bin/sh
    # is dash. Prefer bash when present; fall back to sh on minimal images.
    _SHELL_WRAPPER = (
        'if command -v bash >/dev/null 2>&1; then exec bash -c "$1"; '
        'else exec sh -c "$1"; fi'
    )

    # ---- low-level SDK helpers (sync SDK wrapped for the async interface) ----
    async def _mexec(
        self,
        machine: Machine,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
    ) -> ExecResult:
        opts = ExecOptions(env=env or None, workdir=cwd, timeout=timeout_sec)
        argv = ["/bin/sh", "-c", self._SHELL_WRAPPER, "sh", command]
        res = await asyncio.to_thread(machine.exec, argv, opts)
        return ExecResult(
            stdout=res.stdout, stderr=res.stderr, return_code=res.exit_code
        )

    async def _provision_dirs(self, machine: Machine) -> None:
        dirs = " ".join(self._HARBOR_DIRS)
        await self._mexec(machine, f"mkdir -p {dirs} && chmod 777 {dirs}")

    # ---- golden VM + CoW fork ----
    def _golden_name(self, image: str) -> str:
        return "hb-golden-" + hashlib.sha1(image.encode()).hexdigest()[:12]

    def _fork_enabled(self) -> bool:
        # Opt-in (SMOLVM_HARBOR_FORK=on). The CoW-fork fast path works via the
        # `smol` CLI, but the Python SDK's local fork currently times out waiting
        # for the clone agent, so `auto` does NOT attempt it (a failed attempt
        # just wastes a golden build before falling back). Cloud fork also needs
        # a forkable-deploy surface that isn't wired. Flip to `on` to try it.
        if self._cloud:
            return False
        return _FORK_MODE in ("1", "on", "true", "yes", "force")

    async def _ensure_golden(self, image: str) -> Machine:
        """Bring up (once) a running, forkable golden Machine for *image* and take
        a clone reference. Raises _ForkUnavailable if it can't boot forkable."""
        global _fork_unavailable
        async with _golden_lock:
            if _fork_unavailable:
                raise _ForkUnavailable("forkable boot already known-unavailable")
            st = _goldens.get(image)
            if st is None:
                st = _GoldenState(self._golden_name(image))
                _goldens[image] = st
            if not st.ready:
                cfg = MachineConfig(
                    name=st.name,
                    image=image,
                    resources=ResourceSpec(network=True),
                    persistent=True,
                    forkable=True,
                )
                try:
                    st.machine = await asyncio.to_thread(Machine.create, cfg, None)
                except Exception as e:
                    if st.machine is not None:
                        await asyncio.to_thread(_safe_delete, st.machine)
                    _goldens.pop(image, None)
                    _fork_unavailable = True
                    raise _ForkUnavailable(str(e)) from e
                st.ready = True
            st.refcount += 1
            return st.machine

    async def _release_golden(self, image: str) -> None:
        async with _golden_lock:
            st = _goldens.get(image)
            if st is None:
                return
            st.refcount -= 1
            if st.refcount <= 0 and not _KEEP_GOLDEN:
                _goldens.pop(image, None)
                if st.machine is not None:
                    await asyncio.to_thread(_safe_delete, st.machine)

    # ---- lifecycle ----
    async def start(self, force_build: bool) -> None:
        image = self.task_env_config.docker_image
        global _fork_unavailable

        if self._fork_enabled() and not _fork_unavailable:
            golden = None
            try:
                golden = await self._ensure_golden(image)
                clone = await asyncio.to_thread(golden.fork, self._name)
                self._machine = clone
                self._forked = True
                self._golden_image = image
                # A clone's exec overlay is keyed on its own name, so provision
                # its canonical dirs (cheap; the clone boots in ~0.1s).
                await self._provision_dirs(clone)
                return
            except _ForkUnavailable:
                _fork_unavailable = True
            except Exception:
                if golden is not None:
                    await self._release_golden(image)
                if _FORK_MODE in ("1", "on", "true", "yes", "force"):
                    raise
                _fork_unavailable = True

        # Fallback / cloud: a full per-trial machine.
        cfg = MachineConfig(
            name=self._name,
            image=image,
            resources=ResourceSpec(network=True),
            persistent=True,
        )
        self._machine = await asyncio.to_thread(Machine.create, cfg, self._conn())
        await self._provision_dirs(self._machine)

    async def stop(self, delete: bool) -> None:
        machine = self._machine
        if machine is None:
            return
        try:
            # Forked clones are cheap+ephemeral: always delete. Otherwise honor
            # `delete` (stop keeps the machine for inspection).
            if delete or self._forked:
                await asyncio.to_thread(_safe_delete, machine)
            else:
                await asyncio.to_thread(_safe_stop, machine)
        finally:
            if self._forked and self._golden_image is not None:
                await self._release_golden(self._golden_image)

    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult:
        return await self._mexec(
            self._machine, command, cwd=cwd, env=env, timeout_sec=timeout_sec
        )

    # ---- file transfer (SDK read_file/write_file share the exec filesystem) ----
    async def upload_file(self, source_path: Path | str, target_path: str) -> None:
        data = Path(source_path).read_bytes()
        # write_file doesn't create parents; ensure the target dir exists.
        await self._mexec(
            self._machine, f'mkdir -p "$(dirname {shlex.quote(target_path)})"'
        )
        await asyncio.to_thread(self._machine.write_file, target_path, data)

    async def download_file(self, source_path: str, target_path: Path | str) -> None:
        try:
            data = await asyncio.to_thread(self._machine.read_file, source_path)
        except Exception as e:
            raise FileNotFoundError(
                f"download_file: {source_path} not found in guest"
            ) from e
        Path(target_path).write_bytes(data)

    async def upload_dir(self, source_dir: Path | str, target_dir: str) -> None:
        # Tar locally, ship the bytes with write_file, untar in-guest.
        src = Path(source_dir)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for item in src.iterdir():
                tar.add(item, arcname=item.name)
        remote = f"/tmp/.hb-up-{uuid.uuid4().hex}.tar"
        await asyncio.to_thread(self._machine.write_file, remote, buf.getvalue())
        qd, qr = shlex.quote(target_dir), shlex.quote(remote)
        res = await self._mexec(
            self._machine, f"mkdir -p {qd} && tar -xf {qr} -C {qd} && rm -f {qr}"
        )
        if res.return_code != 0:
            raise RuntimeError(
                f"upload_dir -> {target_dir} failed: {res.stderr or res.stdout}"
            )

    async def download_dir(self, source_dir: str, target_dir: Path | str) -> None:
        # Tar in-guest, read the bytes out, untar locally. Tolerant of a missing
        # source dir (Harbor pulls optional log/artifact dirs).
        remote = f"/tmp/.hb-dn-{uuid.uuid4().hex}.tar"
        qs, qr = shlex.quote(source_dir), shlex.quote(remote)
        res = await self._mexec(
            self._machine, f"[ -d {qs} ] && tar -cf {qr} -C {qs} . || exit 7"
        )
        if res.return_code != 0:
            return
        try:
            data = await asyncio.to_thread(self._machine.read_file, remote)
        finally:
            await self._mexec(self._machine, f"rm -f {qr}")
        Path(target_dir).mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(data)) as tar:
            tar.extractall(target_dir)


def _safe_stop(machine: Machine) -> None:
    try:
        machine.stop()
    except Exception:
        pass


def _safe_delete(machine: Machine) -> None:
    try:
        machine.delete()
    except Exception:
        pass


class SmolvmEnvironment(_SmolEnvBase):
    """Run the task as a local smolvm microVM (cross-platform host)."""

    _cloud = False

    @staticmethod
    def type() -> str:
        return "smolvm"


class SmolvmCloudEnvironment(_SmolEnvBase):
    """Run the task on the hosted smolfleet cloud (api.smolmachines.com)."""

    _cloud = True

    @staticmethod
    def type() -> str:
        return "smolvm-cloud"
