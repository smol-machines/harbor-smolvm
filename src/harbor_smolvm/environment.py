"""Harbor environment backend that runs task environments on smolvm / smolfleet.

Use it as a custom Harbor environment via its import path (after
``pip install harbor-smolvm``):

    # local microVM (this host)
    harbor run ... --env harbor_smolvm:SmolvmEnvironment
    # hosted smolfleet cloud (api.smolmachines.com)
    harbor run ... --env harbor_smolvm:SmolvmCloudEnvironment

A Harbor task's environment is an OCI image; smolvm runs that image as a real
microVM (locally, cross-platform) or smolfleet runs it on the cloud — both via
the unified ``smol`` CLI. Each lifecycle method shells to ``smol``:

    start(force_build) -> smol create -I <image> ; smol start      (local)
                       -> smol deploy <image>                       (cloud)
    exec(command)      -> smol exec [-w cwd] [-e k=v] -- sh -c cmd  (+ --cloud)
    upload/download    -> smol cp <src> <dst>                       (+ --cloud)
    stop(delete)       -> smol stop ; smol rm        / smol destroy

Requires the ``smol`` CLI on PATH (or set ``SMOL_BIN`` to its path).

Env overrides:
  SMOL_BIN         path to the ``smol`` binary (else discovered on PATH)
  SMOLVM_LIB_DIR   the bundled libkrun dir, exported as DYLD/LD_LIBRARY_PATH for
                   the subprocess (only needed for an unbundled/dev build; a
                   released ``smol`` finds its own libs)
  SMOLVM_HARBOR_FORK        auto|on|off  — CoW-fork clones per trial (default auto)
  SMOLVM_HARBOR_KEEP_GOLDEN 1            — keep the golden VM warm across runs
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shlex
import shutil
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

def _smol_bin() -> str:
    """Resolve the ``smol`` binary: ``SMOL_BIN`` env, else PATH."""
    found = os.environ.get("SMOL_BIN") or shutil.which("smol")
    if not found:
        raise RuntimeError(
            "the `smol` CLI was not found — install it and ensure it is on PATH, "
            "or set SMOL_BIN to its path. See https://smolmachines.com"
        )
    return found


def _subprocess_env() -> dict[str, str]:
    env = dict(os.environ)
    # Only wire the bundled-libkrun lookup when explicitly pointed at it (a dev
    # build with a separate lib/ dir). A released `smol` finds its own libraries.
    lib = os.environ.get("SMOLVM_LIB_DIR")
    if lib and Path(lib).is_dir():
        # smol dlopens the bundled libkrun/libkrunfw at runtime; SMOLVM_LIB_DIR is
        # the explicit lookup, DYLD/LD_LIBRARY_PATH the loader fallback.
        env["SMOLVM_LIB_DIR"] = lib
        env["DYLD_LIBRARY_PATH"] = lib + ":" + env.get("DYLD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = lib + ":" + env.get("LD_LIBRARY_PATH", "")
    return env


# ---- Phase 2: golden-VM + CoW fork fast path -------------------------------
# Harbor's `--n-concurrent` runs trials as asyncio tasks in ONE process, so a
# module-level golden registry guarded by an asyncio.Lock is the right primitive
# (no cross-process file locks). Per task image we build ONE forkable golden VM,
# then `smol fork` a copy-on-write clone per trial — instant start, no repeated
# image pull. Falls back to a full per-trial machine where forkable boot isn't
# available (cloud `deploy` has no --forkable yet; macOS/HVF can't memfd-back a
# forkable golden — that path needs Linux/KVM or the cloud control plane).
#
#   SMOLVM_HARBOR_FORK=auto|on|off   (default auto: try fork, degrade silently)
#   SMOLVM_HARBOR_KEEP_GOLDEN=1      (keep goldens warm across runs)
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
    """Shared per-image golden VM: built once, ref-counted by live clones."""

    __slots__ = ("name", "refcount", "ready")

    def __init__(self, name: str):
        self.name = name
        self.refcount = 0
        self.ready = False


# image ref -> golden state, guarded by _golden_lock; _fork_unavailable latches
# True the first time a forkable boot fails so we stop retrying it this process.
_goldens: dict[str, _GoldenState] = {}
_golden_lock = asyncio.Lock()
_fork_unavailable = False


class _SmolEnvBase(BaseEnvironment):
    """Shared smol-CLI driver; subclasses set ``_cloud``."""

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
        # smol machine name: stable + DNS-safe, unique per trial. Collapse runs
        # of non-alnum to a single hyphen (smol rejects consecutive hyphens) and
        # trim leading/trailing hyphens.
        slug = re.sub(r"[^a-z0-9]+", "-", session_id.lower()).strip("-")[:40]
        self._name = ("hb-" + slug).rstrip("-")
        # On the cloud, deploy returns a machine id we operate on.
        self._cloud_id: str | None = None
        # Set when this trial's machine is a CoW fork of a golden (Phase 2).
        self._forked: bool = False
        self._golden_image: str | None = None

    # ---- capabilities / metadata ----
    @staticmethod
    def type() -> str:  # selected via the import path, so any stable str works
        return "smolvm-cloud" if False else "smolvm"

    @classmethod
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
        return EnvironmentResourceCapabilities(cpu_limit=True, memory_limit=True)

    @property
    def capabilities(self) -> EnvironmentCapabilities:
        # File transfer is via `smol cp`, not a host bind-mount.
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

    # ---- subprocess helper ----
    async def _smol(
        self,
        args: list[str],
        *,
        check: bool = True,
        timeout_sec: int | None = None,
    ) -> ExecResult:
        proc = await asyncio.create_subprocess_exec(
            _smol_bin(),
            *args,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_subprocess_env(),
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise
        res = ExecResult(
            stdout=out.decode(errors="replace"),
            stderr=err.decode(errors="replace"),
            return_code=proc.returncode if proc.returncode is not None else -1,
        )
        if check and res.return_code != 0:
            raise RuntimeError(
                f"smol {' '.join(args)} failed ({res.return_code}):\n{res.stderr}"
            )
        return res

    def _target(self) -> list[str]:
        """Name/id + --cloud flag selecting the machine for exec/cp."""
        if self._cloud:
            return ["-n", self._cloud_id or self._name, "--cloud"]
        return ["-n", self._name]

    # Canonical guest dirs Harbor redirects script stdout into and uploads
    # solution/tests/skills to. A bare OCI image has none of them, so the very
    # first `(script) > /logs/agent/oracle.txt 2>&1` redirect would fail before
    # the script runs. Mounted backends get these from bind-mounts; we mkdir.
    _HARBOR_DIRS = (
        "/logs/agent",
        "/logs/verifier",
        "/logs/artifacts",
        "/tests",
        "/solution",
        "/harbor/skills",
    )

    def _golden_name(self, image: str) -> str:
        return "hb-golden-" + hashlib.sha1(image.encode()).hexdigest()[:12]

    def _fork_enabled(self) -> bool:
        # Fork fast-path is local-only for now: the cloud `deploy` command has no
        # --forkable surface, so cloud trials always take the full deploy path.
        # Works on Linux/KVM and macOS/HVF (the latter needs the `smol` binary
        # signed with a JIT entitlement — see smolvm.entitlements; without it a
        # forkable boot fails -22 and we degrade to the full path). Unsupported
        # hosts fall back gracefully via the _ForkUnavailable latch.
        if self._cloud or _FORK_MODE in ("0", "off", "false", "no", "none"):
            return False
        return True

    async def _provision_dirs(self, target: list[str]) -> None:
        """Pre-create Harbor's canonical dirs + the /workspace staging area on
        the machine named by *target* (world-writable for non-root agent users)."""
        dirs = " ".join(self._HARBOR_DIRS)
        await self._exec_on(
            target,
            f"mkdir -p {dirs} {self._STAGE} && chmod 777 {dirs} {self._STAGE}",
        )

    async def _ensure_golden(self, image: str) -> str:
        """Bring up (once) a running, forkable golden VM for *image* and take a
        clone reference on it. Raises _ForkUnavailable if it can't boot forkable."""
        global _fork_unavailable
        async with _golden_lock:
            # Latched under the lock so a forkable-boot failure is attempted once,
            # not once per concurrent trial in the first batch.
            if _fork_unavailable:
                raise _ForkUnavailable("forkable boot already known-unavailable")
            st = _goldens.get(image)
            if st is None:
                st = _GoldenState(self._golden_name(image))
                _goldens[image] = st
            if not st.ready:
                # Exactly one builder: others wait on the lock, then see ready.
                try:
                    await self._smol(["create", "-n", st.name, "-I", image, "--net"])
                    await self._smol(["start", "-n", st.name, "--forkable"])
                except RuntimeError as e:
                    await self._smol(["stop", "-n", st.name], check=False)
                    await self._smol(["rm", "-n", st.name, "--force"], check=False)
                    _goldens.pop(image, None)
                    _fork_unavailable = True
                    raise _ForkUnavailable(str(e)) from e
                # No golden-side provisioning: `exec` keys the persistent overlay
                # on the machine name, so a clone named differently won't see the
                # golden's dirs — each clone provisions itself post-fork instead.
                st.ready = True
            st.refcount += 1
            return st.name

    async def _release_golden(self, image: str) -> None:
        async with _golden_lock:
            st = _goldens.get(image)
            if st is None:
                return
            st.refcount -= 1
            if st.refcount <= 0 and not _KEEP_GOLDEN:
                _goldens.pop(image, None)
                await self._smol(["stop", "-n", st.name], check=False)
                await self._smol(["rm", "-n", st.name, "--force"], check=False)

    # ---- lifecycle ----
    async def start(self, force_build: bool) -> None:
        image = self.task_env_config.docker_image
        global _fork_unavailable

        # Phase 2 fast path: CoW-fork a per-image golden instead of a full create.
        if self._fork_enabled() and not _fork_unavailable:
            golden = None
            try:
                golden = await self._ensure_golden(image)
                await self._smol(["fork", "--golden", golden, "-n", self._name])
                self._forked = True
                self._golden_image = image
                # The clone boots live (CoW RAM+disk) in ~0.1s; provision its
                # canonical dirs (its `exec` overlay is keyed on the clone name,
                # not the golden's, so it doesn't inherit the golden's setup).
                await self._provision_dirs(self._target())
                return
            except _ForkUnavailable:
                # Host can't boot a forkable golden (e.g. macOS/HVF). Latch off
                # and fall through to a full per-trial machine.
                _fork_unavailable = True
            except RuntimeError:
                # The golden booted but `fork` itself failed; release our ref and
                # degrade (unless fork was explicitly forced on).
                if golden is not None:
                    await self._release_golden(image)
                if _FORK_MODE in ("1", "on", "true", "yes", "force"):
                    raise
                _fork_unavailable = True

        # Fallback: a full per-trial machine (local create+start or cloud deploy).
        if self._cloud:
            res = await self._smol(["deploy", image])
            m = re.search(r"\bmach-[0-9a-f]+\b", res.stdout + res.stderr)
            self._cloud_id = m.group(0) if m else self._name
        else:
            # --net so the OCI image pull has egress.
            await self._smol(["create", "-n", self._name, "-I", image, "--net"])
            await self._smol(["start", "-n", self._name])
        await self._provision_dirs(self._target())

    async def stop(self, delete: bool) -> None:
        if self._forked:
            # Forked clones are cheap+ephemeral: always tear down and release the
            # golden ref (Harbor has already pulled logs/reward by now).
            await self._smol(["stop", "-n", self._name], check=False)
            await self._smol(["rm", "-n", self._name, "--force"], check=False)
            if self._golden_image is not None:
                await self._release_golden(self._golden_image)
            return
        if self._cloud:
            if self._cloud_id:
                await self._smol(["destroy", "-n", self._cloud_id], check=False)
            return
        await self._smol(["stop", "-n", self._name], check=False)
        if delete:
            await self._smol(["rm", "-n", self._name, "--force"], check=False)

    # Agents emit bash-only syntax (`&>`, `set -o pipefail`, ...), but debian's
    # /bin/sh is dash. Prefer bash when present; fall back to sh on minimal
    # images (e.g. alpine) that ship no bash. The command runs as "$1".
    _SHELL_WRAPPER = (
        'if command -v bash >/dev/null 2>&1; then exec bash -c "$1"; '
        'else exec sh -c "$1"; fi'
    )

    async def _exec_on(
        self,
        target: list[str],
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
    ) -> ExecResult:
        args = ["exec", *target]
        if cwd:
            args += ["-w", cwd]
        for k, v in (env or {}).items():
            args += ["-e", f"{k}={v}"]
        # /bin/sh runs the wrapper, which re-execs the command under bash if
        # available (so cwd/redirs/pipes and bashisms work), else sh.
        args += ["--", "/bin/sh", "-c", self._SHELL_WRAPPER, "sh", command]
        return await self._smol(args, check=False, timeout_sec=timeout_sec)

    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult:
        return await self._exec_on(
            self._target(), command, cwd=cwd, env=env, timeout_sec=timeout_sec
        )

    # File transfer crosses two filesystems: `smol cp` reads/writes the agent's
    # VM root, while `smol exec` runs in the container's (now persistent) overlay
    # — they do NOT share `/`. The one path both see is `/workspace` (a volume
    # mounted into the container and reachable by the agent). So every transfer
    # stages through `/workspace/.hb`, then an `exec` (which sees both /workspace
    # and the real target path) moves the bytes the last hop.
    _STAGE = "/workspace/.hb"

    def _stage_path(self, suffix: str = "") -> str:
        return f"{self._STAGE}/{uuid.uuid4().hex}{suffix}"

    async def _cp_in(self, local: Path | str, guest_path: str) -> None:
        args = ["cp"]
        if self._cloud:
            args.append("--cloud")
        tgt = (self._cloud_id or self._name) if self._cloud else self._name
        args += [str(local), f"{tgt}:{guest_path}"]
        await self._smol(args)

    async def _cp_out(self, guest_path: str, local: Path | str) -> None:
        args = ["cp"]
        if self._cloud:
            args.append("--cloud")
        src = (self._cloud_id or self._name) if self._cloud else self._name
        args += [f"{src}:{guest_path}", str(local)]
        await self._smol(args)

    async def upload_file(self, source_path: Path | str, target_path: str) -> None:
        stage = self._stage_path()
        await self._cp_in(source_path, stage)
        qt, qs = shlex.quote(target_path), shlex.quote(stage)
        res = await self.exec(
            f'mkdir -p "$(dirname {qt})" && cp {qs} {qt} && rm -f {qs}'
        )
        if res.return_code != 0:
            raise RuntimeError(
                f"upload_file -> {target_path} failed: {res.stderr or res.stdout}"
            )

    async def upload_dir(self, source_dir: Path | str, target_dir: str) -> None:
        # Tar the dir, stage the tarball via /workspace, untar in-guest.
        src = Path(source_dir)
        fd, tar_path = tempfile.mkstemp(suffix=".tar")
        os.close(fd)
        stage = self._stage_path(".tar")
        try:
            with tarfile.open(tar_path, "w") as tar:
                for item in src.iterdir():
                    tar.add(item, arcname=item.name)
            await self._cp_in(tar_path, stage)
            qd, qs = shlex.quote(target_dir), shlex.quote(stage)
            res = await self.exec(
                f"mkdir -p {qd} && tar -xf {qs} -C {qd} && rm -f {qs}"
            )
            if res.return_code != 0:
                raise RuntimeError(
                    f"upload_dir -> {target_dir} failed: {res.stderr or res.stdout}"
                )
        finally:
            os.unlink(tar_path)

    async def download_file(self, source_path: str, target_path: Path | str) -> None:
        stage = self._stage_path()
        qsrc, qst = shlex.quote(source_path), shlex.quote(stage)
        res = await self.exec(f"cp {qsrc} {qst}")
        if res.return_code != 0:
            raise FileNotFoundError(f"download_file: {source_path} not found in guest")
        try:
            await self._cp_out(stage, target_path)
        finally:
            await self.exec(f"rm -f {qst}")

    async def download_dir(self, source_dir: str, target_dir: Path | str) -> None:
        # Reverse of upload_dir: tar in-guest into /workspace, copy out, untar
        # locally. Tolerant of a missing source dir (Harbor pulls optional dirs).
        stage = self._stage_path(".tar")
        qsrc, qst = shlex.quote(source_dir), shlex.quote(stage)
        res = await self.exec(f"[ -d {qsrc} ] && tar -cf {qst} -C {qsrc} . || exit 7")
        if res.return_code != 0:
            return
        fd, tar_path = tempfile.mkstemp(suffix=".tar")
        os.close(fd)
        try:
            await self._cp_out(stage, tar_path)
            Path(target_dir).mkdir(parents=True, exist_ok=True)
            with tarfile.open(tar_path) as tar:
                tar.extractall(target_dir)
        finally:
            os.unlink(tar_path)
            await self.exec(f"rm -f {qst}")


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
