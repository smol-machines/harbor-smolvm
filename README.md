# harbor-smolvm

Run [Harbor](https://www.harborframework.com/) agent-evaluation tasks on
**[smolvm](https://github.com/smol-machines/smolvm)** microVMs — locally
(macOS/HVF, Linux/KVM, Windows/WHP) or on the hosted **smolfleet** cloud.

A Harbor task's environment is an OCI image. This package implements Harbor's
`BaseEnvironment` so each eval runs as a real per-task microVM instead of a
shared-kernel container or a cloud sandbox. Why you might want that:

- **Local evals with real isolation** — the only built-in *local* Harbor runtime
  is `--env docker` (shared kernel); agent evals run untrusted agent-written
  code, so a per-task kernel/VM boundary matters.
- **No cloud account, no per-run cost** — run the same eval on your own machine.
- **Cross-platform parity** — the identical task image runs the same on
  macOS/Linux/Windows.
- **One API, local or cloud** — the same backend targets an embedded local engine
  or the smolfleet cloud.

It drives the smolvm **Python SDK** ([`smolmachines`](https://pypi.org/project/smolmachines/)) —
a `Machine` API over both targets — so there is **no CLI to install** and no
subprocess plumbing.

## Install

```bash
pip install harbor-smolvm      # this backend (pulls the smolmachines SDK)
uv tool install harbor         # Harbor itself (or `pip install harbor`)
```

No separate binary or `PATH` setup — the `smolmachines` wheel bundles the engine.

## Use

```bash
# local microVM (this host)
harbor run --path examples/tasks/smoke --agent oracle \
  --env harbor_smolvm:SmolvmEnvironment

# hosted smolfleet cloud (set SMOL_CLOUD_TOKEN)
harbor run --path examples/tasks/smoke-cloud --agent oracle \
  --env harbor_smolvm:SmolvmCloudEnvironment
```

Each lifecycle method calls the SDK:

```
start(force_build) -> Machine.create(image=..., persistent=True[, forkable])
                      / golden.fork(name)              (CoW clone per trial)
exec(command)      -> Machine.exec(...)
upload/download    -> Machine.write_file / read_file
stop(delete)       -> Machine.stop() / delete()
```

A task's environment must declare a prebuilt image in `task.toml`
(`[environment].docker_image = "<ref>"`). Dockerfile-only tasks (no
`docker_image`) need a docker-build → smolvm-import bridge that is not yet wired.

## Why the SDK makes this simple

The SDK's semantics are exactly what Harbor's agent→verifier flow needs, so the
backend has almost no glue:

- **`exec` persists writes to `/` across calls** — the agent writes a solution and
  the verifier reads it back with no shared-volume tricks.
- **`write_file` / `read_file` share `exec`'s filesystem** — file transfer is a
  direct SDK call, no staging bridge.

`start()` still pre-creates Harbor's canonical guest dirs (`/logs/agent`,
`/logs/verifier`, `/tests`, `/solution`, …) since a bare OCI image has none, and
runs agent commands through `bash` when present (agents emit bashisms). The sync
SDK is wrapped with `asyncio.to_thread` for Harbor's async interface.

## Parallelism — golden VM + CoW fork (opt-in)

Per task image the backend can build **one forkable golden VM** and `fork` a
copy-on-write clone per trial (instant start) for `--n-concurrent` — a
module-level registry guarded by an `asyncio.Lock` builds it once, ref-counts it,
and tears it down after the last clone.

```
SMOLVM_HARBOR_FORK=on             # opt in (default: off)
SMOLVM_HARBOR_KEEP_GOLDEN=1       # keep goldens warm across runs
```

It is **opt-in** because the SDK's *local* fork currently times out waiting for
the clone agent (the CLI's fork works; this is being fixed upstream), so the
default path is a full per-trial machine. Where fork isn't available the backend
degrades automatically, so `--n-concurrent` always works.

## Status

Validated with the model-free **oracle** agent on the SDK backend (macOS/HVF):

- `examples/tasks/smoke` → reward **1.0**
- Concurrent `-k 3 -n 3` → mean **1.0** (~1s, machines cleaned up)
- Fork orchestration unit test (`tests/`) → green
- SDK semantics probe: `exec` persists root; `write_file`/`read_file` share it

A real **model-driven** run (e.g. `--agent claude-code`) installs the agent in
the VM and reaches the model API end-to-end; validate with your own key.

Known gaps: SDK local CoW fork (clone-agent timeout — upstream); cloud fork
(needs a forkable-deploy surface); Dockerfile-only tasks (build→import bridge).

## License

Apache-2.0.
