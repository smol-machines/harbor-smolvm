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
- **Fast parallelism via CoW fork** — build the task image into a golden VM once,
  then copy-on-write clone it per trial (sub-second start) for `--n-concurrent`.

## Install

```bash
pip install harbor-smolvm            # this backend
uv tool install harbor               # Harbor itself (if not already installed)
```

You also need the **`smol` CLI** on `PATH` (or set `SMOL_BIN`). See
[smolmachines.com](https://smolmachines.com).

## Use

```bash
# local microVM (this host)
harbor run --path examples/tasks/smoke --agent oracle \
  --env harbor_smolvm:SmolvmEnvironment

# hosted smolfleet cloud (needs ~/.config/smolvm/config.toml [cloud] api_key)
harbor run --path examples/tasks/smoke-cloud --agent oracle \
  --env harbor_smolvm:SmolvmCloudEnvironment
```

Each lifecycle method shells to `smol`:

```
start(force_build) -> smol create -I <image> --net ; smol start   (local)
                   -> smol deploy <image>                          (cloud)
exec(command)      -> smol exec [--cloud] [-w cwd] [-e k=v] -- sh -c cmd
upload/download    -> smol cp [--cloud] ...   (staged through /workspace)
stop(delete)       -> smol stop ; smol rm --force   /  smol destroy
```

A task's environment must declare a prebuilt image in `task.toml`
(`[environment].docker_image = "<ref>"`). Dockerfile-only tasks (no
`docker_image`) need a docker-build → smolvm-import bridge that is not yet wired.

## Parallelism — golden VM + CoW fork

Harbor's `--n-concurrent` runs trials as asyncio tasks in one process. Instead of
a full image pull + boot per trial, this backend builds **one forkable golden VM
per task image**, then `smol fork`s a copy-on-write clone per trial. The golden
registry is guarded by an `asyncio.Lock` (built once, ref-counted by live clones,
torn down after the last one). Knobs:

```
SMOLVM_HARBOR_FORK=auto|on|off    # default auto
SMOLVM_HARBOR_KEEP_GOLDEN=1       # keep goldens warm across runs
```

Fork works on Linux/KVM and macOS/HVF (the latter needs the `smol` binary signed
with a JIT entitlement). Cloud trials don't fork yet (the `deploy` command has no
`--forkable` surface). Where fork is unavailable the backend **degrades
automatically** to a full per-trial machine, so it runs everywhere.

## How it works (two non-obvious details)

`smol`'s `exec` and `cp` touch **different filesystems**, and a container's root
is normally fresh per exec. The backend handles both:

1. **Persistent root across execs** — Harbor runs the agent and verifier as
   separate `exec` calls and expects their writes to `/` to persist; the backend
   runs execs in the machine's persistent overlay so they do.
2. **`/workspace` file-transfer bridge** — `smol cp` reads/writes the agent's VM
   root while `smol exec` runs in the container overlay; they don't share `/`.
   The one path both see is the `/workspace` volume, so every transfer stages
   through `/workspace/.hb` and an `exec` moves it the last hop.

`start()` also pre-creates Harbor's canonical guest dirs (`/logs/agent`,
`/logs/verifier`, `/tests`, `/solution`, …) since a bare OCI image has none, and
runs agent commands through `bash` when present (agents emit bashisms).

## Status

Validated end-to-end with the model-free **oracle** agent:

- **Local** (macOS/HVF): `examples/tasks/smoke` → reward 1.0
- **Cloud** (smolfleet): `examples/tasks/smoke-cloud` → reward 1.0
- **Concurrent** `-k 4 -n 4` → mean 1.0; **real CoW fork** on macOS → mean 1.0
- **Fork orchestration** unit test (`tests/`) → green

A real **model-driven** run (e.g. `--agent claude-code`) installs the agent in
the VM and reaches the model API end-to-end; validate with your own key.

Known gaps: cloud CoW fork (needs a `smol deploy --forkable` surface);
Dockerfile-only tasks (needs a build→import bridge); a published green model run.

## License

Apache-2.0.
