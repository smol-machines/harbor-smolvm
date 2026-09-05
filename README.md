# Branch real agent-eval environments from one running machine

This repository contains reproducible [Harbor](https://github.com/laude-institute/harbor) and [Braintrust](https://github.com/braintrustdata/bash-agent-evals) experiments for Smol's branchable machine runtime.

The Harbor provider itself ships in the `smolmachines` SDK. It prepares one running machine for a task image, then gives each trial an isolated copy-on-write branch with the same warm memory and filesystem state. This package keeps the older `harbor_smolvm:SmolvmEnvironment` import working and houses the public benchmark harness.

For the complete public comparison, run `./demo-public-suite.sh`. It executes a verified Terminal-Bench task through Smol and Docker, runs Braintrust's pinned data application through equivalently prepared Smol and Docker environments, then writes one standalone HTML report.

## Reproduce the Terminal-Bench demo

You need Python 3.12+, `uv`, KVM on Linux (or HVF on Apple Silicon), and enough memory for the requested concurrency. Add a working Docker daemon when you include the Docker baseline.

```bash
git clone https://github.com/smol-machines/harbor-smolvm.git
cd harbor-smolvm
uv sync --extra dev

# Fast lifecycle-only check: no model or verifier network traffic.
ATTEMPTS=16 CONCURRENCY=16 REPETITIONS=3 \
  ./demo-terminal-bench.sh --install-only
```

To compare the complete verified task with Harbor's default Docker provider:

```bash
PROVIDERS="smol-branch docker" ATTEMPTS=4 CONCURRENCY=4 REPETITIONS=3 \
  ./demo-terminal-bench.sh \
  --prepare-script bench/warmups/terminal_bench_verifier.sh
```

The harness downloads the public `terminal-bench-sample@2.0` dataset and runs its `regex-log` task unchanged. Every oracle trial must score `1.0`; a missing, errored, or lower reward makes the command fail. Provider order reverses between repetitions, checkpoint preparation is reported separately, and each run writes raw Harbor artifacts plus JSON, CSV, and a standalone HTML report.

## Current measured result

On a 26-vCPU Intel Xeon Platinum 8480+ host, four concurrent `regex-log` trials were run three times per provider:

| Path | Median wall time per 4-trial wave | Setup p50 | Verifier p50 | Correct |
| --- | ---: | ---: | ---: | ---: |
| Prepared Smol branches | 11.75 s | 0.80 s | 7.44 s | 12/12 |
| Equivalently prepared Harbor Docker | 19.72 s | 1.00 s | 4.25 s | 12/12 |

That is a **1.68× steady-state lifecycle speedup** for this task. Both environments ran the same preparation script: the live Smol checkpoint took 19.47 seconds to create and warm, while building the warmed Docker image took 10.49 seconds on a cold cache. Charging both costs across only three waves leaves Smol at **1.27×** using the median wave times.

The result is narrower than “VM execution is faster.” Docker's verifier finished 1.75× faster, while Smol reached the four isolated environments 1.25× faster and avoided Docker's long default teardown. This is a comparison of Harbor's user-visible lifecycle, including cleanup; it demonstrates faster repeated eval orchestration despite slower work inside each VM.

Without the preparation script, this task repeatedly downloads verifier dependencies inside every guest. In that deliberately cold shape, concurrent Smol verifier work was slower than Docker on this host. The warm checkpoint is the feature under test: perform repeatable setup once, then branch the initialized state for each clean trial.

For environment lifecycle alone, five four-way waves completed without errors at 3.55 seconds median for Smol versus 14.88 seconds for Harbor Docker. Actual setup p50 was much closer (0.87 versus 1.02 seconds); fast cleanup accounts for most of that full-lifecycle difference.

## Run Braintrust's public data-agent workload

The second experiment uses Braintrust's real `bash-agent-evals` application at a pinned commit and a digest-pinned Node base image. It downloads and transforms the project's 958 MB GH Archive corpus, installs its TypeScript dependencies and native SQLite module, checkpoints that initialized state, and gives different eval questions to independent branches. The optional Docker control uses the same prepared application and the same two-CPU, 4 GiB runtime limit.

```bash
# No model key: verify the real dataset/runtime with deterministic queries.
uv run python bench/braintrust_fanout.py --fanout 4 --parallel 4 --docker

# Run the repository's ordinary SQL agent unchanged.
ANTHROPIC_API_KEY=... uv run python bench/braintrust_fanout.py \
  --fanout 4 --parallel 4 --mode agent --agent sql \
  --model claude-sonnet-4-5
```

In the matched 26-vCPU-host control, three four-way repetitions produced 12/12 correct outputs on both runtimes. Smol created each four-branch wave in 0.460 seconds median and then completed the queries in 0.908 seconds; four warm Docker containers completed in 0.209 seconds. Docker is 6.55× faster for this tiny, disk-prepared workload. That negative result establishes an important boundary: branching a live machine is not useful when ordinary container startup and the entire task already fit in a few hundred milliseconds. A model-backed score is intentionally not claimed until run with a real model key.

Use `--keep-checkpoint` to retain the expensive prepared state, then pass its printed name through `--checkpoint NAME` for later fan-out waves.

## Run a real SWE-bench Verified issue

The third experiment downloads Harbor's 500-task SWE-bench Verified package and selects `django__django-10999`, a real Django issue. It applies the published oracle patch independently in every environment and runs the official issue-specific test and grading path. The task contents, source-tree hash, and public SWE-bench image digest are recorded rather than replaced with a toy fixture.

```bash
uv run python bench/swebench_verified.py \
  --fanout 4 --parallel 4 --repetitions 3
```

On the same 26-vCPU host, Smol completed each four-trial wave in 24.93 seconds median versus Docker's 27.03 seconds, a modest **1.08× lifecycle speedup**. Both paths passed 12/12 trials at reward `1.0`. Smol setup was 0.946 versus 1.023 seconds, while Docker's actual verifier was much faster at 11.38 versus 19.30 seconds. This is best read as end-to-end compatibility and approximate lifecycle parity on a recognizable coding-agent task—not as faster VM compute. The selected SWE-bench image is x86-64, so this demo currently requires an x86-64 Linux host.

## Compatibility proven beyond the headline demo

- `sqlite-with-gcov`: 8/8 trials passed across branched and cold machines. Each trial installed build tools, unpacked SQLite, configured coverage instrumentation, compiled in parallel, and ran the Python verifier. Branch readiness was 24.4× faster (0.192 versus 4.684 seconds p50), but network and compilation variance were too large for an honest full-runtime claim.
- `configure-git-webserver`: 2/2 concurrent branches passed after installing and starting SSH and nginx, creating users, initializing a bare Git repository, and exercising deployment hooks. This verifies that branches are not limited to shell snippets; independent long-running service state works.

## Use the provider directly

```bash
pip install "smolmachines[harbor]"

harbor run --path /path/to/task --agent oracle \
  --env smol.harbor:SmolEnvironment \
  --n-attempts 16 --n-concurrent 16
```

The same provider accepts `--ek target=cloud` with Smol Cloud credentials. Set `auto_checkpoint=false` to create one cold machine per trial, or pass a prepared machine through the provider's `checkpoints` map when setup must happen before the benchmark starts.

## What the harness records

- Full wall time and Harbor job time.
- Environment setup, agent, and verifier p50/p95/p99.
- One-time checkpoint preparation.
- Trial errors and verifier rewards.
- Approximate host-memory pressure, clearly labeled as a `MemAvailable` delta rather than per-machine RSS.
- Host, Python, Harbor, and SDK versions.

Raw jobs and ad hoc results stay untracked. `bench/render_results.py` turns any result JSON into a self-contained report suitable for a browser or screen recording.

## Honest boundaries

- Harbor tasks must publish an OCI `docker_image`; Dockerfile-only and Compose tasks are not supported by the current provider.
- Setup-heavy tasks only benefit when the useful prepared state is inside the checkpoint. Repeating package downloads after every branch can dominate the entire run.
- One public `build-cython-ext` sample currently scores `0.0` from both cold and branched Smol machines because the same upstream `pyknotid` repository test fails in each. The harness caught this dependency/test drift and excludes it from performance claims.
- A Linux 6.8 H100 host completed repeated four-way branch waves but became unreliable during a sustained 16-way cold-boot stress run. Do not publish high-fanout results from a host that reports VM boot errors; use a current kernel and require every trial to pass.

## Development

```bash
uv run ruff format --check bench src tests
uv run ruff check bench src tests
uv run pytest -q
```

Apache-2.0.
