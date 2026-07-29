# StratNeo Rust Crate Release

This runbook releases the pure-Rust StratNeo fork of NautilusTrader aligned to
official `v1.230.0`. It publishes one coordinated `0.60.0` bundle to crates.io.
It does not publish Python wheels, an sdist, PyPI artifacts, R2 artifacts,
container images, or an upstream-style GitHub release.

## Release boundary

The release script has an exact allowlist of 20 crates:

```text
stratneo-nautilus-analysis
stratneo-nautilus-backtest
stratneo-nautilus-common
stratneo-nautilus-core
stratneo-nautilus-cryptography
stratneo-nautilus-data
stratneo-nautilus-execution
stratneo-nautilus-indicators
stratneo-nautilus-live
stratneo-nautilus-model
stratneo-nautilus-network
stratneo-nautilus-persistence
stratneo-nautilus-persistence-macros
stratneo-nautilus-portfolio
stratneo-nautilus-risk
stratneo-nautilus-serialization
stratneo-nautilus-system
stratneo-nautilus-trading
stratneo-nautilus-binance
stratneo-nautilus-sandbox
```

The script fails if any crate is missing, has a different version, is not
publishable to crates.io, or if another `stratneo-nautilus-*` workspace package
exists outside this allowlist. It retains the upstream dependency-source and
unpublishable-local-dependency checks.

## Preconditions

- Confirm the fork is based on official tag `v1.230.0`.
- Confirm all 20 packages and their workspace dependency entries are `0.60.0`.
- Regenerate Cap'n Proto and Binance Spot SBE sources with the repository
  generators whenever their schemas change.
- Work from a reviewed, clean release commit. Do not release from an uncommitted
  worktree.
- Confirm the required official `nautilus-*` dependencies at `0.60.0` are
  already available on crates.io.
- Never print or persist a crates.io token in the repository or shell history.

The fork's `build.yml`, `build-v2.yml`, and `docker.yml` workflows explicitly
restrict upstream publication jobs to `nautechsystems/nautilus_trader`.
Therefore a push to `yoa-finance/nautilus_trader` cannot publish upstream
wheels, sdists, PyPI, R2, or GHCR artifacts, create an upstream tag, or run the
upstream release DAG. StratNeo Cargo publication is intentionally performed
with the script below.

## 1. Verify the source bundle

```bash
cd /home/allen/StratNeo/nautilus_trader

make check-capnp-schemas
cargo +1.96.0 check -p stratneo-nautilus-backtest --all-targets
cargo +1.96.0 check -p stratneo-nautilus-live
cargo +1.96.0 check -p stratneo-nautilus-binance --all-targets
```

Verify the Binance adapter exposes the package version expected by
`trade-service`:

```rust
nautilus_binance::STRATNEO_NAUTILUS_BINANCE_VERSION
```

## 2. Validate the exact publish plan

```bash
bash scripts/ci/publish-cargo-crates.sh \
  --check \
  --version 0.60.0
```

The output must contain exactly 20 entries, all at `0.60.0`, followed by:

```text
Cargo crate publish plan is valid.
```

Run Cargo packaging for those same 20 packages only:

```bash
bash scripts/ci/publish-cargo-crates.sh \
  --dry-run \
  --version 0.60.0
```

The dry-run iterates the allowlisted dependency plan. It never uses
`cargo publish --workspace`.

## 3. Verify consumers against the local fork

The known consumers are:

| Consumer | Direct StratNeo crates |
|---|---|
| `trade-service` | common, Binance, core, cryptography, execution, live, model, persistence, sandbox, system, trading |
| `backtest-service` | backtest, common, core, execution, model, persistence, portfolio, trading |
| `yoa-graph-runtime` | common, core, model, trading, portfolio |

Before publication, use each consumer's existing local `[patch.crates-io]`
entries to compile against this exact worktree:

```bash
cd /home/allen/StratNeo/trade-service
INFISICAL_ENV=prod \
INFISICAL_LOAD_APP_PATH=false \
INFISICAL_LOAD_SHARED_PATH=true \
./scripts/with_infisical.sh cargo +1.96.0 check -p trade-runner

cd /home/allen/StratNeo/backtest-service
INFISICAL_ENV=prod \
./scripts/with_infisical.sh cargo +1.96.0 check -p backtest-runner

cd /home/allen/StratNeo/yoa-graph-runtime
cargo +1.96.0 check --all-features
```

Treat compile failures as release blockers. In particular, validate the native
order-list API, backtest observer/termination API, endpoint-routed trading
commands, and the Binance version constant.

## 4. Publish

Retrieve the token from the approved secret store into a shell variable. The
script accepts `CARGO_REGISTRY_TOKEN` first and falls back to
`STRATNEO_CARGO_REGISTRY_TOKEN`, then the legacy `CRATES_IO_TOKEN`.

```bash
cd /home/allen/StratNeo/nautilus_trader

STRATNEO_CARGO_REGISTRY_TOKEN="${stratneo_crates_io_token}" \
CARGO_PUBLISH_ATTEMPTS=5 \
CARGO_PUBLISH_POLL_SECONDS=5 \
CARGO_PUBLISH_RETRY_DELAY_SECONDS=15 \
CARGO_PUBLISH_SUCCESS_DELAY_SECONDS=0 \
CARGO_PUBLISH_WAIT_TIMEOUT_SECONDS=300 \
CARGO_PUBLISH_USER_AGENT='stratneo-rust-release (https://github.com/yoa-finance/nautilus_trader)' \
bash scripts/ci/publish-cargo-crates.sh \
  --version 0.60.0
```

The script publishes in dependency order, skips an already-visible immutable
version, retries transient failures, and waits for both the crates.io API and
sparse index before moving to the next crate.

## 5. Verify registry consumers

After all 20 versions are visible, validate from clean consumer checkouts with
the local `[patch.crates-io]` overrides absent or disabled. Keep every direct
dependency pinned to `=0.60.0`, update lockfiles through the consumer's normal
credential wrapper, then rerun the commands from step 3 plus each workspace's
normal check.

Review lockfile changes carefully. The resolved `stratneo-nautilus-*` packages
must all be `0.60.0` from crates.io; no Git or local-path source should remain
in the registry verification checkout.

## Rollout order

1. Publish and verify all 20 crates.
2. Deploy `backtest-service` canary and exercise exact catalog loading,
   observer callbacks, and termination reporting.
3. Deploy `trade-service` canary and exercise Binance connect, submit,
   OCO/OPOCO, modify, cancel-all, reconnect, and reconciliation.
4. Deploy `yoa-graph-runtime` after its Nautilus feature build and graph
   execution smoke test pass.
5. Expand each deployment only after logs show no protocol anomalies, adapter
   fatal signals, duplicate accepted events, or valuation divergence.

Because crates.io versions are immutable, rollback means restoring consumer
pins and lockfiles to the previous published bundle. Never overwrite or
republish `0.60.0`.
