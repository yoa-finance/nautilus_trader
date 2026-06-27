# StratNeo Cargo Crate Release

This runbook covers the StratNeo single-crate release flow for Nautilus adapter
crates, especially `stratneo-nautilus-binance`. Use it when a StratNeo consumer
such as `trade-service` depends on the crate from crates.io.

## When This Is Needed

Changing source in `nautilus_trader` is not enough for consumers. For example,
`trade-service` depends on:

```toml
nautilus-binance = { package = "stratneo-nautilus-binance", version = "=0.59.0", ... }
```

That pin resolves from crates.io. A consumer will not receive a fix until the
adapter crate is published and the consumer pin and lockfile are updated.

## Preconditions

- Work from a clean `nautilus_trader` tree.
- Use the repo publish script, not raw `cargo publish`.
- Do not print registry tokens.
- crates.io versions are immutable. Never try to republish the same version.
- For a breaking public API/type change in a `0.x` crate, prefer a minor bump
  such as `0.58.8` to `0.59.0`. Use a patch bump only for compatible fixes.

The crates.io token is stored in Infisical:

| Field | Value |
|-------|-------|
| Environment | `prod` |
| Path | `/k8s/infrastructure/workflows` |
| Key | `CRATES_IO_TOKEN` |

## 1. Bump The Crate Version

Update both the package version and the workspace dependency entry:

- `crates/adapters/binance/Cargo.toml`
- `Cargo.toml`

Then update the workspace lockfile:

```bash
cd /home/allen/StratNeo/nautilus_trader
cargo +1.96.0 update -p stratneo-nautilus-binance --precise 0.59.0
```

Run the adapter tests before publishing:

```bash
cargo +1.96.0 test -p stratneo-nautilus-binance
```

## 2. Check The Publish Plan

Use the official script in check mode:

```bash
cd /home/allen/StratNeo/nautilus_trader
CARGO_PUBLISH_SUCCESS_DELAY_SECONDS=0 \
bash scripts/ci/publish-cargo-crates.sh \
  --check \
  --package stratneo-nautilus-binance \
  --version 0.59.0
```

Expected output includes:

```text
Publishing crates in dependency order:
1. stratneo-nautilus-binance  0.59.0
Cargo crate publish plan is valid.
```

Optionally verify the target version is still available:

```bash
curl --proto '=https' --tlsv1.2 --silent --show-error --location \
  --header 'User-Agent: stratneo-local-release-check (https://github.com/yoa-finance/nautilus_trader)' \
  https://crates.io/api/v1/crates/stratneo-nautilus-binance/0.59.0
```

A `404` response means the version is not published yet.

## 3. Publish To crates.io

Read the token from Infisical into a shell variable and pass it only through the
child process environment:

```bash
cd /home/allen/StratNeo/nautilus_trader

crates_io_token="$(
  infisical export \
    --silent \
    --env=prod \
    --path=/k8s/infrastructure/workflows \
    --format=json \
    --include-imports=false \
  | jq -r '.[] | select(.key == "CRATES_IO_TOKEN") | .value' \
  | head -n 1
)"

test -n "${crates_io_token}"

CARGO_REGISTRY_TOKEN="${crates_io_token}" \
CARGO_PUBLISH_ATTEMPTS=5 \
CARGO_PUBLISH_POLL_SECONDS=5 \
CARGO_PUBLISH_RETRY_DELAY_SECONDS=15 \
CARGO_PUBLISH_SUCCESS_DELAY_SECONDS=0 \
CARGO_PUBLISH_WAIT_TIMEOUT_SECONDS=120 \
CARGO_PUBLISH_USER_AGENT='stratneo-local-release (https://github.com/yoa-finance/nautilus_trader)' \
bash scripts/ci/publish-cargo-crates.sh \
  --package stratneo-nautilus-binance \
  --version 0.59.0

unset crates_io_token
```

Expected output includes:

```text
Uploaded stratneo-nautilus-binance v0.59.0 to registry `crates-io`
Published stratneo-nautilus-binance v0.59.0 at registry `crates-io`
Finished publishing Cargo crates.
```

Verify crates.io can see the version:

```bash
curl --proto '=https' --tlsv1.2 --silent --show-error --location \
  --header 'User-Agent: stratneo-local-release-check (https://github.com/yoa-finance/nautilus_trader)' \
  https://crates.io/api/v1/crates/stratneo-nautilus-binance/0.59.0 \
| jq -r '"crate=" + .version.crate + " version=" + .version.num + " yanked=" + (.version.yanked|tostring)'
```

Expected:

```text
crate=stratneo-nautilus-binance version=0.59.0 yanked=false
```

## 4. Audit And Update Consumers

After crates.io has the new version, audit all StratNeo Cargo consumers before
updating any one repo:

```bash
cd /home/allen/StratNeo
rg -n "stratneo-nautilus-binance|nautilus-binance" \
  --glob 'Cargo.toml' \
  --glob 'Cargo.lock' \
  --glob '!target'
```

Known consumer actions:

| Repo | Action |
|------|--------|
| `trade-service` | Update the direct `nautilus-binance` pin and lockfile. |
| `backtest-service` | No update unless the audit finds `stratneo-nautilus-binance`; it currently has no direct or resolved Binance adapter dependency. |

Confirm the resolved dependency graph in any suspected consumer:

```bash
cd /home/allen/StratNeo/backtest-service
INFISICAL_ENV=prod ./scripts/with_infisical.sh \
  cargo +1.96.0 tree -i stratneo-nautilus-binance
```

If Cargo reports `did not match any packages`, do not add a new dependency just
for the release. That repo is not consuming the published adapter crate.

For `trade-service`, update the consumer pin:

```toml
nautilus-binance = { package = "stratneo-nautilus-binance", version = "=0.59.0", default-features = false, features = ["high-precision"] }
```

Then update `trade-service/Cargo.lock` using the service wrapper so the private
registry credentials are present:

```bash
cd /home/allen/StratNeo/trade-service

INFISICAL_ENV=prod \
INFISICAL_LOAD_APP_PATH=false \
INFISICAL_LOAD_SHARED_PATH=true \
./scripts/with_infisical.sh \
  cargo +1.96.0 update -p stratneo-nautilus-binance --precise 0.59.0
```

Review `Cargo.lock`. If Cargo only changed unrelated transitive dependency edge
selection, reduce the diff back to the adapter version and checksum. The desired
consumer diff should normally be:

- `Cargo.toml`: `=old` to `=new`
- `Cargo.lock`: `stratneo-nautilus-binance` version and checksum

## 5. Verify Consumers

For `trade-service`, run the live runner target first, then the workspace:

```bash
cd /home/allen/StratNeo/trade-service

INFISICAL_ENV=prod \
INFISICAL_LOAD_APP_PATH=false \
INFISICAL_LOAD_SHARED_PATH=true \
./scripts/with_infisical.sh cargo +1.96.0 check -p trade-runner

INFISICAL_ENV=prod \
INFISICAL_LOAD_APP_PATH=false \
INFISICAL_LOAD_SHARED_PATH=true \
./scripts/with_infisical.sh cargo +1.96.0 check --workspace
```

For any additional consumer found by the audit, run that repo's equivalent
package-level check plus workspace check. For `backtest-service`, no Binance
adapter verification is required when the dependency graph audit shows the crate
is absent.

## 6. Commit Order

Use separate commits when possible:

1. `nautilus_trader`: adapter fix.
2. `nautilus_trader`: crate version bump.
3. Consumer repos such as `trade-service`: pin and lockfile update.

This keeps the source fix, publish metadata, and consumer rollout auditable.

## Common Failures

- `CARGO_REGISTRY_TOKEN or CRATES_IO_TOKEN not set`: read `CRATES_IO_TOKEN`
  from Infisical and pass it as `CARGO_REGISTRY_TOKEN`.
- `Cannot index array with string "CRATES_IO_TOKEN"`: Infisical JSON export is
  an array of objects. Use `.[] | select(.key == "CRATES_IO_TOKEN") | .value`.
- `registry index was not found in any configuration: stratneo_rust_crates` in
  `trade-service`: run Cargo through `scripts/with_infisical.sh` with shared
  secrets enabled.
- Consumer still uses the old code: confirm the consumer pin and lockfile point
  to the published version, not only to a local source commit.
