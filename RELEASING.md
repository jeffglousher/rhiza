# Releasing rhiza

Repository tags and Rust crates have independent versions. A GitHub release is
a reviewed source snapshot; it does not imply crates.io or OCI publication.
Each release note must state exactly which channels were published.

## v0.4.0 source release

`v0.4.0` expands the supported source runtime from SQL-only to SQL plus an
opt-in Graph/LadybugDB profile. SQL remains the default and KV is excluded.
Graph Kubernetes manifests are beta; no OCI image is published by this runbook.

The v0.4.0 operation publishes only an annotated Git tag and GitHub release.
It must not run `cargo publish`, replace crate versions, or push container
images. The latest crates.io `rhizadb` artifact remains SQL-only v0.3.0.

### Preconditions

Run from a clean, up-to-date `main` after the release PR and required checks
have merged:

```bash
git fetch origin main --tags
git switch main
git pull --ff-only origin main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
test -z "$(git status --porcelain --untracked-files=all)"

cargo fmt --all -- --check
cargo test --workspace --all-features --locked
scripts/check-workspace-packages.sh
scripts/check-deploy.sh
cargo build --release --locked -p rhiza-cli --bin rhiza \
  --no-default-features --features graph,recorder-postcard-rpc
```

Confirm that SQL and Graph remain isolated:

```bash
cargo tree --locked -p rhiza-cli --no-default-features \
  --features graph,recorder-postcard-rpc \
  | grep -Eq 'rhiza-sql|rusqlite|libsqlite3' && exit 1 || true
```

If Docker is available, build the Graph image and verify its label and
entrypoint before tagging:

```bash
docker build --build-arg RHIZA_PROFILE=graph -t rhiza-graph:v0.4.0 .
test "$(docker image inspect rhiza-graph:v0.4.0 \
  --format '{{ index .Config.Labels "io.rhiza.build-profile" }}')" = graph
test "$(docker image inspect rhiza-graph:v0.4.0 \
  --format '{{ json .Config.Entrypoint }}')" = '["rhiza"]'
```

### Tag and GitHub release

The remote tag is immutable. Refuse to continue if it already exists:

```bash
git ls-remote --exit-code --tags origin refs/tags/v0.4.0 >/dev/null 2>&1 && {
  echo "origin already has v0.4.0; do not replace it" >&2
  exit 1
}

git tag -a v0.4.0 -m "rhiza v0.4.0"
git push origin v0.4.0
gh release create v0.4.0 --verify-tag --title "rhiza v0.4.0" \
  --notes-file CHANGELOG.md
```

Verify that the release and tag point to the reviewed `main` commit.

## Future crates.io release

Crates.io publication requires a separate release proposal that defines the
public crate set, aligns or explicitly documents crate versions, updates every
registry dependency version, establishes dependency-tier publish order, and
passes `cargo publish --dry-run` plus clean external-consumer tests for the SQL
default and Graph feature. Never infer crates.io publication from a repository
tag.
