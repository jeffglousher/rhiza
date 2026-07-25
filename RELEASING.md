# Releasing rhiza

This runbook is for the SQL-only crates.io release, `v0.2.0`. It
covers exactly eight Rust crates: `rhiza-core`, `rhiza-obj-store`, `rhiza-log`,
`rhiza-quepaxa`, `rhiza-archive`, `rhiza-sql`, `rhiza-node`, and `rhizadb`.
`rhiza-graph`, `rhiza-kv`, `rhiza-client`, and `rhiza-cli` are workspace
components excluded from this release. `rhiza-testkit` and the
`basic-app-server` example have `publish = false` and are not released.

The facade is published and imported as `rhizadb`; this runbook uses
`rhizadb` in Cargo and registry commands.

## Preconditions and source freeze

Run the release from a clean, up-to-date local `main`. Do not release an
unpushed commit, a feature branch, or a working tree with uncommitted or
untracked files.

```bash
git fetch origin main
git switch main
git pull --ff-only origin main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
git diff --quiet
git diff --cached --quiet
test -z "$(git status --porcelain --untracked-files=all)"

cargo fmt --all -- --check
cargo test --workspace --all-features --locked
./scripts/check-workspace-packages.sh
```

After a follow-up release-preparation merge, replace a stale local `v0.2.0`
tag with an annotated tag at the verified `main` commit. The local tag is the
source freeze. **Do not push it yet.** A remote tag is immutable release
history: this procedure refuses to replace one.

```bash
git ls-remote --exit-code --tags origin refs/tags/v0.2.0 >/dev/null 2>&1 && {
  echo "origin already has v0.2.0; do not replace a remote release tag" >&2
  exit 1
}
git rev-parse -q --verify refs/tags/v0.2.0 >/dev/null && git tag -d v0.2.0
git tag -a v0.2.0 -m "rhiza v0.2.0"
git show --no-patch v0.2.0
```

## Authenticate with crates.io

Create a crates.io token with only the permissions needed to publish these
crates, then authenticate interactively:

```bash
cargo login
```

Cargo reads the token from standard input and stores it through its configured
credential provider. Do not put a token in a command line, shell history,
repository file, release notes, or issue.

## Publish by dependency tier

For each tier, first check whether each listed version is already visible on
crates.io. Skip visible versions; dry-run and publish only versions that are
not yet visible. At the end of the tier, verify registry visibility for
**every** crate before moving on. This is necessary because crates.io index
and package visibility can lag an accepted upload.

Run the following helper once in the same shell:

```bash
publish_tier() {
  local crate

  for crate in "$@"; do
    if cargo info --registry crates-io "$crate@0.2.0" >/dev/null 2>&1; then
      echo "Skipping $crate@0.2.0: already visible on crates.io."
      continue
    fi

    cargo publish --dry-run --locked -p "$crate" || return 1
    cargo publish --locked -p "$crate" || return 1
  done

  for crate in "$@"; do
    until cargo info --registry crates-io "$crate@0.2.0" >/dev/null 2>&1; do
      echo "Waiting for $crate@0.2.0 to become visible on crates.io..." >&2
      sleep 10
    done
    echo "Verified $crate@0.2.0 on crates.io."
  done
}
```

Publish tiers in this exact order:

```bash
# Tier 1: foundational crates
publish_tier rhiza-core rhiza-obj-store || exit 1

# Tier 2: direct consumers of rhiza-core
publish_tier rhiza-log rhiza-quepaxa || exit 1

# Tier 3: depend on tiers 1 and 2
publish_tier rhiza-archive rhiza-sql || exit 1

# Tier 4
publish_tier rhiza-node || exit 1

# Tier 5: facade crate
publish_tier rhizadb || exit 1
```

## If a publish attempt fails

Treat a failure as potentially ambiguous: crates.io may have accepted an
upload even if the local command lost its response. Never issue `cargo publish`
again for a version that might exist.

1. For each crate in the failing tier, run
   `cargo info --registry crates-io <crate>@0.2.0`.
2. If the version is visible, consider that crate published and do not rerun
   either its dry-run or publish command.
3. For a crate that is still absent, correct the reported issue, dry-run and
   publish **only that absent crate**, then wait for it to become visible.
4. Once every crate in that tier is visible, continue with the next tier.

This makes a resumed release idempotent: already-published versions are
observed, never republished. If a package needs changed contents after any
successful upload, stop this `v0.2.0` release and plan a new version; crates.io
versions are immutable.

## Publish the tag and GitHub release

Only after all eight initial SQL crate versions are visible with
`cargo info --registry crates-io` may the source freeze become public:

```bash
git push origin v0.2.0
gh release create v0.2.0 --title "rhiza v0.2.0" --generate-notes
```

Verify that the GitHub release targets `v0.2.0` and that it points to the same
commit reviewed before publishing.

## Container images are separate

Container publication is deliberately outside this runbook. CI builds the
image variants, but this repository does not configure an OCI registry target
or registry tags for publication. Define and review that target in a separate
release operation; do not infer or publish an image while performing this
crates.io release.
