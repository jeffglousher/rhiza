# SQL read concurrency diagnostic — 2026-08-10

This document records the local observation used while evaluating replacement
of the single SQLite read mutex with a bounded persistent read-connection pool.
The raw reports and input hashes remain uncommitted under `target/`, so this is
not independently reproducible release evidence or a publishable cross-system
benchmark.

## Contract

- Baseline: `a7f0b43298e281995bc7ec1a6e389b5555d69cef`
- Candidate: the dirty worktree at the same HEAD, with the relevant input
  hashes retained in `target/sql-read-concurrency-comparison/20260809T182226Z-32559`
- Command: `rhiza-profile --profile sql --workload native-read --layer runtime
  --consistency local --operations 50000 --warmup 5000 --concurrency {1,4,16}`
- Repetitions: three, interleaving baseline and candidate
- Host: Apple `Mac15,13`, 8 logical CPUs, macOS 26.3, APFS
- Toolchain: Rust/Cargo 1.97.1

Every one of the 18 reports completed 50,000 operations with zero errors.
No Cargo or rustc process was present at the measurement boundaries, and the
relevant SQL, Node, and benchmark inputs did not drift during the run.

## Median results

| Concurrency | Baseline ops/s | Candidate ops/s | Candidate / baseline | p99 baseline | p99 candidate |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 93,118 | 163,803 | 1.76x | 13 us | 7 us |
| 4 | 74,722 | 388,379 | 5.20x | 32 us | 18 us |
| 16 | 75,137 | 350,339 | 4.66x | 452 us | 205 us |

In this retained local run, concurrency 1 did not regress and concurrency 4 and
16 improved materially. The first implementation, which
opened and closed a SQLite connection for every query, was rejected before
adoption after measuring 3.77x–7.44x lower throughput than baseline. The
accepted implementation instead reuses a bounded pool and drains it before
every exclusive canonical-file transition.

## Limits

These numbers cover an in-process runtime-layer local read on one macOS host.
They do not establish Linux, container, remote-client, strong-read, mixed
read/write, or Rhiza-versus-Hiqlite performance. Raw reports remain local
diagnostic artifacts under `target/` and are intentionally not committed.
