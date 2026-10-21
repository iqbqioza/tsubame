# Tsubame benchmarks

This is a standalone crate so the benchmark dependencies (including heed and
its LMDB build) are not part of the published workspace or CI.

```sh
cd benchmarks
cargo run --release
```

The harness compares:

- batch insert into an empty unnamed database (one transaction)
- point reads from a read-only snapshot
- durable one-key commit latency
- reopen time

Results are machine- and filesystem-dependent. Run with a quiet machine and
compare both engines in the same invocation.

Tsubame does not have cursors yet, so range scans are not included.
