use std::hint::black_box;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use heed::EnvOpenOptions as HeedOptions;
use heed::byteorder::NativeEndian as HeedEndian;
use heed::types::{Bytes as HeedBytes, Str as HeedStr, U64 as HeedU64};
use tempfile::tempdir;
use tsubame::EnvOpenOptions as TsubameOptions;
use tsubame::byteorder::NativeEndian as TsubameEndian;
use tsubame::types::{Bytes as TsubameBytes, Str as TsubameStr, U64 as TsubameU64};

const MAP_SIZE: usize = 1024 * 1024 * 1024;
const LARGE_ENTRIES: u64 = 1_000;
const LARGE_VALUE_LEN: usize = 64 * 1024;

struct Config {
    entries: u64,
    reads: u64,
    commits: u64,
    warmup_reads: u64,
}

fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(|| Config {
        entries: env_u64("TSUBAME_BENCH_ENTRIES", 100_000),
        reads: env_u64("TSUBAME_BENCH_READS", 1_000_000),
        commits: env_u64("TSUBAME_BENCH_COMMITS", 50),
        warmup_reads: env_u64("TSUBAME_BENCH_WARMUP", 10_000),
    })
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

type TsubameDb = tsubame::Database<TsubameU64<TsubameEndian>, TsubameStr>;
type HeedDb = heed::Database<HeedU64<HeedEndian>, HeedStr>;
type TsubameLargeDb = tsubame::Database<TsubameU64<TsubameEndian>, TsubameBytes>;
type HeedLargeDb = heed::Database<HeedU64<HeedEndian>, HeedBytes>;

struct Report {
    insert: Duration,
    read: Duration,
    range: Duration,
    commit: Duration,
    delete: Duration,
    first_write: Duration,
    large_insert: Duration,
    large_read: Duration,
    reopen: Duration,
}

fn main() -> Result<()> {
    if let Ok(mode) = std::env::var("TSUBAME_BENCH_ONLY") {
        let dir = tempdir()?;
        match mode.as_str() {
            "tsubame-commit" => {
                let mut options = TsubameOptions::new();
                options.map_size(MAP_SIZE);
                let duration = bench_tsubame_commit(dir.path(), &mut options)?;
                println!(
                    "tsubame commit: {:.2} us/op",
                    duration.as_secs_f64() * 1_000_000.0 / config().commits as f64
                );
                return Ok(());
            }
            "heed-commit" => {
                let duration = bench_heed_commit(dir.path())?;
                println!(
                    "heed commit: {:.2} us/op",
                    duration.as_secs_f64() * 1_000_000.0 / config().commits as f64
                );
                return Ok(());
            }
            _ => {}
        }
    }

    let config = config();
    println!(
        "dataset: {} entries, {} point reads, {} commits, map {MAP_SIZE} bytes",
        config.entries, config.reads, config.commits
    );
    let tsubame = run_tsubame()?;
    let heed = run_heed()?;

    println!();
    println!("scenario                                  Tsubame          heed     Tsubame/heed");
    print_row("insert (1 txn)", tsubame.insert, heed.insert, 1, "ms");
    print_row(
        "point reads",
        tsubame.read,
        heed.read,
        config.reads,
        "ns/op",
    );
    print_row(
        "range scan",
        tsubame.range,
        heed.range,
        config.entries,
        "ns/op",
    );
    print_row(
        "large insert (64 KiB)",
        tsubame.large_insert,
        heed.large_insert,
        1,
        "ms",
    );
    print_row(
        "large reads (64 KiB)",
        tsubame.large_read,
        heed.large_read,
        LARGE_ENTRIES,
        "ns/op",
    );
    print_row(
        "durable commit",
        tsubame.commit,
        heed.commit,
        config.commits,
        "us/op",
    );
    print_row("bulk delete (1 txn)", tsubame.delete, heed.delete, 1, "ms");
    print_row("reopen", tsubame.reopen, heed.reopen, 1, "ms");
    print_row(
        "first write after reopen",
        tsubame.first_write,
        heed.first_write,
        1,
        "us/op",
    );

    Ok(())
}

fn print_row(label: &str, tsubame: Duration, heed: Duration, ops: u64, unit: &str) {
    let tsubame_value = per_op(tsubame, ops, unit);
    let heed_value = per_op(heed, ops, unit);
    let ratio = tsubame.as_secs_f64() / heed.as_secs_f64();
    println!(
        "{label:<40} {tsubame_value:>10.2} {unit:>4} {heed_value:>10.2} {unit:>4} {ratio:>10.2}x"
    );
}

fn per_op(duration: Duration, ops: u64, unit: &str) -> f64 {
    match unit {
        "ns/op" => duration.as_secs_f64() * 1_000_000_000.0 / ops as f64,
        "us/op" => duration.as_secs_f64() * 1_000_000.0 / ops as f64,
        "ms" => duration.as_secs_f64() * 1_000.0 / ops as f64,
        _ => duration.as_secs_f64() * 1_000.0,
    }
}

fn run_tsubame() -> Result<Report> {
    let config = config();
    let dir = tempdir()?;
    let mut options = TsubameOptions::new();
    options.map_size(MAP_SIZE);
    let values: Vec<String> = (0..config.entries).map(value).collect();

    let env = options.open(dir.path())?;
    let start = Instant::now();
    let mut wtxn = env.write_txn()?;
    let db: TsubameDb = env.create_database(&mut wtxn, None)?;
    for i in 0..config.entries {
        db.put(&mut wtxn, &i, values[i as usize].as_str())?;
    }
    wtxn.commit()?;
    let insert = start.elapsed();

    let rtxn = env.read_txn()?;
    let mut state = 0x1234_5678_9abc_def0;
    for _ in 0..config.warmup_reads {
        let key = next_key(&mut state, config.entries);
        black_box(db.get(&rtxn, &key)?);
    }
    let start = Instant::now();
    for _ in 0..config.reads {
        let key = next_key(&mut state, config.entries);
        black_box(db.get(&rtxn, &key)?);
    }
    let read = start.elapsed();

    let start = Instant::now();
    let mut scanned = 0u64;
    for item in db.iter(&rtxn)? {
        let (key, value) = item?;
        black_box((key, value));
        scanned += 1;
    }
    black_box(scanned);
    let range = start.elapsed();

    drop(rtxn);
    drop(db);
    drop(env);

    let start = Instant::now();
    let env = options.open(dir.path())?;
    let reopen = start.elapsed();
    let rtxn = env.read_txn()?;
    let db: TsubameDb = env.open_database(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, &0)?, Some(values[0].as_str()));
    drop(rtxn);
    let start = Instant::now();
    let first_write_txn = env.write_txn()?;
    let first_write = start.elapsed();
    first_write_txn.abort()?;
    drop(db);
    drop(env);

    let (large_insert, large_read) = bench_tsubame_large()?;
    let commit = bench_tsubame_commit(dir.path(), &mut options)?;
    let delete = bench_tsubame_delete(dir.path(), &mut options)?;

    Ok(Report {
        insert,
        read,
        range,
        commit,
        delete,
        first_write,
        large_insert,
        large_read,
        reopen,
    })
}

fn bench_tsubame_commit(path: &std::path::Path, options: &mut TsubameOptions) -> Result<Duration> {
    let config = config();
    let env = options.open(path)?;
    let mut wtxn = env.write_txn()?;
    let db: TsubameDb = env.create_database(&mut wtxn, None)?;
    wtxn.commit()?;

    let start = Instant::now();
    for i in 0..config.commits {
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, &i, "value")?;
        wtxn.commit()?;
    }

    Ok(start.elapsed())
}

fn bench_tsubame_delete(path: &std::path::Path, options: &mut TsubameOptions) -> Result<Duration> {
    let config = config();
    let env = options.open(path)?;
    let rtxn = env.read_txn()?;
    let db: TsubameDb = env.open_database(&rtxn, None)?;
    drop(rtxn);

    let start = Instant::now();
    let mut wtxn = env.write_txn()?;
    for i in (0..config.entries).step_by(2) {
        db.delete(&mut wtxn, &i)?;
    }
    wtxn.commit()?;
    Ok(start.elapsed())
}

fn run_heed() -> Result<Report> {
    let config = config();
    let dir = tempdir()?;
    let values: Vec<String> = (0..config.entries).map(value).collect();

    // SAFETY: this benchmark owns the directory and no other process opens it.
    let env = unsafe {
        HeedOptions::new()
            .map_size(MAP_SIZE)
            .max_dbs(1)
            .open(dir.path())?
    };
    let start = Instant::now();
    let mut wtxn = env.write_txn()?;
    let db: HeedDb = env.create_database(&mut wtxn, None)?;
    for i in 0..config.entries {
        db.put(&mut wtxn, &i, values[i as usize].as_str())?;
    }
    wtxn.commit()?;
    let insert = start.elapsed();

    let rtxn = env.read_txn()?;
    let mut state = 0x1234_5678_9abc_def0;
    for _ in 0..config.warmup_reads {
        let key = next_key(&mut state, config.entries);
        black_box(db.get(&rtxn, &key)?);
    }
    let start = Instant::now();
    for _ in 0..config.reads {
        let key = next_key(&mut state, config.entries);
        black_box(db.get(&rtxn, &key)?);
    }
    let read = start.elapsed();

    let start = Instant::now();
    let mut scanned = 0u64;
    for item in db.iter(&rtxn)? {
        let (key, value) = item?;
        black_box((key, value));
        scanned += 1;
    }
    black_box(scanned);
    let range = start.elapsed();

    drop(rtxn);
    drop(env);

    // SAFETY: the directory is still owned by this benchmark.
    let start = Instant::now();
    let env = unsafe {
        HeedOptions::new()
            .map_size(MAP_SIZE)
            .max_dbs(1)
            .open(dir.path())?
    };
    let reopen = start.elapsed();
    let rtxn = env.read_txn()?;
    let db: HeedDb = env
        .open_database(&rtxn, None)?
        .context("unnamed database is missing")?;
    assert_eq!(db.get(&rtxn, &0)?, Some(values[0].as_str()));
    drop(rtxn);
    let start = Instant::now();
    let first_write_txn = env.write_txn()?;
    let first_write = start.elapsed();
    first_write_txn.abort();
    drop(env);

    let (large_insert, large_read) = bench_heed_large()?;
    let commit = bench_heed_commit(dir.path())?;
    let delete = bench_heed_delete(dir.path())?;

    Ok(Report {
        insert,
        read,
        range,
        commit,
        delete,
        first_write,
        large_insert,
        large_read,
        reopen,
    })
}

fn bench_heed_commit(path: &std::path::Path) -> Result<Duration> {
    let config = config();
    // SAFETY: this benchmark owns the directory.
    let env = unsafe {
        HeedOptions::new()
            .map_size(MAP_SIZE)
            .max_dbs(1)
            .open(path)?
    };
    let mut wtxn = env.write_txn()?;
    let db: HeedDb = env.create_database(&mut wtxn, None)?;
    wtxn.commit()?;

    let start = Instant::now();
    for i in 0..config.commits {
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, &i, "value")?;
        wtxn.commit()?;
    }

    Ok(start.elapsed())
}

fn bench_heed_delete(path: &std::path::Path) -> Result<Duration> {
    let config = config();
    // SAFETY: this benchmark owns the directory.
    let env = unsafe {
        HeedOptions::new()
            .map_size(MAP_SIZE)
            .max_dbs(1)
            .open(path)?
    };
    let rtxn = env.read_txn()?;
    let db: HeedDb = env
        .open_database(&rtxn, None)?
        .context("unnamed database is missing")?;
    drop(rtxn);

    let start = Instant::now();
    let mut wtxn = env.write_txn()?;
    for i in (0..config.entries).step_by(2) {
        db.delete(&mut wtxn, &i)?;
    }
    wtxn.commit()?;
    Ok(start.elapsed())
}

fn bench_tsubame_large() -> Result<(Duration, Duration)> {
    let dir = tempdir()?;
    let mut options = TsubameOptions::new();
    options.map_size(MAP_SIZE);
    let env = options.open(dir.path())?;
    let value = vec![0x5au8; LARGE_VALUE_LEN];
    let mut wtxn = env.write_txn()?;
    let db: TsubameLargeDb = env.create_database(&mut wtxn, None)?;
    let start = Instant::now();
    for i in 0..LARGE_ENTRIES {
        db.put(&mut wtxn, &i, value.as_slice())?;
    }
    wtxn.commit()?;
    let insert = start.elapsed();

    let rtxn = env.read_txn()?;
    let start = Instant::now();
    for i in 0..LARGE_ENTRIES {
        black_box(db.get(&rtxn, &i)?);
    }
    let read = start.elapsed();
    Ok((insert, read))
}

fn bench_heed_large() -> Result<(Duration, Duration)> {
    let dir = tempdir()?;
    // SAFETY: this benchmark owns the directory.
    let env = unsafe {
        HeedOptions::new()
            .map_size(MAP_SIZE)
            .max_dbs(1)
            .open(dir.path())?
    };
    let value = vec![0x5au8; LARGE_VALUE_LEN];
    let mut wtxn = env.write_txn()?;
    let db: HeedLargeDb = env.create_database(&mut wtxn, None)?;
    let start = Instant::now();
    for i in 0..LARGE_ENTRIES {
        db.put(&mut wtxn, &i, value.as_slice())?;
    }
    wtxn.commit()?;
    let insert = start.elapsed();

    let rtxn = env.read_txn()?;
    let start = Instant::now();
    for i in 0..LARGE_ENTRIES {
        black_box(db.get(&rtxn, &i)?);
    }
    let read = start.elapsed();
    Ok((insert, read))
}

fn value(i: u64) -> String {
    format!("value-{i:08}")
}

fn next_key(state: &mut u64, entries: u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1);
    *state % entries
}
