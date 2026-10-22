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
    repeats: u32,
}

fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(|| Config {
        entries: env_u64("TSUBAME_BENCH_ENTRIES", 100_000),
        reads: env_u64("TSUBAME_BENCH_READS", 1_000_000),
        commits: env_u64("TSUBAME_BENCH_COMMITS", 1_000),
        warmup_reads: env_u64("TSUBAME_BENCH_WARMUP", 10_000),
        repeats: u32::try_from(env_u64("TSUBAME_BENCH_REPEATS", 5))
            .unwrap_or(3)
            .max(1),
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
type TsubamePrefixDb = tsubame::Database<TsubameStr, TsubameStr>;
type HeedPrefixDb = heed::Database<HeedStr, HeedStr>;
type TsubameMixedDb = tsubame::Database<TsubameStr, TsubameStr>;
type HeedMixedDb = heed::Database<HeedStr, HeedStr>;

#[derive(Clone, Copy)]
struct Report {
    insert: Duration,
    read: Duration,
    range: Duration,
    reverse: Duration,
    prefix: Duration,
    commit: Duration,
    delete: Duration,
    mixed: Duration,
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
                    "tsubame commit median: {:.2} us/op",
                    duration.as_secs_f64() * 1_000_000.0
                );
                return Ok(());
            }
            "heed-commit" => {
                let duration = bench_heed_commit(dir.path())?;
                println!(
                    "heed commit median: {:.2} us/op",
                    duration.as_secs_f64() * 1_000_000.0
                );
                return Ok(());
            }
            "tsubame-mixed" => {
                let duration = bench_tsubame_mixed()?;
                println!("tsubame mixed: {:.3} ms", duration.as_secs_f64() * 1_000.0);
                return Ok(());
            }
            "heed-mixed" => {
                let duration = bench_heed_mixed()?;
                println!("heed mixed: {:.3} ms", duration.as_secs_f64() * 1_000.0);
                return Ok(());
            }
            "tsubame-large" => {
                let (insert, read) = bench_tsubame_large()?;
                println!(
                    "tsubame large insert: {:.3} ms read: {:.2} ns/op",
                    insert.as_secs_f64() * 1_000.0,
                    read.as_secs_f64() * 1_000_000_000.0 / LARGE_ENTRIES as f64
                );
                return Ok(());
            }
            "heed-large" => {
                let (insert, read) = bench_heed_large()?;
                println!(
                    "heed large insert: {:.3} ms read: {:.2} ns/op",
                    insert.as_secs_f64() * 1_000.0,
                    read.as_secs_f64() * 1_000_000_000.0 / LARGE_ENTRIES as f64
                );
                return Ok(());
            }
            "tsubame-prefix" => {
                let duration = bench_tsubame_prefix()?;
                println!(
                    "tsubame prefix: {:.2} ns/op",
                    duration.as_secs_f64() * 1_000_000_000.0 / (config().entries / 10) as f64
                );
                return Ok(());
            }
            "heed-prefix" => {
                let duration = bench_heed_prefix()?;
                println!(
                    "heed prefix: {:.2} ns/op",
                    duration.as_secs_f64() * 1_000_000_000.0 / (config().entries / 10) as f64
                );
                return Ok(());
            }
            _ => {}
        }
    }

    let config = config();
    println!(
        "dataset: {} entries, {} point reads, {} commits, {} repeats, map {MAP_SIZE} bytes",
        config.entries, config.reads, config.commits, config.repeats
    );
    let mut tsubame_runs = Vec::with_capacity(config.repeats as usize);
    let mut heed_runs = Vec::with_capacity(config.repeats as usize);
    for _ in 0..config.repeats {
        tsubame_runs.push(run_tsubame()?);
        heed_runs.push(run_heed()?);
    }
    let tsubame = median_report(&tsubame_runs);
    let heed = median_report(&heed_runs);

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
        "reverse scan",
        tsubame.reverse,
        heed.reverse,
        config.entries,
        "ns/op",
    );
    print_row(
        "prefix scan",
        tsubame.prefix,
        heed.prefix,
        config.entries / 10,
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
        "durable commit (median)",
        tsubame.commit,
        heed.commit,
        1,
        "us/op",
    );
    print_row("bulk delete (1 txn)", tsubame.delete, heed.delete, 1, "ms");
    print_row(
        "mixed updates+deletes (1 txn)",
        tsubame.mixed,
        heed.mixed,
        1,
        "ms",
    );
    print_row("reopen", tsubame.reopen, heed.reopen, 1, "ms");
    print_row(
        "startup writes (1000)",
        tsubame.first_write,
        heed.first_write,
        1000,
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

fn median_report(reports: &[Report]) -> Report {
    fn median(mut values: Vec<Duration>) -> Duration {
        values.sort_unstable();
        values[values.len() / 2]
    }
    Report {
        insert: median(reports.iter().map(|report| report.insert).collect()),
        read: median(reports.iter().map(|report| report.read).collect()),
        range: median(reports.iter().map(|report| report.range).collect()),
        reverse: median(reports.iter().map(|report| report.reverse).collect()),
        prefix: median(reports.iter().map(|report| report.prefix).collect()),
        commit: median(reports.iter().map(|report| report.commit).collect()),
        delete: median(reports.iter().map(|report| report.delete).collect()),
        mixed: median(reports.iter().map(|report| report.mixed).collect()),
        first_write: median(reports.iter().map(|report| report.first_write).collect()),
        large_insert: median(reports.iter().map(|report| report.large_insert).collect()),
        large_read: median(reports.iter().map(|report| report.large_read).collect()),
        reopen: median(reports.iter().map(|report| report.reopen).collect()),
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

    let start = Instant::now();
    let mut scanned = 0u64;
    for item in db.rev_iter(&rtxn)? {
        let (key, value) = item?;
        black_box((key, value));
        scanned += 1;
    }
    black_box(scanned);
    let reverse = start.elapsed();

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
    drop(db);
    drop(env);

    let (large_insert, large_read) = bench_tsubame_large()?;
    let prefix = bench_tsubame_prefix()?;
    let commit = bench_tsubame_commit(dir.path(), &mut options)?;
    let delete = bench_tsubame_delete(dir.path(), &mut options)?;
    let mixed = bench_tsubame_mixed()?;
    let first_write = bench_tsubame_startup(dir.path(), &mut options)?;

    Ok(Report {
        insert,
        read,
        range,
        reverse,
        prefix,
        commit,
        delete,
        mixed,
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

    let mut samples = Vec::with_capacity(config.commits as usize);
    for i in 0..config.commits {
        let start = Instant::now();
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, &i, "value")?;
        wtxn.commit()?;
        samples.push(start.elapsed());
    }
    samples.sort_unstable();
    Ok(samples[samples.len() / 2])
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

fn bench_tsubame_mixed() -> Result<Duration> {
    let config = config();
    let updates = (config.entries / 5).max(1);
    let deletes = (config.entries / 5).max(1);
    let dir = tempdir()?;
    let mut options = TsubameOptions::new();
    options.map_size(MAP_SIZE);
    let env = options.open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db: TsubameMixedDb = env.create_database(&mut wtxn, None)?;
    for i in 0..config.entries {
        db.put(&mut wtxn, &format!("key-{i:06}"), &format!("value-{i:08}"))?;
    }
    wtxn.commit()?;

    let keys: Vec<String> = (0..config.entries).map(|i| format!("key-{i:06}")).collect();
    let updated_values: Vec<String> = (0..updates)
        .map(|i| format!("updated-{i:08}-{}", "x".repeat(64)))
        .collect();
    let delete_start = config.entries - deletes;

    let start = Instant::now();
    let mut wtxn = env.write_txn()?;
    for i in 0..updates {
        db.put(
            &mut wtxn,
            keys[i as usize].as_str(),
            updated_values[i as usize].as_str(),
        )?;
    }
    for i in delete_start..config.entries {
        db.delete(&mut wtxn, keys[i as usize].as_str())?;
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

    let start = Instant::now();
    let mut scanned = 0u64;
    for item in db.rev_iter(&rtxn)? {
        let (key, value) = item?;
        black_box((key, value));
        scanned += 1;
    }
    black_box(scanned);
    let reverse = start.elapsed();

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
    drop(env);

    let (large_insert, large_read) = bench_heed_large()?;
    let prefix = bench_heed_prefix()?;
    let commit = bench_heed_commit(dir.path())?;
    let delete = bench_heed_delete(dir.path())?;
    let mixed = bench_heed_mixed()?;
    let first_write = bench_heed_startup(dir.path())?;

    Ok(Report {
        insert,
        read,
        range,
        reverse,
        prefix,
        commit,
        delete,
        mixed,
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

    let mut samples = Vec::with_capacity(config.commits as usize);
    for i in 0..config.commits {
        let start = Instant::now();
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, &i, "value")?;
        wtxn.commit()?;
        samples.push(start.elapsed());
    }
    samples.sort_unstable();
    Ok(samples[samples.len() / 2])
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

fn bench_tsubame_startup(path: &std::path::Path, options: &mut TsubameOptions) -> Result<Duration> {
    let env = options.open(path)?;
    let rtxn = env.read_txn()?;
    let db: TsubameDb = env.open_database(&rtxn, None)?;
    drop(rtxn);
    let start = Instant::now();
    for i in 0..1000 {
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, &i, "startup-write")?;
        wtxn.commit()?;
    }
    Ok(start.elapsed())
}

fn bench_heed_mixed() -> Result<Duration> {
    let config = config();
    let updates = (config.entries / 5).max(1);
    let deletes = (config.entries / 5).max(1);
    let dir = tempdir()?;
    // SAFETY: this benchmark owns the directory.
    let env = unsafe {
        HeedOptions::new()
            .map_size(MAP_SIZE)
            .max_dbs(1)
            .open(dir.path())?
    };
    let mut wtxn = env.write_txn()?;
    let db: HeedMixedDb = env.create_database(&mut wtxn, None)?;
    for i in 0..config.entries {
        db.put(&mut wtxn, &format!("key-{i:06}"), &format!("value-{i:08}"))?;
    }
    wtxn.commit()?;

    let keys: Vec<String> = (0..config.entries).map(|i| format!("key-{i:06}")).collect();
    let updated_values: Vec<String> = (0..updates)
        .map(|i| format!("updated-{i:08}-{}", "x".repeat(64)))
        .collect();
    let delete_start = config.entries - deletes;

    let start = Instant::now();
    let mut wtxn = env.write_txn()?;
    for i in 0..updates {
        db.put(
            &mut wtxn,
            keys[i as usize].as_str(),
            updated_values[i as usize].as_str(),
        )?;
    }
    for i in delete_start..config.entries {
        db.delete(&mut wtxn, keys[i as usize].as_str())?;
    }
    wtxn.commit()?;
    Ok(start.elapsed())
}

fn bench_heed_startup(path: &std::path::Path) -> Result<Duration> {
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
    for i in 0..1000 {
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, &i, "startup-write")?;
        wtxn.commit()?;
    }
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

fn bench_tsubame_prefix() -> Result<Duration> {
    let dir = tempdir()?;
    let mut options = TsubameOptions::new();
    options.map_size(MAP_SIZE);
    let env = options.open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db: TsubamePrefixDb = env.create_database(&mut wtxn, None)?;
    for i in 0..100_000u64 {
        db.put(&mut wtxn, &format!("key-{i:06}"), &format!("value-{i:08}"))?;
    }
    wtxn.commit()?;
    let rtxn = env.read_txn()?;
    let start = Instant::now();
    let mut count = 0u64;
    for item in db.prefix_iter(&rtxn, "key-05")? {
        black_box(item?);
        count += 1;
    }
    black_box(count);
    Ok(start.elapsed())
}

fn bench_heed_prefix() -> Result<Duration> {
    let dir = tempdir()?;
    // SAFETY: this benchmark owns the directory.
    let env = unsafe {
        HeedOptions::new()
            .map_size(MAP_SIZE)
            .max_dbs(1)
            .open(dir.path())?
    };
    let mut wtxn = env.write_txn()?;
    let db: HeedPrefixDb = env.create_database(&mut wtxn, None)?;
    for i in 0..100_000u64 {
        db.put(&mut wtxn, &format!("key-{i:06}"), &format!("value-{i:08}"))?;
    }
    wtxn.commit()?;
    let rtxn = env.read_txn()?;
    let start = Instant::now();
    let mut count = 0u64;
    for item in db.prefix_iter(&rtxn, "key-05")? {
        black_box(item?);
        count += 1;
    }
    black_box(count);
    Ok(start.elapsed())
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
