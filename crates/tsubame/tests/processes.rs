//! Multi-process integration tests.
//!
//! These tests spawn child processes. Keep them in a separate test binary and
//! run them one at a time: on Unix a forked child briefly inherits the parent's
//! open file descriptors until `exec`, so spawning while another test holds an
//! environment lock can make that lock appear transiently held.

use std::sync::{Mutex, MutexGuard};
use std::thread;

use anyhow::Context;
use tempfile::tempdir;
use tsubame::types::Str;
use tsubame::{Env, EnvOpenOptions, Result};

fn options() -> EnvOpenOptions {
    let mut options = EnvOpenOptions::new();
    options.map_size(16 * 1024 * 1024);
    options
}

static PROCESS_TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialize_process_test() -> MutexGuard<'static, ()> {
    PROCESS_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn multi_process_writer_coexists_with_read_only_process() -> Result<()> {
    let _serial = serialize_process_test();
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "initial")?;
        wtxn.commit()?;
    }
    std::fs::remove_file(dir.path().join("child-ready")).ok();
    std::fs::remove_file(dir.path().join("child-release")).ok();

    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "read_only_child_process_helper", "--nocapture"])
        .env("TSUBAME_READ_ONLY_CHILD", dir.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let ready = dir.path().join("child-ready");
    for _ in 0..500 {
        if ready.is_file() {
            break;
        }
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("read-only child exited before becoming ready: {status}");
        }
        thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(ready.is_file(), "read-only child did not become ready");

    let mut writer_options = options();
    writer_options.multi_process(true);
    let writer = writer_options
        .open(dir.path())
        .context("opening multi writer")?;
    let db = writer.open_database::<Str, Str>(&writer.read_txn()?, None)?;
    let mut wtxn = writer.write_txn()?;
    db.put(&mut wtxn, "key", "updated")?;
    wtxn.commit()?;
    assert!(Env::compact(dir.path()).is_err());

    std::fs::write(dir.path().join("child-release"), b"release")?;
    assert!(child.wait()?.success());
    drop(db);
    drop(writer);

    let env = options()
        .open(dir.path())
        .context("opening final writer after multi writer")?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("updated"));

    Ok(())
}

#[test]
fn multi_process_page_reuse_respects_cross_process_reader() -> Result<()> {
    let _serial = serialize_process_test();
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "initial")?;
        for i in 0..500u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), "initial")?;
        }
        wtxn.commit()?;
    }
    std::fs::remove_file(dir.path().join("child-ready")).ok();
    std::fs::remove_file(dir.path().join("child-release")).ok();

    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "read_only_child_process_helper", "--nocapture"])
        .env("TSUBAME_READ_ONLY_CHILD", dir.path())
        .env("TSUBAME_READ_ONLY_CHILD_TXN", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let ready = dir.path().join("child-ready");
    for _ in 0..500 {
        if ready.is_file() {
            break;
        }
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("read-only child exited before becoming ready: {status}");
        }
        thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(ready.is_file(), "read-only child did not become ready");

    let mut writer_options = options();
    writer_options.multi_process(true);
    let writer = writer_options.open(dir.path())?;
    let mut wtxn = writer.write_txn()?;
    let db = writer.create_database::<Str, Str>(&mut wtxn, None)?;
    for round in 0..30u32 {
        for i in 0..500u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), &format!("round-{round}"))?;
        }
        db.put(&mut wtxn, "key", &format!("round-{round}"))?;
        wtxn.commit()?;
        wtxn = writer.write_txn()?;
    }
    wtxn.abort()?;

    std::fs::write(dir.path().join("child-release"), b"release")?;
    assert!(child.wait()?.success());
    drop(db);
    drop(writer);

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("round-29"));
    assert_eq!(db.get(&rtxn, "key-0499")?, Some("round-29"));

    Ok(())
}

#[test]
fn read_only_child_process_helper() -> Result<()> {
    let Ok(path) = std::env::var("TSUBAME_READ_ONLY_CHILD") else {
        return Ok(());
    };
    let dir = std::path::PathBuf::from(path);
    let mut options = options();
    options.read_only(true);
    let env = options.open(&dir)?;
    let hold_txn = std::env::var_os("TSUBAME_READ_ONLY_CHILD_TXN").is_some();
    let held = if hold_txn {
        let rtxn = env.read_txn()?;
        let db = env.open_database::<Str, Str>(&rtxn, None)?;
        assert_eq!(db.get(&rtxn, "key")?, Some("initial"));
        for i in 0..500u32 {
            assert_eq!(db.get(&rtxn, &format!("key-{i:04}"))?, Some("initial"));
        }
        Some((rtxn, db))
    } else {
        None
    };
    std::fs::write(dir.join("child-ready"), b"ready")?;

    for _ in 0..500 {
        if dir.join("child-release").is_file() {
            if let Some((rtxn, db)) = &held {
                assert_eq!(db.get(rtxn, "key")?, Some("initial"));
                for i in 0..500u32 {
                    assert_eq!(db.get(rtxn, &format!("key-{i:04}"))?, Some("initial"));
                }
            }
            return Ok(());
        }
        thread::sleep(std::time::Duration::from_millis(10));
    }

    anyhow::bail!("parent did not release the read-only child")
}

#[test]
fn read_only_environments_share_across_processes() -> Result<()> {
    let _serial = serialize_process_test();
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "value")?;
        wtxn.commit()?;
    }
    std::fs::remove_file(dir.path().join("child-ready")).ok();
    std::fs::remove_file(dir.path().join("child-release")).ok();

    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "read_only_child_process_helper", "--nocapture"])
        .env("TSUBAME_READ_ONLY_CHILD", dir.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    let ready = dir.path().join("child-ready");
    for _ in 0..500 {
        if ready.is_file() {
            break;
        }
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("read-only child exited before becoming ready: {status}");
        }
        thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(ready.is_file(), "read-only child did not become ready");

    let mut read_options = options();
    read_options.read_only(true);
    let reader = read_options.open(dir.path())?;
    assert!(options().open(dir.path()).is_err());
    assert!(reader.write_txn().is_err());

    std::fs::write(dir.path().join("child-release"), b"release")?;
    assert!(child.wait()?.success());
    drop(reader);

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("value"));

    Ok(())
}
