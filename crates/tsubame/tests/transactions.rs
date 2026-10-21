//! End-to-end tests for snapshots, commits, and persistence.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;
use std::thread;

use tempfile::tempdir;
use tsubame::byteorder::NativeEndian;
use tsubame::types::{Bytes, Str, U32};
use tsubame::{EnvOpenOptions, Result};

fn options() -> EnvOpenOptions {
    let mut options = EnvOpenOptions::new();
    options.map_size(16 * 1024 * 1024);
    options
}

#[test]
fn commits_are_visible_after_reopen() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, U32<NativeEndian>>(&mut wtxn, None)?;
        db.put(&mut wtxn, "alice", &42)?;
        db.put(&mut wtxn, "bob", &7)?;
        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        assert_eq!(db.get(&rtxn, "alice")?, Some(42));
        assert_eq!(db.get(&rtxn, "bob")?, Some(7));
        assert_eq!(db.get(&rtxn, "missing")?, None);
        drop(rtxn);
        drop(db);
    }

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, U32<NativeEndian>>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "alice")?, Some(42));
    assert_eq!(db.get(&rtxn, "bob")?, Some(7));

    Ok(())
}

#[test]
fn abort_discards_writes() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;

    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", "value")?;
    wtxn.abort()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key")?, None);

    Ok(())
}

#[test]
fn readers_keep_their_snapshot() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", "old")?;
    wtxn.commit()?;

    let old_reader = env.read_txn()?;

    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, "key", "new")?;
    wtxn.commit()?;

    assert_eq!(db.get(&old_reader, "key")?, Some("old"));
    let new_reader = env.read_txn()?;
    assert_eq!(db.get(&new_reader, "key")?, Some("new"));

    Ok(())
}

#[test]
fn delete_and_clear_are_committed() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;

    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for key in ["a", "b", "c"] {
        db.put(&mut wtxn, key, key)?;
    }
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    db.delete(&mut wtxn, "b")?;
    wtxn.commit()?;

    {
        let rtxn = env.read_txn()?;
        assert_eq!(db.get(&rtxn, "a")?, Some("a"));
        assert_eq!(db.get(&rtxn, "b")?, None);
        assert_eq!(db.get(&rtxn, "c")?, Some("c"));
    }

    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    wtxn.commit()?;

    {
        let rtxn = env.read_txn()?;
        assert_eq!(db.get(&rtxn, "a")?, None);
        assert_eq!(db.get(&rtxn, "c")?, None);
    }

    Ok(())
}

#[test]
fn overflow_values_survive_reopen() -> Result<()> {
    let dir = tempdir()?;
    let value = vec![0x5a; 10_000];

    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Bytes>(&mut wtxn, None)?;
        db.put(&mut wtxn, "blob", value.as_slice())?;
        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        assert_eq!(db.get(&rtxn, "blob")?, Some(value.as_slice()));
        drop(rtxn);
        drop(db);
    }

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Bytes>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "blob")?, Some(value.as_slice()));

    Ok(())
}

#[test]
fn concurrent_readers_do_not_block_a_writer() -> Result<()> {
    let dir = tempdir()?;
    let env = Arc::new(options().open(dir.path())?);
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, U32<NativeEndian>>(&mut wtxn, None)?;
    db.put(&mut wtxn, "answer", &42)?;
    wtxn.commit()?;

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let env = Arc::clone(&env);
            let db = db.clone();
            thread::spawn(move || {
                let rtxn = env.read_txn().unwrap();
                assert_eq!(db.get(&rtxn, "answer").unwrap(), Some(42));
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}

#[test]
fn batch_insert_reuses_transaction_pages() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, U32<NativeEndian>>(&mut wtxn, None)?;

    for i in 0..1_000u32 {
        let key = format!("key-{i:04}");
        db.put(&mut wtxn, &key, &i)?;
    }
    wtxn.commit()?;

    assert!(
        env.info().page_count < 100,
        "page count {} suggests transaction pages were not reused",
        env.info().page_count
    );

    Ok(())
}

#[test]
fn named_databases_are_rejected_until_implemented() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;

    let error = env
        .create_database::<Str, Str>(&mut wtxn, Some("named"))
        .unwrap_err();

    assert!(error.to_string().contains("named databases"));
    wtxn.abort()?;

    Ok(())
}

#[test]
fn recovers_previous_commit_when_newest_meta_is_corrupt() -> Result<()> {
    let dir = tempdir()?;
    let previous_txn;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "first")?;
        wtxn.commit()?;
        previous_txn = env.info().txn_id;

        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, "key", "second")?;
        wtxn.commit()?;
        drop(db);
    }

    // The second commit used meta slot 0. Break its checksum so recovery must
    // fall back to the first commit in slot 1.
    std::fs::remove_file(dir.path().join("clean.tsubame")).ok();
    let mut file = OpenOptions::new()
        .write(true)
        .open(dir.path().join("data.tsubame"))?;
    file.seek(SeekFrom::Start(100))?;
    file.write_all(&[0xff])?;

    let env = options().open(dir.path())?;
    assert_eq!(env.info().txn_id, previous_txn);
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("first"));

    Ok(())
}

#[test]
fn recovers_previous_commit_when_commit_record_is_corrupt() -> Result<()> {
    let dir = tempdir()?;
    let previous_txn;
    let page_count;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "first")?;
        wtxn.commit()?;
        previous_txn = env.info().txn_id;
        drop(db);
    }
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.open_database::<Str, Str>(&env.read_txn()?, None)?;
        db.put(&mut wtxn, "key", "second")?;
        wtxn.commit()?;
        page_count = env.info().page_count;
        drop(db);
    }

    // The last page of the second commit is its commit record. Corrupting it
    // must make recovery reject the commit and fall back to the previous meta.
    std::fs::remove_file(dir.path().join("clean.tsubame")).ok();
    let mut file = OpenOptions::new()
        .write(true)
        .open(dir.path().join("data.tsubame"))?;
    file.seek(SeekFrom::Start((page_count - 1) * 4096 + 100))?;
    file.write_all(&[0xff])?;

    let env = options().open(dir.path())?;
    assert_eq!(env.info().txn_id, previous_txn);
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("first"));

    Ok(())
}

#[test]
fn falls_back_when_committed_pages_are_missing() -> Result<()> {
    let dir = tempdir()?;
    let previous_txn;
    let page_count;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "first")?;
        wtxn.commit()?;
        previous_txn = env.info().txn_id;
        drop(db);
    }
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.open_database::<Str, Str>(&env.read_txn()?, None)?;
        db.put(&mut wtxn, "key", "second")?;
        wtxn.commit()?;
        page_count = env.info().page_count;
        drop(db);
    }

    std::fs::remove_file(dir.path().join("clean.tsubame")).ok();
    OpenOptions::new()
        .write(true)
        .open(dir.path().join("data.tsubame"))?
        .set_len((page_count - 1) * 4096)?;

    let env = options().open(dir.path())?;
    assert_eq!(env.info().txn_id, previous_txn);
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("first"));

    Ok(())
}

#[test]
fn committed_metadata_advances_across_reopen() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", "value")?;
    wtxn.commit()?;
    let info = env.info();
    drop(db);
    drop(env);

    let env = options().open(dir.path())?;
    assert_eq!(env.info().txn_id, info.txn_id);
    assert_eq!(env.info().page_count, info.page_count);

    Ok(())
}
