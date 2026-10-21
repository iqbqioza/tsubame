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
fn named_databases_are_independent() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let first = env.create_database::<Str, Str>(&mut wtxn, Some("first"))?;
        let second = env.create_database::<Str, Str>(&mut wtxn, Some("second"))?;
        let unnamed = env.create_database::<Str, Str>(&mut wtxn, None)?;

        first.put(&mut wtxn, "key", "first-value")?;
        second.put(&mut wtxn, "key", "second-value")?;
        unnamed.put(&mut wtxn, "key", "unnamed-value")?;
        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        assert_eq!(first.get(&rtxn, "key")?, Some("first-value"));
        assert_eq!(second.get(&rtxn, "key")?, Some("second-value"));
        assert_eq!(unnamed.get(&rtxn, "key")?, Some("unnamed-value"));
        assert_eq!(env.info().database_count, 2);
    }

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let first = env.open_database::<Str, Str>(&rtxn, Some("first"))?;
    let second = env.open_database::<Str, Str>(&rtxn, Some("second"))?;
    let unnamed = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(first.get(&rtxn, "key")?, Some("first-value"));
    assert_eq!(second.get(&rtxn, "key")?, Some("second-value"));
    assert_eq!(unnamed.get(&rtxn, "key")?, Some("unnamed-value"));
    assert!(
        env.open_database::<Str, Str>(&rtxn, Some("missing"))
            .is_err()
    );

    Ok(())
}

#[test]
fn named_database_delete_and_clear() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, Some("named"))?;
    db.put(&mut wtxn, "a", "1")?;
    db.put(&mut wtxn, "b", "2")?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    db.delete(&mut wtxn, "a")?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "a")?, None);
    assert_eq!(db.get(&rtxn, "b")?, Some("2"));

    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "b")?, None);
    assert_eq!(
        env.open_database::<Str, Str>(&rtxn, Some("named"))?
            .get(&rtxn, "a")?,
        None
    );

    Ok(())
}

#[test]
fn iterates_entries_in_key_order() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, Some("ordered"))?;
    for key in ["d", "a", "c", "b", "e"] {
        db.put(&mut wtxn, key, key)?;
    }
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, Some("ordered"))?;
    let mut seen = Vec::new();
    for item in db.iter(&rtxn)? {
        let (key, value) = item?;
        seen.push((key.to_owned(), value.to_owned()));
    }

    assert_eq!(
        seen,
        [
            ("a".to_owned(), "a".to_owned()),
            ("b".to_owned(), "b".to_owned()),
            ("c".to_owned(), "c".to_owned()),
            ("d".to_owned(), "d".to_owned()),
            ("e".to_owned(), "e".to_owned()),
        ]
    );

    Ok(())
}

#[test]
fn iterates_many_leaves_in_order() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..1_000u32 {
        let key = format!("key-{i:04}");
        let value = format!("value-{i:04}");
        db.put(&mut wtxn, &key, &value)?;
    }
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let mut expected = 0u32;
    for item in db.iter(&rtxn)? {
        let (key, value) = item?;
        assert_eq!(key, format!("key-{expected:04}"));
        assert_eq!(value, format!("value-{expected:04}"));
        expected += 1;
    }
    assert_eq!(expected, 1_000);

    Ok(())
}

#[test]
fn empty_database_iterates_zero_entries() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.iter(&rtxn)?.count(), 0);

    Ok(())
}

#[test]
fn iteration_keeps_the_snapshot() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", "old")?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let mut iter = db.iter(&rtxn)?;

    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, "key", "new")?;
    wtxn.commit()?;

    let (key, value) = iter.next().unwrap()?;
    assert_eq!((key, value), ("key", "old"));
    assert!(iter.next().is_none());

    Ok(())
}

#[test]
fn max_dbs_is_enforced() -> Result<()> {
    let dir = tempdir()?;
    let mut options = options();
    options.max_dbs(1);
    let env = options.open(dir.path())?;
    let mut wtxn = env.write_txn()?;

    env.create_database::<Str, Str>(&mut wtxn, Some("first"))?;
    let error = env
        .create_database::<Str, Str>(&mut wtxn, Some("second"))
        .unwrap_err();

    assert!(error.to_string().contains("maximum number"));
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
