//! End-to-end tests for snapshots, commits, and persistence.

use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::ops::Bound;
use std::sync::Arc;
use std::thread;

use anyhow::Context;
use tempfile::tempdir;
use tsubame::byteorder::NativeEndian;
use tsubame::types::{Bytes, Str, U32};
use tsubame::{Env, EnvOpenOptions, KeyComparator, Result};

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
fn copy_to_path_creates_a_consistent_backup() -> Result<()> {
    let dir = tempdir()?;
    let backup = dir.path().join("backup");
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..1_000u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), &format!("value-{i:04}"))?;
    }
    wtxn.commit()?;

    let active = env.write_txn()?;
    assert!(env.copy_to_path(&backup).is_err());
    drop(active);
    env.copy_to_path(&backup)?;
    assert!(env.copy_to_path(&backup).is_err());

    let mut read_only = options();
    read_only.read_only(true);
    let restored = read_only.open(&backup)?;
    let rtxn = restored.read_txn()?;
    let db = restored.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key-0000")?, Some("value-0000"));
    assert_eq!(db.get(&rtxn, "key-0999")?, Some("value-0999"));

    Ok(())
}

#[test]
fn database_stat_reports_tree_structure() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Bytes>(&mut wtxn, None)?;
    assert_eq!(db.stat(&env.read_txn()?)?.entries, 0);
    for i in 0..1_000u32 {
        db.put(
            &mut wtxn,
            &format!("key-{i:04}"),
            format!("value-{i}").as_bytes(),
        )?;
    }
    db.put(&mut wtxn, "large", vec![0x5a; 10_000].as_slice())?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let stat = db.stat(&rtxn)?;
    assert_eq!(stat.page_size, 4096);
    assert_eq!(stat.entries, 1_001);
    assert!(stat.depth >= 2, "unexpected depth {}", stat.depth);
    assert!(stat.leaf_pages > 0);
    assert!(stat.branch_pages > 0);
    assert!(stat.overflow_pages >= 3);

    Ok(())
}

#[test]
fn directional_lookups_follow_key_order() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for key in ["b", "d", "f"] {
        db.put(&mut wtxn, key, key)?;
    }
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.get_greater_than(&rtxn, "b")?, Some(("d", "d")));
    assert_eq!(
        db.get_greater_than_or_equal_to(&rtxn, "b")?,
        Some(("b", "b"))
    );
    assert_eq!(db.get_lower_than(&rtxn, "d")?, Some(("b", "b")));
    assert_eq!(db.get_lower_than_or_equal_to(&rtxn, "d")?, Some(("d", "d")));
    assert_eq!(db.get_greater_than(&rtxn, "f")?, None);
    assert_eq!(db.get_lower_than(&rtxn, "b")?, None);

    Ok(())
}

#[test]
fn delete_range_respects_bounds_and_staged_changes() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for key in ["a", "b", "c", "d", "e"] {
        db.put(&mut wtxn, key, key)?;
    }
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, "d", "updated")?;
    db.delete(&mut wtxn, "c")?;
    let count = db.delete_range(&mut wtxn, &(Bound::Included("b"), Bound::Excluded("e")))?;
    assert_eq!(count, 2);
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let entries = db.iter(&rtxn)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(entries, vec![("a", "a"), ("e", "e")]);
    drop(rtxn);

    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, "f", "f")?;
    let count = db.delete_range(&mut wtxn, &(Bound::Unbounded, Bound::Unbounded))?;
    assert_eq!(count, 3);
    assert!(db.is_empty_rw(&wtxn)?);
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert!(db.is_empty(&rtxn)?);

    Ok(())
}

#[test]
fn heed_style_mut_aliases_reflect_staged_changes() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for key in ["a", "b", "c"] {
        db.put(&mut wtxn, key, key)?;
    }
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, "d", "d")?;
    db.put(&mut wtxn, "e", "e")?;

    let all = db.iter_mut(&mut wtxn)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(
        all,
        vec![("a", "a"), ("b", "b"), ("c", "c"), ("d", "d"), ("e", "e")]
    );

    let range = db
        .range_mut(&mut wtxn, Bound::Included("b"), Bound::Excluded("e"))?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(range, vec![("b", "b"), ("c", "c"), ("d", "d")]);

    let rev = db.rev_iter_mut(&mut wtxn)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(
        rev,
        vec![("e", "e"), ("d", "d"), ("c", "c"), ("b", "b"), ("a", "a")]
    );

    let rev_range = db
        .rev_range_mut(&mut wtxn, Bound::Included("b"), Bound::Included("d"))?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rev_range, vec![("d", "d"), ("c", "c"), ("b", "b")]);

    let prefix = db
        .prefix_iter_mut(&mut wtxn, "c")?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(prefix, vec![("c", "c")]);

    let rev_prefix = db
        .rev_prefix_iter_mut(&mut wtxn, "c")?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rev_prefix, vec![("c", "c")]);

    Ok(())
}

#[test]
fn force_sync_waits_for_writers() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", "value")?;
    wtxn.commit()?;

    env.force_sync()?;
    let wtxn = env.write_txn()?;
    assert!(env.force_sync().is_err());
    drop(wtxn);
    env.force_sync()?;
    drop(db);
    drop(env);

    let mut read_options = options();
    read_options.read_only(true);
    let read_only = read_options.open(dir.path())?;
    read_only.force_sync()?;

    Ok(())
}

#[test]
fn database_inspection_helpers_match_the_visible_snapshot() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "b", "2")?;
    db.put(&mut wtxn, "a", "1")?;
    db.put(&mut wtxn, "c", "3")?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.first(&rtxn)?, Some(("a", "1")));
    assert_eq!(db.last(&rtxn)?, Some(("c", "3")));
    assert_eq!(db.len(&rtxn)?, 3);
    assert!(!db.is_empty(&rtxn)?);
    drop(rtxn);

    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, "d", "4")?;
    db.delete(&mut wtxn, "b")?;
    assert_eq!(db.first_rw(&wtxn)?, Some(("a", "1")));
    assert_eq!(db.last_rw(&wtxn)?, Some(("d", "4")));
    assert_eq!(db.len_rw(&wtxn)?, 3);
    assert!(!db.is_empty_rw(&wtxn)?);
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.first(&rtxn)?, Some(("a", "1")));
    assert_eq!(db.last(&rtxn)?, Some(("d", "4")));
    assert_eq!(db.len(&rtxn)?, 3);
    drop(rtxn);

    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    assert!(db.is_empty_rw(&wtxn)?);
    assert_eq!(db.len_rw(&wtxn)?, 0);
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert!(db.is_empty(&rtxn)?);
    assert_eq!(db.first(&rtxn)?, None);
    assert_eq!(db.last(&rtxn)?, None);

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
fn staged_large_values_are_readable_and_reused_in_one_transaction() -> Result<()> {
    let dir = tempdir()?;
    let first = vec![0x11; 16_000];
    let second = vec![0x22; 20_000];
    let env = options().open(dir.path())?;

    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Bytes>(&mut wtxn, None)?;
    db.put(&mut wtxn, "large", first.as_slice())?;
    assert_eq!(db.get_rw(&wtxn, "large")?, Some(first.as_slice()));
    let staged = db.iter_rw(&wtxn)?.next().transpose()?.unwrap();
    assert_eq!(staged.1, first.as_slice());

    db.put(&mut wtxn, "large", second.as_slice())?;
    assert_eq!(db.get_rw(&wtxn, "large")?, Some(second.as_slice()));
    wtxn.commit()?;

    let base_pages = env.info().page_count;
    let value = vec![0x33; 16_000];
    let mut wtxn = env.write_txn()?;
    let db = env.open_database::<Str, Bytes>(&env.read_txn()?, None)?;
    for _ in 0..32 {
        db.put(&mut wtxn, "large", value.as_slice())?;
    }
    wtxn.commit()?;
    assert!(
        env.info().page_count <= base_pages + 32,
        "staged large values leaked pages: {base_pages} -> {}",
        env.info().page_count
    );

    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Bytes>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "large")?, Some(value.as_slice()));

    Ok(())
}

#[test]
fn unordered_staged_large_overwrites_do_not_leak_pages() -> Result<()> {
    let dir = tempdir()?;
    let value = vec![0x44; 16_000];
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Bytes>(&mut wtxn, None)?;
    wtxn.commit()?;
    let before = env.info().page_count;

    let mut wtxn = env.write_txn()?;
    for _ in 0..1_100 {
        db.put(&mut wtxn, "large", value.as_slice())?;
    }
    wtxn.commit()?;
    assert!(
        env.info().page_count <= before + 32,
        "unordered staged overwrites leaked pages: {before} -> {}",
        env.info().page_count
    );

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "large")?, Some(value.as_slice()));

    Ok(())
}

#[test]
fn single_page_overflow_extents_can_be_replaced_and_deleted() -> Result<()> {
    let dir = tempdir()?;
    let first = vec![0x11; 2_000];
    let second = vec![0x22; 2_500];
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Bytes>(&mut wtxn, None)?;
        db.put(&mut wtxn, "value", first.as_slice())?;
        wtxn.commit()?;

        let mut wtxn = env.write_txn()?;
        let db = env.open_database::<Str, Bytes>(&env.read_txn()?, None)?;
        db.put(&mut wtxn, "value", second.as_slice())?;
        wtxn.commit()?;
    }

    {
        let env = options().open(dir.path())?;
        let rtxn = env.read_txn()?;
        let db = env.open_database::<Str, Bytes>(&rtxn, None)?;
        assert_eq!(db.get(&rtxn, "value")?, Some(second.as_slice()));
        drop(rtxn);

        let mut wtxn = env.write_txn()?;
        let db = env.open_database::<Str, Bytes>(&env.read_txn()?, None)?;
        db.delete(&mut wtxn, "value")?;
        wtxn.commit()?;
    }

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Bytes>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "value")?, None);

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
fn pending_values_cross_the_inline_threshold() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Bytes>(&mut wtxn, None)?;
    for len in [0usize, 1, 31, 32, 33, 1_000] {
        let value = vec![b'x'; len];
        db.put(&mut wtxn, &format!("key-{len}"), value.as_slice())?;
    }
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    for len in [0usize, 1, 31, 32, 33, 1_000] {
        assert_eq!(
            db.get(&rtxn, &format!("key-{len}"))?,
            Some(vec![b'x'; len].as_slice())
        );
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
fn clear_reclaims_pages_and_abort_keeps_the_tree() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..1_000u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), "initial")?;
    }
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    for i in 0..1_000u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), "second")?;
    }
    wtxn.commit()?;
    let after_refill = env.info().page_count;

    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    for i in 0..1_000u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), "third")?;
    }
    wtxn.commit()?;

    assert!(
        env.info().page_count <= after_refill + 20,
        "clear/refill grew the file from {after_refill} to {}",
        env.info().page_count
    );

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key-0000")?, Some("third"));
    drop(rtxn);

    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    wtxn.abort()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key-0000")?, Some("third"));

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
fn ranges_respect_bounds() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for key in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        db.put(&mut wtxn, key, key)?;
    }
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let mut seen = Vec::new();
    for item in db.range(&rtxn, Bound::Included("c"), Bound::Excluded("g"))? {
        seen.push(item?.0.to_owned());
    }
    assert_eq!(seen, ["c", "d", "e", "f"]);

    seen.clear();
    for item in db.range(&rtxn, Bound::Excluded("c"), Bound::Included("f"))? {
        seen.push(item?.0.to_owned());
    }
    assert_eq!(seen, ["d", "e", "f"]);

    seen.clear();
    for item in db.range(&rtxn, Bound::Unbounded, Bound::Excluded("c"))? {
        seen.push(item?.0.to_owned());
    }
    assert_eq!(seen, ["a", "b"]);

    Ok(())
}

#[test]
fn prefix_iteration_filters_keys() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for key in ["a", "aa", "ab", "ac", "b", "ba"] {
        db.put(&mut wtxn, key, key)?;
    }
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let mut seen = Vec::new();
    for item in db.prefix_iter(&rtxn, "a")? {
        seen.push(item?.0.to_owned());
    }
    assert_eq!(seen, ["a", "aa", "ab", "ac"]);

    seen.clear();
    for item in db.rev_prefix_iter(&rtxn, "a")? {
        seen.push(item?.0.to_owned());
    }
    assert_eq!(seen, ["ac", "ab", "aa", "a"]);

    Ok(())
}

#[test]
fn reverse_iteration_and_ranges() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for key in ["a", "b", "c", "d", "e", "f"] {
        db.put(&mut wtxn, key, key)?;
    }
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let mut seen = Vec::new();
    for item in db.rev_iter(&rtxn)? {
        seen.push(item?.0.to_owned());
    }
    assert_eq!(seen, ["f", "e", "d", "c", "b", "a"]);

    seen.clear();
    for item in db.rev_range(&rtxn, Bound::Included("b"), Bound::Excluded("f"))? {
        seen.push(item?.0.to_owned());
    }
    assert_eq!(seen, ["e", "d", "c", "b"]);

    Ok(())
}

#[test]
fn reverse_iterates_many_leaves() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..1_000u32 {
        let key = format!("key-{i:04}");
        db.put(&mut wtxn, &key, &key)?;
    }
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let mut expected = 999i32;
    for item in db.rev_iter(&rtxn)? {
        let (key, value) = item?;
        assert_eq!(key, format!("key-{expected:04}"));
        assert_eq!(value, key);
        expected -= 1;
    }
    assert_eq!(expected, -1);

    Ok(())
}

#[cfg(feature = "serde-json")]
#[test]
fn serde_json_codec_round_trips_in_a_database() -> Result<()> {
    use tsubame::types::SerdeJson;

    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, SerdeJson<String>>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", &"value".to_owned())?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key")?, Some("value".to_owned()));

    Ok(())
}

#[cfg(feature = "serde-bincode")]
#[test]
fn serde_bincode_codec_round_trips_in_a_database() -> Result<()> {
    use tsubame::types::SerdeBincode;

    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, SerdeBincode<String>>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", &"value".to_owned())?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key")?, Some("value".to_owned()));

    Ok(())
}

#[test]
fn write_txn_iteration_includes_pending_changes() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, Some("pending"))?;
    db.put(&mut wtxn, "a", "old-a")?;
    db.put(&mut wtxn, "c", "old-c")?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let db = env.open_database::<Str, Str>(&env.read_txn()?, Some("pending"))?;
    db.put(&mut wtxn, "a", "new-a")?;
    db.put(&mut wtxn, "b", "new-b")?;
    db.delete(&mut wtxn, "c")?;

    let mut seen = Vec::new();
    for item in db.iter_rw(&wtxn)? {
        let (key, value) = item?;
        seen.push((key.to_owned(), value.to_owned()));
    }
    assert_eq!(
        seen,
        [
            ("a".to_owned(), "new-a".to_owned()),
            ("b".to_owned(), "new-b".to_owned()),
        ]
    );

    wtxn.abort()?;
    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "a")?, Some("old-a"));
    assert_eq!(db.get(&rtxn, "b")?, None);
    assert_eq!(db.get(&rtxn, "c")?, Some("old-c"));

    Ok(())
}

#[test]
fn write_txn_read_api_merges_pending_writes() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, Some("rw-api"))?;
    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        db.put(&mut wtxn, key, value)?;
    }
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let db = env.open_database::<Str, Str>(&env.read_txn()?, Some("rw-api"))?;
    db.put(&mut wtxn, "b", "20")?;
    db.put(&mut wtxn, "c2", "new")?;
    db.put(&mut wtxn, "e", "5")?;
    db.delete(&mut wtxn, "c")?;

    assert_eq!(db.get_rw(&wtxn, "a")?, Some("1"));
    assert_eq!(db.get_rw(&wtxn, "b")?, Some("20"));
    assert_eq!(db.get_rw(&wtxn, "c")?, None);
    assert_eq!(db.get_rw(&wtxn, "c2")?, Some("new"));
    assert_eq!(db.get_rw(&wtxn, "e")?, Some("5"));
    assert_eq!(db.get_rw(&wtxn, "missing")?, None);

    let forward: Vec<_> = db
        .iter_rw(&wtxn)?
        .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(
        forward,
        [
            ("a".to_owned(), "1".to_owned()),
            ("b".to_owned(), "20".to_owned()),
            ("c2".to_owned(), "new".to_owned()),
            ("d".to_owned(), "4".to_owned()),
            ("e".to_owned(), "5".to_owned()),
        ]
    );

    let reverse: Vec<_> = db
        .rev_iter_rw(&wtxn)?
        .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(
        reverse,
        [
            ("e".to_owned(), "5".to_owned()),
            ("d".to_owned(), "4".to_owned()),
            ("c2".to_owned(), "new".to_owned()),
            ("b".to_owned(), "20".to_owned()),
            ("a".to_owned(), "1".to_owned()),
        ]
    );

    let range: Vec<_> = db
        .range_rw(&wtxn, Bound::Included("b"), Bound::Excluded("e"))?
        .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(
        range,
        [
            ("b".to_owned(), "20".to_owned()),
            ("c2".to_owned(), "new".to_owned()),
            ("d".to_owned(), "4".to_owned()),
        ]
    );

    let rev_range: Vec<_> = db
        .rev_range_rw(&wtxn, Bound::Included("b"), Bound::Excluded("e"))?
        .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(
        rev_range,
        [
            ("d".to_owned(), "4".to_owned()),
            ("c2".to_owned(), "new".to_owned()),
            ("b".to_owned(), "20".to_owned()),
        ]
    );

    let prefix: Vec<_> = db
        .prefix_rw(&wtxn, "c")?
        .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(prefix, [("c2".to_owned(), "new".to_owned())]);
    let rev_prefix: Vec<_> = db
        .rev_prefix_rw(&wtxn, "c")?
        .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(rev_prefix, [("c2".to_owned(), "new".to_owned())]);

    db.clear(&mut wtxn)?;
    assert_eq!(db.get_rw(&wtxn, "a")?, None);
    let empty: Vec<_> = db
        .range_rw(&wtxn, Bound::<&str>::Unbounded, Bound::<&str>::Unbounded)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert!(empty.is_empty());

    db.put(&mut wtxn, "x", "9")?;
    assert_eq!(db.get_rw(&wtxn, "x")?, Some("9"));
    let after_clear: Vec<_> = db
        .rev_iter_rw(&wtxn)?
        .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(after_clear, [("x".to_owned(), "9".to_owned())]);

    wtxn.abort()?;
    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "a")?, Some("1"));
    assert_eq!(db.get(&rtxn, "c")?, Some("3"));
    assert_eq!(db.get(&rtxn, "e")?, None);

    Ok(())
}

#[test]
fn write_txn_ranges_match_btreemap() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    let base = [("a", "0"), ("b", "1"), ("c", "2"), ("d", "3"), ("e", "4")];
    for (key, value) in base {
        db.put(&mut wtxn, key, value)?;
    }
    wtxn.commit()?;

    let mut expected: BTreeMap<String, String> = base
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();

    let mut wtxn = env.write_txn()?;
    for (key, value) in [("b", "10"), ("c2", "20"), ("f", "50")] {
        db.put(&mut wtxn, key, value)?;
        expected.insert(key.to_owned(), value.to_owned());
    }
    db.delete(&mut wtxn, "c")?;
    expected.remove("c");
    db.delete(&mut wtxn, "e")?;
    expected.remove("e");

    let bounds = [
        Bound::Unbounded,
        Bound::Included(""),
        Bound::Excluded(""),
        Bound::Included("a"),
        Bound::Excluded("a"),
        Bound::Included("b"),
        Bound::Excluded("c"),
        Bound::Included("c2"),
        Bound::Excluded("d"),
        Bound::Included("e"),
        Bound::Excluded("f"),
        Bound::Included("z"),
    ];

    for start in bounds {
        for end in bounds {
            let wanted: Vec<_> = expected
                .iter()
                .filter(|(key, _)| {
                    let after_start = match start {
                        Bound::Unbounded => true,
                        Bound::Included(start) => key.as_str() >= start,
                        Bound::Excluded(start) => key.as_str() > start,
                    };
                    let before_end = match end {
                        Bound::Unbounded => true,
                        Bound::Included(end) => key.as_str() <= end,
                        Bound::Excluded(end) => key.as_str() < end,
                    };
                    after_start && before_end
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            let actual: Vec<_> = db
                .range_rw(&wtxn, start, end)?
                .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
                .collect::<std::result::Result<_, _>>()?;
            assert_eq!(actual, wanted, "forward range {start:?}..{end:?}");

            let mut reverse = wanted.clone();
            reverse.reverse();
            let actual: Vec<_> = db
                .rev_range_rw(&wtxn, start, end)?
                .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
                .collect::<std::result::Result<_, _>>()?;
            assert_eq!(actual, reverse, "reverse range {start:?}..{end:?}");
        }
    }

    for prefix in ["", "a", "b", "c", "c2", "d", "e", "f", "z"] {
        let wanted: Vec<_> = expected
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let actual: Vec<_> = db
            .prefix_rw(&wtxn, prefix)?
            .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(actual, wanted, "prefix {prefix:?}");

        let mut reverse = wanted;
        reverse.reverse();
        let actual: Vec<_> = db
            .rev_prefix_rw(&wtxn, prefix)?
            .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(actual, reverse, "reverse prefix {prefix:?}");
    }

    for index in 0..26u8 {
        let key = ((b'a' + index) as char).to_string();
        assert_eq!(
            db.get_rw(&wtxn, &key)?,
            expected.get(&key).map(String::as_str),
            "get_rw {key}"
        );
    }

    wtxn.abort()?;
    Ok(())
}

#[test]
fn write_txn_large_pending_reads_and_updates() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for round in 0..2u32 {
        for i in 0..1000u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), &format!("round-{round}"))?;
        }
        if round == 0 {
            assert_eq!(db.get_rw(&wtxn, "key-0500")?, Some("round-0"));
        }
    }
    assert_eq!(db.get_rw(&wtxn, "key-0500")?, Some("round-1"));
    db.put(&mut wtxn, "key-0500", "updated")?;
    assert_eq!(db.get_rw(&wtxn, "key-0500")?, Some("updated"));
    assert_eq!(db.iter_rw(&wtxn)?.count(), 1000);
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key-0500")?, Some("updated"));
    assert_eq!(db.iter(&rtxn)?.count(), 1000);
    Ok(())
}

#[test]
fn write_txn_iteration_after_clear() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "a", "1")?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn)?;
    db.put(&mut wtxn, "b", "2")?;
    let seen: Vec<_> = db
        .iter_rw(&wtxn)?
        .map(|item| item.map(|(key, value)| (key.to_owned(), value.to_owned())))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(seen, [("b".to_owned(), "2".to_owned())]);

    drop(wtxn);

    Ok(())
}

#[test]
fn compaction_reclaims_space_and_preserves_data() -> Result<()> {
    let dir = tempdir()?;
    let before_pages;
    let before_len;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let unnamed = env.create_database::<Str, Str>(&mut wtxn, None)?;
        let named = env.create_database::<Str, Str>(&mut wtxn, Some("named"))?;

        for i in 0..1_000u32 {
            let key = format!("key-{i:04}");
            let value = format!("value-{i:04}");
            unnamed.put(&mut wtxn, &key, &value)?;
            if i < 200 {
                named.put(&mut wtxn, &key, &value)?;
            }
        }
        wtxn.commit()?;

        let mut wtxn = env.write_txn()?;
        for i in 0..500u32 {
            let key = format!("key-{i:04}");
            let value = format!("updated-{i:04}");
            unnamed.put(&mut wtxn, &key, &value)?;
        }
        for i in 500..750u32 {
            unnamed.delete(&mut wtxn, &format!("key-{i:04}"))?;
        }
        for i in 0..50u32 {
            named.delete(&mut wtxn, &format!("key-{i:04}"))?;
        }
        wtxn.commit()?;

        before_pages = env.info().page_count;
        before_len = fs::metadata(dir.path().join("data.tsubame"))?.len();
        drop(named);
        drop(unnamed);
    }

    Env::compact(dir.path())?;

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let unnamed = env.open_database::<Str, Str>(&rtxn, None)?;
    let named = env.open_database::<Str, Str>(&rtxn, Some("named"))?;
    assert_eq!(unnamed.get(&rtxn, "key-0000")?, Some("updated-0000"));
    assert_eq!(unnamed.get(&rtxn, "key-0499")?, Some("updated-0499"));
    assert_eq!(unnamed.get(&rtxn, "key-0500")?, None);
    assert_eq!(unnamed.get(&rtxn, "key-0750")?, Some("value-0750"));
    assert_eq!(named.get(&rtxn, "key-0100")?, Some("value-0100"));
    assert_eq!(named.get(&rtxn, "key-0010")?, None);

    let after = env.info();
    assert!(after.page_count < before_pages);
    let after_len = fs::metadata(dir.path().join("data.tsubame"))?.len();
    assert!(after_len <= before_len);

    Ok(())
}

#[test]
fn offline_compaction_clears_stale_reader_slots() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "value")?;
        wtxn.commit()?;
    }

    let reader_path = dir.path().join("readers.tsubame");
    let stale = vec![0xff; std::fs::read(&reader_path)?.len()];
    std::fs::write(&reader_path, stale)?;

    Env::compact(dir.path())?;
    let cleared = std::fs::read(&reader_path)?;
    assert!(cleared.iter().all(|byte| *byte == 0));

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("value"));

    Ok(())
}

#[test]
fn bulk_mixed_updates_and_deletes_preserve_the_tree() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..4_096u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), &format!("value-{i:04}"))?;
        }
        wtxn.commit()?;

        let mut wtxn = env.write_txn()?;
        for i in 0..1_500u32 {
            db.put(
                &mut wtxn,
                &format!("key-{i:04}"),
                &format!("updated-{i:04}"),
            )?;
        }
        for i in 1_500..2_500u32 {
            db.delete(&mut wtxn, &format!("key-{i:04}"))?;
        }
        wtxn.commit()?;
    }

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    for i in 0..4_096u32 {
        let key = format!("key-{i:04}");
        let expected = if i < 1_500 {
            Some(format!("updated-{i:04}"))
        } else if (1_500..2_500).contains(&i) {
            None
        } else {
            Some(format!("value-{i:04}"))
        };
        assert_eq!(db.get(&rtxn, &key)?, expected.as_deref(), "key {key}");
    }

    Ok(())
}

#[test]
fn bulk_apply_rewrites_mixed_batches_and_overflow_values() -> Result<()> {
    const ENTRIES: u32 = 12_000;
    const UPDATES: u32 = 9_000;
    const NEW_ENTRIES: u32 = 500;
    let large = "x".repeat(10_000);
    let updated_large = "y".repeat(10_000);
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..ENTRIES {
            if i % 1_000 == 0 {
                db.put(&mut wtxn, &format!("key-{i:05}"), large.as_str())?;
            } else {
                db.put(&mut wtxn, &format!("key-{i:05}"), &format!("value-{i:05}"))?;
            }
        }
        wtxn.commit()?;

        let mut wtxn = env.write_txn()?;
        for i in 0..UPDATES {
            if i % 1_000 == 0 {
                db.put(&mut wtxn, &format!("key-{i:05}"), updated_large.as_str())?;
            } else {
                db.put(
                    &mut wtxn,
                    &format!("key-{i:05}"),
                    &format!("updated-{i:05}"),
                )?;
            }
        }
        for i in UPDATES..ENTRIES {
            db.delete(&mut wtxn, &format!("key-{i:05}"))?;
        }
        for i in 0..NEW_ENTRIES {
            db.put(
                &mut wtxn,
                &format!("new-{i:05}"),
                &format!("new-value-{i:05}"),
            )?;
        }
        wtxn.commit()?;
    }

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.len(&rtxn)?, u64::from(UPDATES + NEW_ENTRIES));
    for i in 0..UPDATES {
        let key = format!("key-{i:05}");
        if i % 1_000 == 0 {
            assert_eq!(
                db.get(&rtxn, &key)?,
                Some(updated_large.as_str()),
                "key {key}"
            );
        } else {
            assert_eq!(
                db.get(&rtxn, &key)?,
                Some(format!("updated-{i:05}").as_str()),
                "key {key}"
            );
        }
    }
    for i in UPDATES..ENTRIES {
        assert_eq!(
            db.get(&rtxn, &format!("key-{i:05}"))?,
            None,
            "deleted key {i}"
        );
    }
    for i in 0..NEW_ENTRIES {
        assert_eq!(
            db.get(&rtxn, &format!("new-{i:05}"))?,
            Some(format!("new-value-{i:05}").as_str()),
            "new key {i}"
        );
    }

    Ok(())
}

#[test]
fn bulk_apply_respects_a_custom_comparator() -> Result<()> {
    const ENTRIES: u32 = 10_000;
    let dir = tempdir()?;
    let mut options = options();
    options
        .comparator(KeyComparator::custom(reverse_bytes))
        .comparator_id("reverse-v1");
    {
        let env = options.open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..ENTRIES {
            db.put(&mut wtxn, &format!("key-{i:05}"), &format!("value-{i:05}"))?;
        }
        wtxn.commit()?;

        let mut wtxn = env.write_txn()?;
        for i in 0..9_000u32 {
            db.put(
                &mut wtxn,
                &format!("key-{i:05}"),
                &format!("updated-{i:05}"),
            )?;
        }
        for i in 8_000..ENTRIES {
            db.delete(&mut wtxn, &format!("key-{i:05}"))?;
        }
        wtxn.commit()?;
    }

    let env = options.open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.len(&rtxn)?, 8_000);
    for i in 0..8_000u32 {
        assert_eq!(
            db.get(&rtxn, &format!("key-{i:05}"))?,
            Some(format!("updated-{i:05}").as_str()),
            "key {i}"
        );
    }
    for i in 8_000..ENTRIES {
        assert_eq!(db.get(&rtxn, &format!("key-{i:05}"))?, None, "key {i}");
    }
    let keys: Vec<_> = db
        .iter(&rtxn)?
        .map(|item| item.map(|(key, _)| key))
        .collect::<Result<_>>()?;
    assert_eq!(keys.first().copied(), Some("key-07999"));
    assert_eq!(keys.last().copied(), Some("key-00000"));

    Ok(())
}

#[test]
fn bulk_apply_handles_large_insert_only_batches() -> Result<()> {
    const ENTRIES: u32 = 10_000;
    const INSERTS: u32 = 9_000;
    const UPDATES: u32 = 500;
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..ENTRIES {
            db.put(&mut wtxn, &format!("key-{i:05}"), &format!("value-{i:05}"))?;
        }
        wtxn.commit()?;

        let mut wtxn = env.write_txn()?;
        for i in 0..INSERTS {
            db.put(
                &mut wtxn,
                &format!("aaa-{i:05}"),
                &format!("inserted-{i:05}"),
            )?;
        }
        for i in 0..UPDATES {
            db.put(
                &mut wtxn,
                &format!("key-{i:05}"),
                &format!("updated-{i:05}"),
            )?;
        }
        wtxn.commit()?;
    }

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.len(&rtxn)?, u64::from(ENTRIES + INSERTS));
    assert_eq!(db.first(&rtxn)?, Some(("aaa-00000", "inserted-00000")));
    assert_eq!(db.last(&rtxn)?, Some(("key-09999", "value-09999")));
    for i in 0..INSERTS {
        assert_eq!(
            db.get(&rtxn, &format!("aaa-{i:05}"))?,
            Some(format!("inserted-{i:05}").as_str()),
            "inserted key {i}"
        );
    }
    for i in 0..ENTRIES {
        let key = format!("key-{i:05}");
        let expected = if i < UPDATES {
            format!("updated-{i:05}")
        } else {
            format!("value-{i:05}")
        };
        assert_eq!(db.get(&rtxn, &key)?, Some(expected.as_str()), "key {key}");
    }

    Ok(())
}

#[test]
fn bulk_load_uses_staged_large_values() -> Result<()> {
    let dir = tempdir()?;
    let values: Vec<Vec<u8>> = (0..1_100u32)
        .map(|i| vec![(i % 251) as u8; 2_000])
        .collect();
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Bytes>(&mut wtxn, None)?;
        for (i, value) in values.iter().enumerate() {
            db.put(&mut wtxn, &format!("key-{i:04}"), value.as_slice())?;
        }
        wtxn.commit()?;
    }

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Bytes>(&rtxn, None)?;
    for (i, value) in values.iter().enumerate() {
        assert_eq!(
            db.get(&rtxn, &format!("key-{i:04}"))?,
            Some(value.as_slice()),
            "key {i}"
        );
    }

    Ok(())
}

#[test]
fn bulk_delete_many_keys_and_overflow_values() -> Result<()> {
    let dir = tempdir()?;
    let large = "x".repeat(10_000);
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..4096u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), &format!("value-{i:04}"))?;
        }
        db.put(&mut wtxn, "large", &large)?;
        wtxn.commit()?;

        let mut wtxn = env.write_txn()?;
        for i in (0..4096u32).step_by(2) {
            db.delete(&mut wtxn, &format!("key-{i:04}"))?;
        }
        db.delete(&mut wtxn, "large")?;
        wtxn.commit()?;
    }

    {
        let env = options().open(dir.path())?;
        let rtxn = env.read_txn()?;
        let db = env.open_database::<Str, Str>(&rtxn, None)?;
        for i in 0..4096u32 {
            let key = format!("key-{i:04}");
            let expected = (i % 2 == 1).then(|| format!("value-{i:04}"));
            assert_eq!(db.get(&rtxn, &key)?, expected.as_deref(), "key {key}");
        }
        assert_eq!(db.get(&rtxn, "large")?, None);
        drop(rtxn);

        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in (0..4096u32).step_by(2) {
            db.delete(&mut wtxn, &format!("key-{:04}", i + 1))?;
        }
        wtxn.commit()?;
        drop(db);
    }

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.iter(&rtxn)?.count(), 0);

    Ok(())
}

#[test]
fn compaction_preserves_overflow_values() -> Result<()> {
    let dir = tempdir()?;
    let value = vec![0x5a; 10_000];
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Bytes>(&mut wtxn, None)?;
        db.put(&mut wtxn, "blob", value.as_slice())?;
        wtxn.commit()?;
    }

    Env::compact(dir.path())?;

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Bytes>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "blob")?, Some(value.as_slice()));

    Ok(())
}

#[test]
fn read_only_environments_share_and_block_writers() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "value")?;
        wtxn.commit()?;
    }

    let mut read_options = options();
    read_options.read_only(true);
    let first = read_options
        .open(dir.path())
        .context("opening first reader")?;
    let second = read_options
        .open(dir.path())
        .context("opening second reader")?;
    assert!(first.is_read_only());
    assert!(second.is_read_only());
    assert!(first.info().read_only);
    assert!(first.write_txn().is_err());

    let rtxn = first.read_txn()?;
    let db = first.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("value"));

    let writer_error = options().open(dir.path()).unwrap_err();
    assert!(writer_error.to_string().contains("locked"));
    assert!(Env::compact(dir.path()).is_err());

    drop(rtxn);
    drop(db);
    drop(first);
    drop(second);

    let env = options().open(dir.path()).context("opening final writer")?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("value"));

    Ok(())
}

#[test]
fn multi_process_page_reuse_waits_for_read_only_reader() -> Result<()> {
    let dir = tempdir()?;
    let mut writer_options = options();
    writer_options.multi_process(true);
    let writer = writer_options.open(dir.path())?;
    let mut wtxn = writer.write_txn()?;
    let db = writer.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..200u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), "old")?;
    }
    wtxn.commit()?;
    let base_pages = writer.info().page_count;

    let mut read_options = options();
    read_options.read_only(true);
    let reader = read_options.open(dir.path())?;
    let old_rtxn = reader.read_txn()?;
    let old_db = reader.open_database::<Str, Str>(&old_rtxn, None)?;

    for round in 0..20u32 {
        let mut wtxn = writer.write_txn()?;
        for i in 0..200u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), &format!("round-{round}"))?;
        }
        wtxn.commit()?;
    }
    for i in 0..200u32 {
        assert_eq!(
            old_db.get(&old_rtxn, &format!("key-{i:04}"))?,
            Some("old"),
            "old snapshot diverged at key-{i:04}"
        );
    }
    let while_reader_pages = writer.info().page_count;
    assert!(while_reader_pages > base_pages);

    drop(old_rtxn);
    drop(old_db);
    drop(reader);

    for round in 20..40u32 {
        let mut wtxn = writer.write_txn()?;
        for i in 0..200u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), &format!("round-{round}"))?;
        }
        wtxn.commit()?;
    }
    let after_reader_pages = writer.info().page_count;
    assert!(
        after_reader_pages <= while_reader_pages + 16,
        "page count grew from {while_reader_pages} to {after_reader_pages} after the reader left"
    );

    drop(db);
    drop(writer);

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key-0000")?, Some("round-39"));
    assert_eq!(db.get(&rtxn, "key-0199")?, Some("round-39"));

    Ok(())
}

#[test]
fn multi_process_writer_coexists_with_read_only_environment() -> Result<()> {
    let dir = tempdir()?;
    let mut writer_options = options();
    writer_options.multi_process(true);
    let writer = writer_options
        .open(dir.path())
        .context("opening initial multi writer")?;
    assert!(writer.is_multi_process());
    assert!(writer.info().multi_process);

    let mut wtxn = writer.write_txn()?;
    let db = writer.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", "old")?;
    for i in 0..100u32 {
        db.put(&mut wtxn, &format!("key-{i:03}"), "old")?;
    }
    wtxn.commit()?;
    let pages_before = writer.info().page_count;

    let mut read_options = options();
    read_options.read_only(true);
    let reader = read_options
        .open(dir.path())
        .context("opening read-only reader")?;
    let old_rtxn = reader.read_txn()?;
    let old_db = reader.open_database::<Str, Str>(&old_rtxn, None)?;
    assert_eq!(old_db.get(&old_rtxn, "key")?, Some("old"));

    for round in 0..5u32 {
        let mut wtxn = writer.write_txn()?;
        for i in 0..100u32 {
            db.put(&mut wtxn, &format!("key-{i:03}"), &format!("round-{round}"))?;
        }
        wtxn.commit()?;
    }
    assert!(writer.info().page_count > pages_before);
    assert_eq!(old_db.get(&old_rtxn, "key-000")?, Some("old"));

    let new_rtxn = reader.read_txn()?;
    let new_db = reader.open_database::<Str, Str>(&new_rtxn, None)?;
    assert_eq!(new_db.get(&new_rtxn, "key-000")?, Some("round-4"));

    let mut second_writer = options();
    second_writer.multi_process(true);
    assert!(second_writer.open(dir.path()).is_err());
    assert!(options().open(dir.path()).is_err());
    assert!(Env::compact(dir.path()).is_err());

    drop(new_rtxn);
    drop(new_db);
    drop(old_rtxn);
    drop(old_db);
    drop(reader);
    drop(db);
    drop(writer);

    let env = options()
        .open(dir.path())
        .context("opening final writer after multi reader")?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key-000")?, Some("round-4"));

    Ok(())
}

#[test]
fn read_only_open_reads_after_reuse_and_clear() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..1_000u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), "initial")?;
        }
        wtxn.commit()?;

        for round in 0..5u32 {
            let mut wtxn = env.write_txn()?;
            for i in 0..100u32 {
                db.put(&mut wtxn, &format!("key-{i:04}"), &format!("round-{round}"))?;
            }
            wtxn.commit()?;
        }

        let mut wtxn = env.write_txn()?;
        db.clear(&mut wtxn)?;
        db.put(&mut wtxn, "after-clear", "ok")?;
        wtxn.commit()?;
    }

    let mut read_options = options();
    read_options.read_only(true);
    let env = read_options.open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "after-clear")?, Some("ok"));
    assert_eq!(db.get(&rtxn, "key-0000")?, None);

    Ok(())
}

#[test]
fn read_only_open_does_not_create_or_extend_an_environment() -> Result<()> {
    let dir = tempdir()?;
    let mut read_options = options();
    read_options.read_only(true);
    assert!(read_options.open(dir.path()).is_err());

    let env = options().open(dir.path())?;
    assert!(read_options.open(dir.path()).is_err());
    drop(env);

    let first = read_options
        .open(dir.path())
        .context("opening first reader")?;
    let second = read_options
        .open(dir.path())
        .context("opening second reader")?;
    drop(first);
    drop(second);

    Ok(())
}

#[test]
fn online_compaction_preserves_custom_comparator() -> Result<()> {
    let dir = tempdir()?;
    let comparator = KeyComparator::custom(reverse_bytes);
    let mut options = options();
    options
        .comparator(comparator)
        .comparator_id("reverse-online");
    let env = options.open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
        db.put(&mut wtxn, key, value)?;
    }
    wtxn.commit()?;
    drop(db);

    env.compact_online()?;
    assert_eq!(env.comparator_id(), "reverse-online");
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    let keys: Vec<_> = db
        .iter(&rtxn)?
        .map(|item| item.map(|(key, _)| key.to_owned()))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(keys, ["c", "b", "a"]);

    Ok(())
}

#[test]
fn multi_process_writer_clears_stale_registry_state() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "value")?;
        wtxn.commit()?;
    }

    let registry = dir.path().join("readers.tsubame");
    let stale = vec![0xff; std::fs::read(&registry)?.len()];
    std::fs::write(&registry, stale)?;

    let mut writer_options = options();
    writer_options.multi_process(true);
    let writer = writer_options.open(dir.path())?;
    writer.compact_online()?;
    let rtxn = writer.read_txn()?;
    let db = writer.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("value"));

    Ok(())
}

#[test]
fn multi_process_online_compaction_refreshes_read_only_environment() -> Result<()> {
    let dir = tempdir()?;
    let mut writer_options = options();
    writer_options.multi_process(true);
    let writer = writer_options.open(dir.path())?;
    let mut wtxn = writer.write_txn()?;
    let db = writer.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", "before")?;
    wtxn.commit()?;

    let mut read_options = options();
    read_options.read_only(true);
    let reader = read_options.open(dir.path())?;
    let old_rtxn = reader.read_txn()?;
    let old_db = reader.open_database::<Str, Str>(&old_rtxn, None)?;
    assert_eq!(old_db.get(&old_rtxn, "key")?, Some("before"));

    writer.compact_online()?;
    assert_eq!(old_db.get(&old_rtxn, "key")?, Some("before"));

    let mut wtxn = writer.write_txn()?;
    db.put(&mut wtxn, "key", "after")?;
    wtxn.commit()?;

    let new_rtxn = reader.read_txn()?;
    let new_db = reader.open_database::<Str, Str>(&new_rtxn, None)?;
    assert_eq!(new_db.get(&new_rtxn, "key")?, Some("after"));

    drop(new_rtxn);
    drop(new_db);
    drop(old_rtxn);
    drop(old_db);
    drop(db);
    drop(reader);
    drop(writer);

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("after"));

    Ok(())
}

#[test]
fn writer_restart_refreshes_read_only_environment() -> Result<()> {
    let dir = tempdir()?;
    let mut writer_options = options();
    writer_options.multi_process(true);
    {
        let writer = writer_options.open(dir.path())?;
        let mut wtxn = writer.write_txn()?;
        let db = writer.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "before")?;
        wtxn.commit()?;
    }

    let mut read_options = options();
    read_options.read_only(true);
    let reader = read_options.open(dir.path())?;
    let old_rtxn = reader.read_txn()?;
    let old_db = reader.open_database::<Str, Str>(&old_rtxn, None)?;
    assert_eq!(old_db.get(&old_rtxn, "key")?, Some("before"));

    let writer = writer_options.open(dir.path())?;
    let read_rtxn = writer.read_txn()?;
    let db = writer.open_database::<Str, Str>(&read_rtxn, None)?;
    drop(read_rtxn);
    let mut wtxn = writer.write_txn()?;
    db.put(&mut wtxn, "key", "after")?;
    wtxn.commit()?;

    let new_rtxn = reader.read_txn()?;
    let new_db = reader.open_database::<Str, Str>(&new_rtxn, None)?;
    assert_eq!(new_db.get(&new_rtxn, "key")?, Some("after"));
    assert_eq!(old_db.get(&old_rtxn, "key")?, Some("before"));

    Ok(())
}

#[test]
fn online_compaction_remaps_an_open_environment() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..1_000u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), "value")?;
    }
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    for i in 0..500u32 {
        db.delete(&mut wtxn, &format!("key-{i:04}"))?;
    }
    for i in 500..1_000u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), "updated")?;
    }
    wtxn.commit()?;
    let before = env.info().page_count;
    drop(db);

    env.compact_online()?;
    assert!(env.info().page_count < before);

    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key-0000")?, None);
    assert_eq!(db.get(&rtxn, "key-0999")?, Some("updated"));
    drop(rtxn);
    drop(db);

    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "after-compaction", "ok")?;
    wtxn.commit()?;
    drop(db);
    drop(env);

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "after-compaction")?, Some("ok"));
    assert_eq!(db.get(&rtxn, "key-0999")?, Some("updated"));

    Ok(())
}

#[test]
fn online_compaction_coexists_with_active_readers() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", "before")?;
    wtxn.commit()?;

    let old_rtxn = env.read_txn()?;
    let old_db = env.open_database::<Str, Str>(&old_rtxn, None)?;
    assert_eq!(old_db.get(&old_rtxn, "key")?, Some("before"));

    let writer = env.write_txn()?;
    assert!(env.compact_online().is_err());
    drop(writer);

    env.compact_online()?;
    assert_eq!(old_db.get(&old_rtxn, "key")?, Some("before"));

    let new_rtxn = env.read_txn()?;
    let new_db = env.open_database::<Str, Str>(&new_rtxn, None)?;
    assert_eq!(new_db.get(&new_rtxn, "key")?, Some("before"));
    drop(new_rtxn);
    drop(new_db);
    drop(old_rtxn);
    drop(old_db);
    drop(db);

    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "after", "ok")?;
    wtxn.commit()?;
    drop(db);
    drop(env);

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "after")?, Some("ok"));

    Ok(())
}

#[test]
fn compaction_requires_the_environment_to_be_closed() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;

    let error = Env::compact(dir.path()).unwrap_err();
    assert!(error.to_string().contains("open"));

    drop(env);
    Env::compact(dir.path())?;

    Ok(())
}

#[test]
fn online_page_reuse_keeps_growth_bounded() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..1_000u32 {
        let key = format!("key-{i:04}");
        db.put(&mut wtxn, &key, "initial")?;
    }
    wtxn.commit()?;
    let base_pages = env.info().page_count;

    for round in 0..50u32 {
        let mut wtxn = env.write_txn()?;
        for i in 0..100u32 {
            let key = format!("key-{:04}", (round * 100 + i) % 1_000);
            db.put(&mut wtxn, &key, &format!("round-{round}"))?;
        }
        wtxn.commit()?;
    }

    let after_pages = env.info().page_count;
    assert!(
        after_pages <= base_pages + 50,
        "page count grew from {base_pages} to {after_pages}"
    );

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key-0000")?, Some("round-40"));
    assert_eq!(db.get(&rtxn, "key-0001")?, Some("round-40"));
    drop(rtxn);
    drop(db);
    drop(env);

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key-0000")?, Some("round-40"));
    assert_eq!(db.get(&rtxn, "key-0900")?, Some("round-49"));

    Ok(())
}

#[test]
fn reuse_splits_large_free_runs_for_small_pages() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Bytes>(&mut wtxn, None)?;
    let large = vec![0x5a; 1024 * 1024];
    db.put(&mut wtxn, "large", large.as_slice())?;
    wtxn.commit()?;
    let base_pages = env.info().page_count;

    let mut wtxn = env.write_txn()?;
    db.delete(&mut wtxn, "large")?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    for i in 0..500u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), b"small".as_slice())?;
    }
    wtxn.commit()?;

    let after_pages = env.info().page_count;
    assert!(
        after_pages <= base_pages + 32,
        "small allocations did not reuse the large free run: {base_pages} -> {after_pages}"
    );

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key-0000")?, Some(b"small".as_slice()));
    assert_eq!(db.get(&rtxn, "key-0499")?, Some(b"small".as_slice()));

    Ok(())
}

#[test]
fn online_page_reuse_reclaims_overflow_extents() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..20u32 {
        let value = format!("initial-{i}-{}", "x".repeat(10_000));
        db.put(&mut wtxn, &format!("key-{i:04}"), &value)?;
    }
    wtxn.commit()?;
    let base_pages = env.info().page_count;

    for round in 0..5u32 {
        let mut wtxn = env.write_txn()?;
        for i in 0..20u32 {
            let value = format!("round-{round}-{i}-{}", "y".repeat(10_000));
            db.put(&mut wtxn, &format!("key-{i:04}"), &value)?;
        }
        wtxn.commit()?;
    }
    let steady_pages = env.info().page_count;

    for round in 5..25u32 {
        let mut wtxn = env.write_txn()?;
        for i in 0..20u32 {
            let value = format!("round-{round}-{i}-{}", "y".repeat(10_000));
            db.put(&mut wtxn, &format!("key-{i:04}"), &value)?;
        }
        wtxn.commit()?;
    }

    let after_pages = env.info().page_count;
    assert!(
        after_pages <= steady_pages + 12,
        "overflow page count grew from {steady_pages} to {after_pages} (base {base_pages})"
    );

    let rtxn = env.read_txn()?;
    let value = db.get(&rtxn, "key-0000")?.expect("missing value");
    assert!(value.starts_with("round-24-0-"));
    let value = db.get(&rtxn, "key-0019")?.expect("missing value");
    assert!(value.starts_with("round-24-19-"));

    Ok(())
}

#[test]
fn reopen_reconstructs_free_pages() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..1_000u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), "initial")?;
        }
        wtxn.commit()?;

        for round in 0..5u32 {
            let mut wtxn = env.write_txn()?;
            for i in 0..1_000u32 {
                db.put(
                    &mut wtxn,
                    &format!("key-{i:04}"),
                    &format!("before-{round}"),
                )?;
            }
            wtxn.commit()?;
        }
    }

    let env = options().open(dir.path())?;
    let db = env.open_database::<Str, Str>(&env.read_txn()?, None)?;

    for round in 0..4u32 {
        let mut wtxn = env.write_txn()?;
        for i in 0..1_000u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), &format!("warm-{round}"))?;
        }
        wtxn.commit()?;
    }
    let steady_pages = env.info().page_count;

    for round in 4..24u32 {
        let mut wtxn = env.write_txn()?;
        for i in 0..1_000u32 {
            db.put(&mut wtxn, &format!("key-{i:04}"), &format!("after-{round}"))?;
        }
        wtxn.commit()?;
    }

    let after_pages = env.info().page_count;
    assert!(
        after_pages <= steady_pages + 40,
        "reopened free list did not stabilize: {steady_pages} -> {after_pages}"
    );

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key-0000")?, Some("after-23"));
    assert_eq!(db.get(&rtxn, "key-0999")?, Some("after-23"));
    drop(rtxn);
    drop(db);
    drop(env);

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key-0000")?, Some("after-23"));
    assert_eq!(db.get(&rtxn, "key-0999")?, Some("after-23"));

    Ok(())
}

#[test]
fn reopen_reconstruction_preserves_overflow_values() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..10u32 {
            db.put(
                &mut wtxn,
                &format!("key-{i:04}"),
                &format!("initial-{i}-{}", "x".repeat(10_000)),
            )?;
        }
        wtxn.commit()?;

        for round in 0..3u32 {
            let mut wtxn = env.write_txn()?;
            for i in 0..10u32 {
                db.put(
                    &mut wtxn,
                    &format!("key-{i:04}"),
                    &format!("before-{round}-{i}-{}", "y".repeat(10_000)),
                )?;
            }
            wtxn.commit()?;
        }
    }

    let env = options().open(dir.path())?;
    let db = env.open_database::<Str, Str>(&env.read_txn()?, None)?;

    for round in 0..5u32 {
        let mut wtxn = env.write_txn()?;
        for i in 0..10u32 {
            db.put(
                &mut wtxn,
                &format!("key-{i:04}"),
                &format!("warm-{round}-{i}-{}", "z".repeat(10_000)),
            )?;
        }
        wtxn.commit()?;
    }
    let steady_pages = env.info().page_count;

    for round in 5..15u32 {
        let mut wtxn = env.write_txn()?;
        for i in 0..10u32 {
            db.put(
                &mut wtxn,
                &format!("key-{i:04}"),
                &format!("after-{round}-{i}-{}", "w".repeat(10_000)),
            )?;
        }
        wtxn.commit()?;
    }

    let after_pages = env.info().page_count;
    assert!(
        after_pages <= steady_pages + 20,
        "overflow pages did not stabilize after reopen: {steady_pages} -> {after_pages}"
    );

    let rtxn = env.read_txn()?;
    let value = db.get(&rtxn, "key-0000")?.expect("missing value");
    assert!(value.starts_with("after-14-0-"));
    let value = db.get(&rtxn, "key-0009")?.expect("missing value");
    assert!(value.starts_with("after-14-9-"));

    Ok(())
}

#[test]
fn reopen_reconstruction_preserves_named_databases() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let named = env.create_database::<Str, Str>(&mut wtxn, Some("named"))?;
        let unnamed = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..100u32 {
            let key = format!("key-{i:03}");
            named.put(&mut wtxn, &key, "named-initial")?;
            unnamed.put(&mut wtxn, &key, "unnamed-initial")?;
        }
        wtxn.commit()?;

        for round in 0..3u32 {
            let mut wtxn = env.write_txn()?;
            for i in 0..100u32 {
                let key = format!("key-{i:03}");
                named.put(&mut wtxn, &key, &format!("named-before-{round}"))?;
                unnamed.put(&mut wtxn, &key, &format!("unnamed-before-{round}"))?;
            }
            wtxn.commit()?;
        }
    }

    let env = options().open(dir.path())?;
    let named = env.open_database::<Str, Str>(&env.read_txn()?, Some("named"))?;
    let unnamed = env.open_database::<Str, Str>(&env.read_txn()?, None)?;

    for round in 0..6u32 {
        let mut wtxn = env.write_txn()?;
        for i in 0..100u32 {
            let key = format!("key-{i:03}");
            named.put(&mut wtxn, &key, &format!("named-after-{round}"))?;
            unnamed.put(&mut wtxn, &key, &format!("unnamed-after-{round}"))?;
        }
        wtxn.commit()?;
    }

    let rtxn = env.read_txn()?;
    assert_eq!(named.get(&rtxn, "key-000")?, Some("named-after-5"));
    assert_eq!(named.get(&rtxn, "key-099")?, Some("named-after-5"));
    assert_eq!(unnamed.get(&rtxn, "key-000")?, Some("unnamed-after-5"));
    assert_eq!(unnamed.get(&rtxn, "key-099")?, Some("unnamed-after-5"));

    Ok(())
}

#[test]
fn reopen_reconstruction_keeps_previous_meta_recoverable() -> Result<()> {
    let dir = tempdir()?;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..100u32 {
            db.put(&mut wtxn, &format!("key-{i:03}"), "initial")?;
        }
        wtxn.commit()?;

        for round in 0..4u32 {
            let mut wtxn = env.write_txn()?;
            for i in 0..100u32 {
                db.put(
                    &mut wtxn,
                    &format!("key-{i:03}"),
                    &format!("before-{round}"),
                )?;
            }
            wtxn.commit()?;
        }
    }

    let latest_txn;
    {
        let env = options().open(dir.path())?;
        let db = env.open_database::<Str, Str>(&env.read_txn()?, None)?;
        for round in 0..5u32 {
            let mut wtxn = env.write_txn()?;
            for i in 0..100u32 {
                db.put(&mut wtxn, &format!("key-{i:03}"), &format!("after-{round}"))?;
            }
            wtxn.commit()?;
        }
        latest_txn = env.info().txn_id;
        let rtxn = env.read_txn()?;
        assert_eq!(db.get(&rtxn, "key-000")?, Some("after-4"));
    }

    std::fs::remove_file(dir.path().join("clean.tsubame")).ok();
    let slot = (latest_txn - 1) % 2;
    let mut file = OpenOptions::new()
        .write(true)
        .open(dir.path().join("data.tsubame"))?;
    file.seek(SeekFrom::Start(slot * 4096 + 100))?;
    file.write_all(&[0xff])?;

    let env = options().open(dir.path())?;
    assert_eq!(env.info().txn_id, latest_txn - 1);
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key-000")?, Some("after-3"));
    assert_eq!(db.get(&rtxn, "key-099")?, Some("after-3"));

    Ok(())
}

#[test]
fn page_reuse_waits_for_readers() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    db.put(&mut wtxn, "key", "old")?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    for round in 0..20 {
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, "key", &format!("new-{round}"))?;
        wtxn.commit()?;
    }
    assert_eq!(db.get(&rtxn, "key")?, Some("old"));
    drop(rtxn);

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key")?, Some("new-19"));

    Ok(())
}

#[test]
fn aborted_transactions_return_reused_pages() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..100u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), "initial")?;
    }
    wtxn.commit()?;
    let base_pages = env.info().page_count;

    for round in 0..20u32 {
        let mut wtxn = env.write_txn()?;
        for i in 0..100u32 {
            db.put(
                &mut wtxn,
                &format!("key-{i:04}"),
                &format!("aborted-{round}"),
            )?;
        }
        wtxn.abort()?;
    }

    let mut wtxn = env.write_txn()?;
    for i in 0..100u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), "committed")?;
    }
    wtxn.commit()?;

    let after_pages = env.info().page_count;
    assert!(
        after_pages <= base_pages + 10,
        "aborted transactions leaked pages: {base_pages} -> {after_pages}"
    );

    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, "key-0000")?, Some("committed"));

    Ok(())
}

#[test]
fn page_reuse_keeps_previous_meta_recoverable() -> Result<()> {
    let dir = tempdir()?;
    let latest_txn;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        wtxn.commit()?;

        for round in 0..30u32 {
            let mut wtxn = env.write_txn()?;
            db.put(&mut wtxn, "key", &format!("round-{round}"))?;
            wtxn.commit()?;
        }
        latest_txn = env.info().txn_id;
        let rtxn = env.read_txn()?;
        assert_eq!(db.get(&rtxn, "key")?, Some("round-29"));
    }

    std::fs::remove_file(dir.path().join("clean.tsubame")).ok();
    let slot = (latest_txn - 1) % 2;
    let mut file = OpenOptions::new()
        .write(true)
        .open(dir.path().join("data.tsubame"))?;
    file.seek(SeekFrom::Start(slot * 4096 + 100))?;
    file.write_all(&[0xff])?;

    let env = options().open(dir.path())?;
    assert_eq!(env.info().txn_id, latest_txn - 1);
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "key")?, Some("round-28"));

    Ok(())
}

#[test]
fn page_reuse_keeps_previous_commit_record_recoverable() -> Result<()> {
    let dir = tempdir()?;
    let latest_txn;
    {
        let env = options().open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        for i in 0..10u32 {
            let value = format!("initial-{i}-{}", "x".repeat(10_000));
            db.put(&mut wtxn, &format!("key-{i:04}"), &value)?;
        }
        wtxn.commit()?;

        for round in 0..6u32 {
            let mut wtxn = env.write_txn()?;
            for i in 0..10u32 {
                let value = format!("round-{round}-{i}-{}", "y".repeat(10_000));
                db.put(&mut wtxn, &format!("key-{i:04}"), &value)?;
            }
            wtxn.commit()?;
        }
        latest_txn = env.info().txn_id;
        let rtxn = env.read_txn()?;
        let value = db.get(&rtxn, "key-0000")?.expect("missing value");
        assert!(value.starts_with("round-5-0-"));
    }

    std::fs::remove_file(dir.path().join("clean.tsubame")).ok();
    let slot = (latest_txn - 1) % 2;
    let mut file = OpenOptions::new()
        .write(true)
        .open(dir.path().join("data.tsubame"))?;
    file.seek(SeekFrom::Start(slot * 4096 + 100))?;
    file.write_all(&[0xff])?;

    let env = options().open(dir.path())?;
    assert_eq!(env.info().txn_id, latest_txn - 1);
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    let value = db.get(&rtxn, "key-0000")?.expect("missing value");
    assert!(value.starts_with("round-4-0-"));

    Ok(())
}

#[test]
fn reuse_survives_mixed_updates_clear_and_readers() -> Result<()> {
    let dir = tempdir()?;
    let env = options().open(dir.path())?;
    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    wtxn.commit()?;

    let mut expected = BTreeMap::new();
    let mut state = 0x1234_5678_9abc_def0u64;
    let mut next = |bound: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % bound
    };

    let mut old_snapshot = None;
    for round in 0..40u32 {
        if round == 10 {
            old_snapshot = Some((env.read_txn()?, expected.clone()));
        }

        let mut trial = expected.clone();
        let mut wtxn = env.write_txn()?;
        for step in 0..20u32 {
            let index = next(64);
            let key = format!("key-{index:03}");
            match next(10) {
                0 => {
                    db.clear(&mut wtxn)?;
                    trial.clear();
                }
                1..=3 => {
                    db.delete(&mut wtxn, &key)?;
                    trial.remove(&key);
                }
                _ => {
                    let value = format!("round-{round}-step-{step}");
                    db.put(&mut wtxn, &key, &value)?;
                    trial.insert(key, value);
                }
            }
        }

        let abort = next(10) == 0;
        if abort {
            wtxn.abort()?;
        } else {
            wtxn.commit()?;
            expected = trial;
        }

        let rtxn = env.read_txn()?;
        for index in 0..64u32 {
            let key = format!("key-{index:03}");
            assert_eq!(
                db.get(&rtxn, &key)?,
                expected.get(&key).map(String::as_str),
                "current snapshot diverged at round {round}, key {key}"
            );
        }
        drop(rtxn);

        if let Some((rtxn, snapshot)) = &old_snapshot {
            for index in 0..64u32 {
                let key = format!("key-{index:03}");
                assert_eq!(
                    db.get(rtxn, &key)?,
                    snapshot.get(&key).map(String::as_str),
                    "old snapshot diverged at round {round}, key {key}"
                );
            }
        }
    }

    drop(old_snapshot);
    drop(db);
    drop(env);

    let env = options().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    for index in 0..64u32 {
        let key = format!("key-{index:03}");
        assert_eq!(
            db.get(&rtxn, &key)?,
            expected.get(&key).map(String::as_str),
            "reopened state diverged at key {key}"
        );
    }

    Ok(())
}

fn reverse_bytes(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    right.cmp(left)
}

#[test]
fn custom_comparator_orders_reads_and_compaction() -> Result<()> {
    let dir = tempdir()?;
    let comparator = KeyComparator::custom(reverse_bytes);
    let mut options = options();
    options.comparator(comparator).comparator_id("reverse-v1");

    {
        let env = options.open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        let named = env.create_database::<Str, Str>(&mut wtxn, Some("named"))?;
        for (key, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
            db.put(&mut wtxn, key, value)?;
            named.put(&mut wtxn, key, value)?;
        }
        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        let ordered: Vec<_> = db
            .iter(&rtxn)?
            .map(|item| item.map(|(key, _)| key.to_owned()))
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(ordered, ["c", "b", "a"]);
        let reverse: Vec<_> = db
            .rev_iter(&rtxn)?
            .map(|item| item.map(|(key, _)| key.to_owned()))
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(reverse, ["a", "b", "c"]);

        let range: Vec<_> = db
            .range(&rtxn, Bound::Included("c"), Bound::Included("b"))?
            .map(|item| item.map(|(key, _)| key.to_owned()))
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(range, ["c", "b"]);
        assert!(db.prefix_iter(&rtxn, "a").is_err());
        assert_eq!(db.get(&rtxn, "b")?, Some("2"));
        assert_eq!(named.get(&rtxn, "a")?, Some("1"));

        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, "d", "4")?;
        db.delete(&mut wtxn, "b")?;
        let pending: Vec<_> = db
            .iter_rw(&wtxn)?
            .map(|item| item.map(|(key, _)| key.to_owned()))
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(pending, ["d", "c", "a"]);
        assert_eq!(db.get_rw(&wtxn, "d")?, Some("4"));
        let reverse_pending: Vec<_> = db
            .rev_iter_rw(&wtxn)?
            .map(|item| item.map(|(key, _)| key.to_owned()))
            .collect::<std::result::Result<_, _>>()?;
        assert_eq!(reverse_pending, ["a", "c", "d"]);
        assert!(db.prefix_rw(&wtxn, "a").is_err());
        wtxn.commit()?;
    }

    Env::compact_with(dir.path(), comparator, "reverse-v1")?;
    let env = options.open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    let ordered: Vec<_> = db
        .iter(&rtxn)?
        .map(|item| item.map(|(key, _)| key.to_owned()))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(ordered, ["d", "c", "a"]);
    assert_eq!(db.get(&rtxn, "c")?, Some("3"));
    let named = env.open_database::<Str, Str>(&rtxn, Some("named"))?;
    let named_keys: Vec<_> = named
        .iter(&rtxn)?
        .map(|item| item.map(|(key, _)| key.to_owned()))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(named_keys, ["c", "b", "a"]);

    Ok(())
}

#[test]
fn custom_comparator_bulk_delete() -> Result<()> {
    let dir = tempdir()?;
    let mut options = options();
    options
        .comparator(KeyComparator::custom(reverse_bytes))
        .comparator_id("reverse-v1");
    let env = options.open(dir.path())?;

    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..2_000u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), &format!("value-{i}"))?;
    }
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    for i in 0..1_500u32 {
        db.delete(&mut wtxn, &format!("key-{i:04}"))?;
    }
    db.delete(&mut wtxn, "missing")?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    for i in 0..2_000u32 {
        let key = format!("key-{i:04}");
        let expected = (i >= 1_500).then(|| format!("value-{i}"));
        assert_eq!(db.get(&rtxn, &key)?, expected.as_deref(), "key {key}");
    }

    Ok(())
}

#[test]
fn custom_comparator_bulk_load_and_write_ranges() -> Result<()> {
    let dir = tempdir()?;
    let mut options = options();
    options
        .comparator(KeyComparator::custom(reverse_bytes))
        .comparator_id("reverse-v1");
    let env = options.open(dir.path())?;

    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
    for i in 0..2_000u32 {
        db.put(&mut wtxn, &format!("key-{i:04}"), &format!("value-{i}"))?;
    }
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let first = db
        .iter(&rtxn)?
        .next()
        .transpose()?
        .expect("missing first entry")
        .0;
    let last = db
        .rev_iter(&rtxn)?
        .next()
        .transpose()?
        .expect("missing last entry")
        .0;
    assert_eq!(first, "key-1999");
    assert_eq!(last, "key-0000");

    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, "key-0500x", "updated")?;
    let range: Vec<_> = db
        .range_rw(
            &wtxn,
            Bound::Included("key-0600"),
            Bound::Included("key-0400"),
        )?
        .map(|item| item.map(|(key, _)| key.to_owned()))
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(range.first().map(String::as_str), Some("key-0600"));
    assert_eq!(range.last().map(String::as_str), Some("key-0400"));
    wtxn.abort()?;

    Ok(())
}

#[test]
fn comparator_identity_is_persisted_and_checked() -> Result<()> {
    let dir = tempdir()?;
    let comparator = KeyComparator::custom(reverse_bytes);
    {
        let mut options = options();
        options.comparator(comparator).comparator_id("reverse-v1");
        let env = options.open(dir.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "a", "1")?;
        wtxn.commit()?;
        assert_eq!(env.comparator_id(), "reverse-v1");
    }

    let mut correct = options();
    correct.comparator(comparator).comparator_id("reverse-v1");
    let env = correct.open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, Str>(&rtxn, None)?;
    assert_eq!(db.get(&rtxn, "a")?, Some("1"));
    drop(rtxn);
    drop(db);
    drop(env);

    let mut wrong = options();
    wrong.comparator(comparator).comparator_id("reverse-v2");
    let error = wrong.open(dir.path()).unwrap_err();
    assert!(error.to_string().contains("comparator id mismatch"));

    let mut missing_id = options();
    missing_id.comparator(comparator);
    let error = missing_id.open(dir.path()).unwrap_err();
    assert!(error.to_string().contains("comparator_id"));

    assert!(Env::compact_with(dir.path(), comparator, "reverse-v2").is_err());

    let legacy = tempdir()?;
    {
        let env = options().open(legacy.path())?;
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, Str>(&mut wtxn, None)?;
        db.put(&mut wtxn, "key", "value")?;
        wtxn.commit()?;
    }
    std::fs::remove_file(legacy.path().join("comparator.tsubame"))?;
    let env = options().open(legacy.path())?;
    assert_eq!(env.comparator_id(), "lexicographic");
    drop(env);
    assert!(legacy.path().join("comparator.tsubame").is_file());

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
    file.seek(SeekFrom::Start((page_count - 1) * 4096 + 8))?;
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
