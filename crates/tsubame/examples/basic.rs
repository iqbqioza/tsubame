//! A small end-to-end example using a named database.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example basic
//! ```

#![allow(clippy::print_stdout)]

use std::ops::Bound;

use tempfile::tempdir;
use tsubame::byteorder::NativeEndian;
use tsubame::types::{Str, U32};
use tsubame::{EnvOpenOptions, Result};

fn main() -> Result<()> {
    let dir = tempdir()?;
    let env = EnvOpenOptions::new().open(dir.path())?;

    {
        let mut wtxn = env.write_txn()?;
        let db = env.create_database::<Str, U32<NativeEndian>>(&mut wtxn, Some("scores"))?;
        db.put(&mut wtxn, "alice", &42)?;
        db.put(&mut wtxn, "bob", &7)?;
        db.put(&mut wtxn, "carol", &99)?;
        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        println!("alice = {:?}", db.get(&rtxn, "alice")?);

        let mut scores = Vec::new();
        for item in db.iter(&rtxn)? {
            let (name, score) = item?;
            scores.push((name.to_owned(), score));
        }
        println!("all scores: {scores:?}");

        let mut selected = Vec::new();
        for item in db.range(&rtxn, Bound::Included("b"), Bound::Unbounded)? {
            let (name, score) = item?;
            selected.push((name.to_owned(), score));
        }
        println!("from b: {selected:?}");

        drop(rtxn);
        drop(db);
    }

    drop(env);

    // Reopen the same directory and read the committed data again.
    let env = EnvOpenOptions::new().open(dir.path())?;
    let rtxn = env.read_txn()?;
    let db = env.open_database::<Str, U32<NativeEndian>>(&rtxn, Some("scores"))?;
    println!("after reopen, bob = {:?}", db.get(&rtxn, "bob")?);

    Ok(())
}
