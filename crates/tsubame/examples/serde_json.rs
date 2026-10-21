//! A serde JSON codec example.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example serde_json --features serde-json
//! ```

#![allow(clippy::print_stdout)]

use serde::{Deserialize, Serialize};
use tempfile::tempdir;
use tsubame::types::{SerdeJson, Str};
use tsubame::{EnvOpenOptions, Result};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Profile {
    name: String,
    score: u32,
}

fn main() -> Result<()> {
    let dir = tempdir()?;
    let env = EnvOpenOptions::new().open(dir.path())?;

    let mut wtxn = env.write_txn()?;
    let db = env.create_database::<Str, SerdeJson<Profile>>(&mut wtxn, Some("profiles"))?;
    db.put(
        &mut wtxn,
        "alice",
        &Profile {
            name: "Alice".to_owned(),
            score: 42,
        },
    )?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    let profile = db.get(&rtxn, "alice")?;
    println!("alice = {profile:?}");

    Ok(())
}
