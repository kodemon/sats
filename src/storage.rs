use core::panic;
use std::collections::HashMap;
// use std::env;

use rusqlite::Connection;

use crate::entry::OutPointValue;

#[derive(Debug)]
struct Indexer {
  id: i64,
  height: u64,
}

#[derive(Debug)]
struct Ranges {
  outpoint: OutPointValue,
  ranges: Vec<u8>,
}

pub struct Storage {}

impl Storage {
  pub fn new() -> Storage {
    let conn = get_connection();

    conn.execute_batch("PRAGMA journal_mode = WAL").unwrap();

    conn
      .execute(
        "
        CREATE TABLE IF NOT EXISTS indexer (
          id      INTEGER PRIMARY KEY AUTOINCREMENT,
          height  INTEGER NOT NULL
        )
        ",
        (),
      )
      .unwrap();

    conn
      .execute(
        "
        CREATE TABLE IF NOT EXISTS ranges (
          outpoint BLOB PRIMARY KEY,
          ranges   BLOB NOT NULL
        )
        ",
        (),
      )
      .unwrap();

    conn
      .execute(
        "
        CREATE TABLE IF NOT EXISTS flush (
          outpoint BLOB PRIMARY KEY
        )
        ",
        (),
      )
      .unwrap();

    Storage {}
  }

  pub fn set_block_height(&self, height: &u64) {
    let conn = get_connection();

    conn.execute_batch("PRAGMA journal_mode = WAL").unwrap();

    let mut stmt = conn
      .prepare("INSERT OR REPLACE INTO indexer (id, height) VALUES (1, ?1)")
      .unwrap();

    stmt.execute(&[height]).unwrap();
  }

  pub fn get_block_height(&self) -> u64 {
    let conn = get_connection();

    conn.execute_batch("PRAGMA journal_mode = WAL").unwrap();

    let mut stmt = conn
      .prepare("SELECT id, height FROM indexer WHERE id = 1")
      .unwrap();

    let indexers = stmt
      .query_map((), |row| {
        Ok(Indexer {
          id: row.get(0)?,
          height: row.get(1)?,
        })
      })
      .expect("Could not get block height");

    for indexer in indexers {
      return indexer.unwrap().height;
    }

    return 0;
  }

  pub fn insert_ranges(&self, ranges: &HashMap<OutPointValue, Vec<u8>>) {
    let mut conn = get_connection();

    conn.execute_batch("PRAGMA journal_mode = WAL").unwrap();

    let tx = conn.transaction().unwrap();
    for (key, value) in ranges {
      tx.execute(
        "INSERT OR IGNORE INTO ranges (outpoint, ranges) VALUES (?, ?)",
        (key, value.as_slice()),
      )
      .unwrap();
    }

    tx.commit().unwrap();
  }

  pub fn get_ranges(&self, outpoint: &OutPointValue) -> Vec<u8> {
    let conn = get_connection();

    conn.execute_batch("PRAGMA journal_mode = WAL").unwrap();

    let mut stmt = conn
      .prepare("SELECT outpoint, ranges FROM ranges WHERE outpoint = ?")
      .unwrap();

    let ranges = stmt
      .query_map(&[outpoint], |row| {
        Ok(Ranges {
          outpoint: row.get(0)?,
          ranges: row.get(1)?,
        })
      })
      .expect("Could not get block height");

    for range in ranges {
      return range.unwrap().ranges;
    }

    panic!("Could not find outpoint in index");
  }

  pub fn flush_ranges(&self, outpoints: &Vec<OutPointValue>) {
    let mut conn = get_connection();

    conn.execute_batch("PRAGMA journal_mode = WAL").unwrap();

    let flush = conn.transaction().unwrap();
    for key in outpoints {
      flush
        .execute("INSERT OR IGNORE INTO flush (outpoint) VALUES (?)", [key])
        .unwrap();
    }
    flush.commit().unwrap();

    let mut ranges = conn
      .prepare(
        "
        DELETE FROM ranges WHERE outpoint IN (
          SELECT outpoint FROM flush
        )
        ",
      )
      .unwrap();

    ranges.execute(()).unwrap();

    let mut clean = conn.prepare("DELETE FROM flush").unwrap();
    clean.execute(()).unwrap();
  }
}

fn get_connection() -> Connection {
  return Connection::open("./sats.sqlite").unwrap();
}
