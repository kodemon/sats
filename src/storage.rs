use crate::entry::OutPointValue;
use core::panic;
use rusqlite::Connection;
use std::collections::HashMap;

#[derive(Debug)]
struct Indexer {
  height: u64,
}

#[derive(Debug)]
struct Ranges {
  ranges: Vec<u8>,
}

pub struct Storage {
  conn: Connection,
}

impl Storage {
  pub fn new(data_dir: String) -> Storage {
    let conn = Connection::open(data_dir + "/sats.sqlite").unwrap();

    conn.execute_batch("PRAGMA journal_mode = WAL").unwrap();

    create_tables(&conn);

    Storage { conn }
  }

  pub fn get_next_height(&self) -> u64 {
    let current = self.get_indexed_height();
    if current == 0 {
      return 0;
    }
    return current + 1;
  }

  pub fn get_indexed_height(&self) -> u64 {
    let mut stmt = self
      .conn
      .prepare("SELECT height FROM indexer WHERE id = 1")
      .unwrap();

    let indexers = stmt
      .query_map((), |row| {
        Ok(Indexer {
          height: row.get(0)?,
        })
      })
      .expect("Could not get block height");

    for indexer in indexers {
      return indexer.unwrap().height;
    }

    return 0;
  }

  pub fn get_sat_ranges(&mut self, outpoint: OutPointValue) -> Vec<u8> {
    let mut stmt = self
      .conn
      .prepare("SELECT ranges FROM ranges WHERE outpoint = ?")
      .unwrap();

    let ranges = stmt
      .query_map(&[&outpoint], |row| {
        Ok(Ranges {
          ranges: row.get(0)?,
        })
      })
      .expect("Could not get block height");

    for range in ranges {
      return range.unwrap().ranges;
    }

    panic!("Could not find outpoint in index");
  }

  pub fn flush(&self) {
    self
      .conn
      .execute_batch("PRAGMA wal_checkpoint(FULL)")
      .unwrap();
  }

  pub fn commit(
    &mut self,
    outputs: &mut HashMap<OutPointValue, Vec<u8>>,
    spents: &mut Vec<OutPointValue>,
    height: &u64,
  ) {
    self.commit_outputs(outputs);
    outputs.clear();

    self.commit_spents(spents);
    spents.clear();

    self.commit_indexed_height(&height);
  }

  fn commit_outputs(&mut self, outputs: &HashMap<OutPointValue, Vec<u8>>) {
    let tx = self.conn.transaction().unwrap();
    for (key, value) in outputs {
      tx.execute(
        "INSERT OR IGNORE INTO ranges (outpoint, ranges) VALUES (?, ?)",
        (key, value.as_slice()),
      )
      .unwrap();
    }
    tx.commit().unwrap();
  }

  fn commit_spents(&mut self, spents: &Vec<OutPointValue>) {
    let flush = self.conn.transaction().unwrap();
    for key in spents {
      flush
        .execute("INSERT OR IGNORE INTO flush (outpoint) VALUES (?)", [key])
        .unwrap();
    }
    flush.commit().unwrap();

    let mut ranges = self
      .conn
      .prepare(
        "
        DELETE FROM ranges WHERE outpoint IN (
          SELECT outpoint FROM flush
        )
        ",
      )
      .unwrap();

    ranges.execute(()).unwrap();

    let mut clean = self.conn.prepare("DELETE FROM flush").unwrap();
    clean.execute(()).unwrap();
  }

  fn commit_indexed_height(&self, height: &u64) {
    let mut stmt = self
      .conn
      .prepare("INSERT OR REPLACE INTO indexer (id, height) VALUES (1, ?1)")
      .unwrap();

    stmt.execute(&[height]).unwrap();
  }
}

fn create_tables(conn: &Connection) {
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
}
