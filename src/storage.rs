use redb::{Database, Error, ReadableTable, TableDefinition};
use std::collections::HashMap;
use std::env;

use crate::entry::OutPointValue;

const UTXO_TO_RANGE_TABLE: TableDefinition<&OutPointValue, &[u8]> =
  TableDefinition::new("utxo_to_ranges");

const BLOCK_HEIGHT: TableDefinition<&str, u64> = TableDefinition::new("block_height");

pub struct Storage {
  db: Database,
}

impl Storage {
  pub fn new() -> Storage {
    let path = env::var("STORAGE_PATH").expect("Storage path is not defined in environment");
    let db = Database::create(path).expect("Failed to create database");

    Storage { db }
  }

  pub fn set_block_height(&self, height: u64) -> Result<(), Error> {
    let wtx = self.db.begin_write()?;

    {
      let mut table = wtx.open_table(BLOCK_HEIGHT)?;
      table.insert("height", &height)?;
    }

    wtx.commit().unwrap();

    Ok(())
  }

  pub fn get_block_height(&self) -> Result<u64, Error> {
    let read_txn = self.db.begin_read()?;
    let table = match read_txn.open_table(BLOCK_HEIGHT) {
      Ok(table) => table,
      Err(_) => {
        self.set_block_height(0)?;
        return Ok(0);
      }
    };
    let binding = table.get("height")?.unwrap();
    let value = binding.value();
    Ok(value)
  }

  pub fn insert_ranges(&self, ranges: &HashMap<OutPointValue, Vec<u8>>) -> Result<(), Error> {
    let wtx = self.db.begin_write()?;

    {
      let mut table = wtx.open_table(UTXO_TO_RANGE_TABLE)?;
      for (key, value) in ranges {
        table.insert(key, value.as_slice())?;
      }
    }

    wtx.commit().unwrap();

    Ok(())
  }

  pub fn flush_ranges(&self, ranges: &Vec<OutPointValue>) -> Result<(), Error> {
    let wtx = self.db.begin_write()?;

    {
      let mut table = wtx.open_table(UTXO_TO_RANGE_TABLE)?;
      for key in ranges {
        table.remove(key)?;
      }
    }

    wtx.commit().unwrap();

    Ok(())
  }

  pub fn get_ranges(&self, outpoint: &OutPointValue) -> Result<Vec<u8>, Error> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(UTXO_TO_RANGE_TABLE)?;
    let binding = table.get(outpoint)?.unwrap();
    let value = binding.value();
    Ok(Vec::from(value))
  }
}
