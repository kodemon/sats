extern crate dotenv;

use std::collections::{HashMap, VecDeque};

use bitcoin::{OutPoint, Transaction};
use entry::{Entry, OutPointValue, SatRange};
use rpc::RpcClient;
use storage::Storage;

mod entry;
mod rpc;
mod storage;

const COIN_VALUE: u64 = 100_000_000;
const SUBSIDY_HALVING_INTERVAL: u64 = 210_000;

#[tokio::main]
async fn main() {
  let storage = Storage::new();
  let client = RpcClient::new();
  let block_count = client.getblockcount().unwrap();

  let mut range_cache: HashMap<OutPointValue, Vec<u8>> = HashMap::new();
  let mut flush_cache: Vec<OutPointValue> = Vec::new();

  let mut height = storage.get_block_height() + 1;
  while height <= block_count {
    let block_hash = client.getblockhash(&height).unwrap();
    let block = client.getblock(&block_hash).unwrap();

    let first = first_ordinal(height);
    let mut coinbase_inputs: VecDeque<(u64, u64)> = VecDeque::new();

    coinbase_inputs.push_front((first, first + subsidy(height)));

    let mut sat_ranges_written = 0;
    let mut outputs_in_block = 0;

    for (_, tx) in block.txdata.iter().enumerate().skip(1) {
      let mut input_sat_ranges: VecDeque<SatRange> = VecDeque::new();

      // ### Extract Inputs
      // Match inputs against previous output sat ranges and assign
      // them to the input sat ranges to be used for the new outputs.

      for input in &tx.input {
        let key = input.previous_output.store();

        let sat_ranges = match range_cache.remove(&key) {
          Some(range) => range,
          None => storage.get_ranges(&key),
        };

        flush_cache.push(key);

        for chunk in sat_ranges.chunks_exact(11) {
          input_sat_ranges.push_back(SatRange::load(chunk.try_into().unwrap()))
        }
      }

      // ### Process Outputs
      // Move satoshis from input sat ranges to outputs. Once completed
      // move any remaining ranges to the coinbase inputs.

      index_transaction_sats(
        tx,
        tx.txid(),
        &mut input_sat_ranges,
        &mut range_cache,
        &mut sat_ranges_written,
        &mut outputs_in_block,
      );

      coinbase_inputs.extend(input_sat_ranges);
    }

    // ### Coinbase
    // Coinbase transactions are the genesis of new satoshis.

    let tx = block.txdata.get(0).unwrap();
    index_transaction_sats(
      tx,
      tx.txid(),
      &mut coinbase_inputs,
      &mut range_cache,
      &mut sat_ranges_written,
      &mut outputs_in_block,
    );

    if height != 0 && height % 5000 == 0 {
      persist(&storage, &mut range_cache, &mut flush_cache, &height);
      range_cache = HashMap::new();
      flush_cache = Vec::new();
    }

    height += 1;
  }

  persist(&storage, &mut range_cache, &mut flush_cache, &height);
}

fn persist(
  storage: &Storage,
  range_cache: &mut HashMap<OutPointValue, Vec<u8>>,
  flush_cache: &mut Vec<OutPointValue>,
  height: &u64,
) {
  println!("---------- Persisting {} ----------", height);

  storage.insert_ranges(&range_cache);
  println!("💽 Inserted {} outpoints", range_cache.len());

  storage.flush_ranges(&flush_cache);
  println!("💽 Deleted {} outpoints", flush_cache.len());

  storage.set_block_height(&height);
  println!("📦 Done");
}

fn index_transaction_sats(
  tx: &Transaction,
  txid: bitcoin::Txid,
  input_sat_ranges: &mut VecDeque<(u64, u64)>,
  range_cache: &mut HashMap<OutPointValue, Vec<u8>>,
  sat_ranges_written: &mut u64,
  outputs_traversed: &mut u64,
) {
  for (vout, output) in tx.output.iter().enumerate() {
    let outpoint = OutPoint {
      vout: vout.try_into().unwrap(),
      txid,
    };
    let mut sats = Vec::new();

    let mut remaining = output.value;
    while remaining > 0 {
      let range = match input_sat_ranges.pop_front() {
        Some(range) => range,
        None => panic!("insufficient inputs for transaction outputs"),
      };

      let count = range.1 - range.0;

      let assigned = if count > remaining {
        let middle = range.0 + remaining;
        input_sat_ranges.push_front((middle, range.1));
        (range.0, middle)
      } else {
        range
      };

      sats.extend_from_slice(&assigned.store());

      remaining -= assigned.1 - assigned.0;

      *sat_ranges_written += 1;
    }

    *outputs_traversed += 1;

    range_cache.insert(outpoint.store(), sats);
  }
}

fn first_ordinal(height: u64) -> u64 {
  let mut start = 0;
  for h in 0..height {
    start += subsidy(h);
  }
  start
}

fn subsidy(height: u64) -> u64 {
  (50 * COIN_VALUE) / 2_u64.pow((height / SUBSIDY_HALVING_INTERVAL) as u32)
}
