extern crate dotenv;

use std::collections::{HashMap, VecDeque};

use bitcoin::{OutPoint, Transaction};
use dotenv::dotenv;
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
  dotenv().ok();

  let storage = Storage::new();
  let client = RpcClient::new();
  let block_count = client.getblockcount().unwrap();

  let mut range_cache: HashMap<OutPointValue, Vec<u8>> = HashMap::new();
  let mut flush_cache: Vec<OutPointValue> = Vec::new();

  let mut height = storage.get_block_height().unwrap();
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
          Some(range) => Ok(range),
          None => match storage.get_ranges(&key) {
            Ok(range) => {
              flush_cache.push(key.clone());
              Ok(range)
            }
            Err(_) => Err(format!(
              "Could not find outpoint {} in index",
              input.previous_output
            )),
          },
        }
        .unwrap();

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

    println!("{} / {}", height, block_count);

    if height == 2_413_342 {
      panic!("Time to prepare for inscriptions");
    }

    if height % 5000 == 0 {
      storage
        .insert_ranges(&range_cache)
        .expect("Failed to insert sat ranges");
      storage
        .flush_ranges(&flush_cache)
        .expect("Failed to flush sat ranges");
      storage
        .set_block_height(height)
        .expect("Failed to set block height");
      range_cache = HashMap::new();
    }

    height += 1;
  }
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
