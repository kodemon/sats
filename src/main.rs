extern crate dotenv;

use redis::Client;
use redis::Commands;
use redis::Connection;

use std::{
  collections::{HashMap, VecDeque},
  io,
};

use bitcoin::{
  consensus::{Decodable, Encodable},
  OutPoint, Transaction,
};
use dotenv::dotenv;

mod rpc;

const COIN_VALUE: u64 = 100_000_000;
const SUBSIDY_HALVING_INTERVAL: u64 = 210_000;

pub type OutPointValue = [u8; 36];

impl Entry for OutPoint {
  type Value = OutPointValue;

  fn load(value: Self::Value) -> Self {
    Decodable::consensus_decode(&mut io::Cursor::new(value)).unwrap()
  }

  fn store(self) -> Self::Value {
    let mut value = [0; 36];
    self.consensus_encode(&mut value.as_mut_slice()).unwrap();
    value
  }
}

pub trait Entry: Sized {
  type Value;

  fn load(value: Self::Value) -> Self;

  fn store(self) -> Self::Value;
}

pub type SatRange = (u64, u64);

impl Entry for SatRange {
  type Value = [u8; 11];

  fn load([b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10]: Self::Value) -> Self {
    let raw_base = u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, 0]);

    // 51 bit base
    let base = raw_base & ((1 << 51) - 1);

    let raw_delta = u64::from_le_bytes([b6, b7, b8, b9, b10, 0, 0, 0]);

    // 33 bit delta
    let delta = raw_delta >> 3;

    (base, base + delta)
  }

  fn store(self) -> Self::Value {
    let base = self.0;
    let delta = self.1 - self.0;
    let n = u128::from(base) | u128::from(delta) << 51;
    n.to_le_bytes()[0..11].try_into().unwrap()
  }
}

#[tokio::main]
async fn main() {
  dotenv().ok();

  // ### Redis

  let block_count = rpc::getblockcount().unwrap();

  let mut range_cache: HashMap<OutPointValue, Vec<u8>> = HashMap::new();
  let mut input_cache: Vec<String> = Vec::new();
  // let mut uncomitted = 0;

  let mut height = get_height();
  while height <= block_count {
    let block_hash = rpc::getblockhash(&height).unwrap();
    let block = rpc::getblock(&block_hash).unwrap();

    let first = first_ordinal(height);
    let mut coinbase_inputs: VecDeque<(u64, u64)> = VecDeque::new();
    let mut con = connect();

    coinbase_inputs.push_front((first, first + subsidy(height)));

    let mut sat_ranges_written = 0;
    let mut outputs_in_block = 0;

    for (_, tx) in block.txdata.iter().enumerate().skip(1) {
      let mut input_sat_ranges: VecDeque<(u64, u64)> = VecDeque::new();

      // ### Extract Inputs
      // Match inputs against previous output sat ranges and assign
      // them to the input sat ranges to be used for the new outputs.

      for input in &tx.input {
        let key = input.previous_output.store();
        let out = format!(
          "{}:{}",
          input.previous_output.txid, input.previous_output.vout
        );

        let sat_ranges = match range_cache.remove(&key) {
          Some(range) => Ok(range),
          None => match con.get(&out) {
            Ok(range) => {
              input_cache.push(out);
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

    height += 1;

    for (outpoint, sat_range) in range_cache.drain() {
      let parsed = OutPoint::load(outpoint);
      let location = format!("{}:{}", parsed.txid, parsed.vout);
      let _: () = con.set(location, sat_range.as_slice()).unwrap();
    }
    let _: () = con.set("height", height).unwrap();
    for outpoint in input_cache.drain(..) {
      let _: () = con.del(outpoint).unwrap();
    }
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

fn connect() -> Connection {
  let client = Client::open("redis://127.0.0.1/").unwrap();
  client.get_connection().unwrap()
}

fn get_height() -> u64 {
  let mut connection = connect();
  connection.get("height").unwrap_or(0)
}
