use crate::{
  entry::{Entry, OutPointValue, SatRange},
  rpc::RpcClient,
  storage::Storage,
};
use bitcoin::{Block, OutPoint, Transaction};
use std::collections::{HashMap, VecDeque};

const COIN_VALUE: u64 = 100_000_000;
const SUBSIDY_HALVING_INTERVAL: u64 = 210_000;

pub struct Indexer {
  range_cache: HashMap<OutPointValue, Vec<u8>>,
  spent_cache: Vec<OutPointValue>,
  storage: Storage,
  rpc: RpcClient,
}

impl Indexer {
  pub fn new(data_dir: String, host: String, username: String, password: String) -> Indexer {
    Indexer {
      range_cache: HashMap::new(),
      spent_cache: Vec::new(),
      storage: Storage::new(data_dir),
      rpc: RpcClient::new(host, username, password),
    }
  }

  pub fn update(&mut self) {
    let block_count = self.rpc.get_block_count();

    let mut height = self.storage.get_next_height();
    while height <= block_count {
      println!("{}", height);
      let block = self.rpc.get_block(&height);
      self.index_block(block, height);

      if self.has_reached_treshold(height) {
        self
          .storage
          .commit(&mut self.range_cache, &mut self.spent_cache, &height);
      }

      height += 1;
    }

    self
      .storage
      .commit(&mut self.range_cache, &mut self.spent_cache, &height);
  }

  fn index_block(&mut self, block: Block, height: u64) {
    let mut coinbase_inputs = VecDeque::new();

    if subsidy(height) > 0 {
      let start = starting_sat(height);
      coinbase_inputs.push_front((start, start + subsidy(height)));
    }

    for (_, tx) in block.txdata.iter().enumerate().skip(1) {
      let mut input_sat_ranges = VecDeque::new();

      for input in &tx.input {
        let outpoint = input.previous_output.store();

        self.spent_cache.push(outpoint);

        let sat_ranges = match self.range_cache.remove(&outpoint) {
          Some(ranges) => ranges,
          None => self.storage.get_sat_ranges(outpoint),
        };

        for chunk in sat_ranges.chunks_exact(11) {
          input_sat_ranges.push_back(SatRange::load(chunk.try_into().unwrap()))
        }
      }

      self.index_transaction_sats(tx, &mut input_sat_ranges);

      coinbase_inputs.extend(input_sat_ranges);
    }

    self.index_transaction_sats(block.txdata.get(0).unwrap(), &mut coinbase_inputs);
  }

  fn index_transaction_sats(
    &mut self,
    tx: &Transaction,
    input_sat_ranges: &mut VecDeque<(u64, u64)>,
  ) {
    let txid = tx.txid();
    for (vout, output) in tx.output.iter().enumerate() {
      let outpoint = OutPoint {
        vout: vout.try_into().unwrap(),
        txid,
      };
      let mut sats = Vec::new();

      let mut remaining = output.value;
      while remaining > 0 {
        let range = input_sat_ranges
          .pop_front()
          .expect("insufficient inputs for transaction outputs");

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
      }

      self.range_cache.insert(outpoint.store(), sats);
    }
  }

  fn has_reached_treshold(&self, height: u64) -> bool {
    if height == 2_413_341 {
      return true;
    }
    if height != 0 && height % 5000 == 0 {
      return true;
    }
    return false;
  }
}

fn starting_sat(height: u64) -> u64 {
  let mut start = 0;
  for h in 0..height {
    start += subsidy(h);
  }
  start
}

fn subsidy(height: u64) -> u64 {
  (50 * COIN_VALUE) / 2_u64.pow((height / SUBSIDY_HALVING_INTERVAL) as u32)
}
