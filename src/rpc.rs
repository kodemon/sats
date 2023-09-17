use bitcoin::blockdata::transaction::Transaction;
use bitcoin::hash_types::Txid;
use bitcoincore_rpc::bitcoin::{Block, BlockHash};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use std::env;
use std::error::Error;

fn connect() -> Result<Client, String> {
  let username =
    env::var("RPC_USER").expect("Bitcoin Core RPC username is not defined in environment");
  let password =
    env::var("RPC_PASS").expect("Bitcoin Core RPC password is not defined in environment");

  let credential = Auth::UserPass(username, password);

  let uri = env::var("RPC_URI").expect("Bitcoin Core RPC Uri is not defined in environment");

  let rpc = Client::new(&uri, credential).expect("Unable to connect");

  Ok(rpc)
}

pub fn getblockcount() -> Result<u64, Box<dyn Error>> {
  let client = connect()?;
  let tip: u64 = client.get_block_count()? as u64;
  Ok(tip)
}

pub fn getblockhash(height: &u64) -> Result<BlockHash, Box<dyn Error>> {
  let client = connect()?;

  let height: u64 = height.to_owned() as u64;
  let block_hash = client.get_block_hash(height)?;

  Ok(block_hash)
}

pub fn getblock(block_hash: &BlockHash) -> Result<Block, Box<dyn Error>> {
  let client = connect()?;
  let block = client.get_block(block_hash)?;

  Ok(block)
}

pub fn _getrawtransaction(
  txid: &Txid,
  block_hash: Option<&BlockHash>,
) -> Result<Transaction, Box<dyn Error>> {
  let client = connect()?;
  let transaction = client.get_raw_transaction(txid, block_hash)?;

  Ok(transaction)
}
