use bitcoincore_rpc::bitcoin::{Block, BlockHash};
use bitcoincore_rpc::{Auth, Client, RpcApi};
use std::env;
use std::error::Error;

pub struct RpcClient {
  client: Client,
}

impl RpcClient {
  pub fn new() -> RpcClient {
    let username =
      env::var("RPC_USER").expect("Bitcoin Core RPC username is not defined in environment");
    let password =
      env::var("RPC_PASS").expect("Bitcoin Core RPC password is not defined in environment");
    let credential = Auth::UserPass(username, password);
    let uri = env::var("RPC_URI").expect("Bitcoin Core RPC Uri is not defined in environment");
    return RpcClient {
      client: Client::new(&uri, credential).expect("Unable to connect"),
    };
  }

  pub fn getblockcount(&self) -> Result<u64, Box<dyn Error>> {
    Ok(self.client.get_block_count()? as u64)
  }

  pub fn getblockhash(&self, height: &u64) -> Result<BlockHash, Box<dyn Error>> {
    let block_hash = self.client.get_block_hash(height.clone())?;
    Ok(block_hash)
  }

  pub fn getblock(&self, block_hash: &BlockHash) -> Result<Block, Box<dyn Error>> {
    Ok(self.client.get_block(block_hash)?)
  }
}
