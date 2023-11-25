use bitcoincore_rpc::bitcoin::{Block, BlockHash};
use bitcoincore_rpc::{Auth, Client, RpcApi};

pub struct RpcClient {
  client: Client,
}

impl RpcClient {
  pub fn new(host: String, username: String, password: String) -> RpcClient {
    let credential = Auth::UserPass(username, password);
    RpcClient {
      client: Client::new(&host, credential).expect("Unable to connect"),
    }
  }

  pub fn get_block(&self, height: &u64) -> Block {
    let block_hash = self.get_block_hash(&height);
    return self.client.get_block(&block_hash).unwrap();
  }

  pub fn get_block_hash(&self, height: &u64) -> BlockHash {
    return self.client.get_block_hash(height.clone()).unwrap();
  }

  pub fn get_block_count(&self) -> u64 {
    return self.client.get_block_count().unwrap() as u64;
  }
}
