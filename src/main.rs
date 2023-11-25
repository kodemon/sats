use bitcoin::OutPoint;
use clap::{command, Arg, ArgMatches, Command};
use indexer::Indexer;
use storage::Storage;

mod entry;
mod indexer;
mod rpc;
mod storage;

fn main() {
  let matched: ArgMatches = command!()
    .arg(
      Arg::new("data-dir")
        .short('d')
        .long("data-dir")
        .help("Absolute or relative path to the data directory")
        .required(true),
    )
    .arg(
      Arg::new("host")
        .short('h')
        .long("host")
        .help("Host of the Bitcoin Core RPC server")
        .required(true),
    )
    .arg(
      Arg::new("user")
        .short('u')
        .long("user")
        .help("User of the Bitcoin Core RPC server")
        .required(true),
    )
    .arg(
      Arg::new("password")
        .short('p')
        .long("password")
        .help("Password of the Bitcoin Core RPC server")
        .required(true),
    )
    .subcommand(Command::new("index").about("Index ordinals from the blockchain"))
    .subcommand(Command::new("flush").about("Flush forwarded data to the main database files"))
    .get_matches();

  let data_dir = matched.get_one::<String>("data-dir").unwrap().clone();
  let hostname = matched.get_one::<String>("host").unwrap().clone();
  let username = matched.get_one::<String>("user").unwrap().clone();
  let password = matched.get_one::<String>("password").unwrap().clone();

  match matched.subcommand() {
    Some(("index", _)) => Indexer::new(data_dir, hostname, username, password).update(),
    Some(("flush", _)) => {
      Storage::new(data_dir).flush();
    }
    _ => println!("No subcommand was used"),
  }
}
