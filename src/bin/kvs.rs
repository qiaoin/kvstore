use clap::{AppSettings, Clap};
use kvs::{KvStore, KvsError, Result};
use std::env::current_dir;
use std::process::exit;

#[derive(Clap)]
#[clap(name = env!("CARGO_PKG_NAME"), about = env!("CARGO_PKG_DESCRIPTION"), version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    Set(SetParams),
    Get(GetParams),
    Rm(RmParams),
}

/// Set the value of a string key to a string. Print an error and return a non-zero exit code on failure.
#[derive(Clap)]
struct SetParams {
    key: String,
    value: String,
}

/// Get the string value of a given string key. Print an error and return a non-zero exit code on failure.
#[derive(Clap)]
struct GetParams {
    key: String,
}

/// Remove a given key. Print an error and return a non-zero exit code on failure.
#[derive(Clap)]
struct RmParams {
    key: String,
}

fn main() -> Result<()> {
    let opts: Opts = Opts::parse();

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::Set(SetParams { key, value }) => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key, value)?;
        }
        SubCommand::Get(GetParams { key }) => {
            let mut store = KvStore::open(current_dir()?)?;
            if let Some(value) = store.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        SubCommand::Rm(RmParams { key }) => {
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(err) => return Err(err),
            }
        }
    }

    Ok(())
}
