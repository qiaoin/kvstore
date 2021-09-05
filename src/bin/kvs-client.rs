use clap::{AppSettings, Clap};
use kvs::{KvsClient, Result};
use std::net::SocketAddr;
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

    /// accepts an IP address, either v4 or v6, and a port number, with the format IP:PORT. If
    /// --addr is not specified then connect on
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,
}

/// Get the string value of a given string key. Print an error and return a non-zero exit code on failure.
#[derive(Clap)]
struct GetParams {
    key: String,

    /// accepts an IP address, either v4 or v6, and a port number, with the format IP:PORT. If
    /// --addr is not specified then connect on
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,
}

/// Remove a given key. Print an error and return a non-zero exit code on failure.
#[derive(Clap)]
struct RmParams {
    key: String,

    /// accepts an IP address, either v4 or v6, and a port number, with the format IP:PORT. If
    /// --addr is not specified then connect on
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,
}

fn main() {
    let opts: Opts = Opts::parse();

    if let Err(e) = run(opts) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opts: Opts) -> Result<()> {
    match opts.subcmd {
        SubCommand::Set(SetParams { key, value, addr }) => {
            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }
        SubCommand::Get(GetParams { key, addr }) => {
            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                print!("Key not found");
            }
        }
        SubCommand::Rm(RmParams { key, addr }) => {
            let mut client = KvsClient::connect(addr)?;
            client.remove(key)?;
        }
    }

    Ok(())
}
