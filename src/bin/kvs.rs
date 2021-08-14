use clap::{AppSettings, Clap};
use std::process;

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

/// Set the value of a string key to a string
#[derive(Clap)]
struct SetParams {
    key: String,
    value: String,
}

/// Get the string value of a given string key
#[derive(Clap)]
struct GetParams {
    key: String,
}

/// Remove a given key
#[derive(Clap)]
struct RmParams {
    key: String,
}

fn main() {
    let opts: Opts = Opts::parse();

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    match opts.subcmd {
        SubCommand::Set(SetParams { key, value }) => {
            eprintln!("Set unimplemented, key: {} -> value: {}", key, value);

            process::exit(1);
        }
        SubCommand::Get(GetParams { key }) => {
            eprintln!("Get unimplemented, key: {}", key);

            process::exit(1);
        }
        SubCommand::Rm(RmParams { key }) => {
            eprintln!("Rm unimplemented, key: {}", key);

            process::exit(1);
        }
    }

    // more program logic goes here...
}
