use clap::{AppSettings, Clap};
use kvs::{KvStore, KvsEngine, KvsServer, Result};
use log::{error, info, warn, LevelFilter};
use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;
use std::str::FromStr;

const DEFAULT_ENGINE: Engine = Engine::kvs;
const ENGINE_FILE: &str = "engine";

#[derive(Clap)]
#[clap(name = env!("CARGO_PKG_NAME"), about = env!("CARGO_PKG_DESCRIPTION"), version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    /// accepts an IP address, either v4 or v6, and a port number, with the format IP:PORT. If
    /// --addr is not specified then listen on
    #[clap(long, default_value = "127.0.0.1:4000")]
    addr: SocketAddr,
    /// engine name
    #[clap(long)]
    engine: Option<Engine>,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Engine {
    kvs,
    sled,
}

// imple FromStr trait
impl FromStr for Engine {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Engine::kvs),
            "sled" => Ok(Engine::sled),
            _ => Err("no match engine"),
        }
    }
}

fn main() {
    // init logging
    env_logger::Builder::new()
        .filter_level(LevelFilter::max())
        .init();

    let mut opts: Opts = Opts::parse();

    let res = current_engine().and_then(|curr_engine| {
        info!("curr engine: {:?}", curr_engine);
        if opts.engine.is_none() {
            opts.engine = curr_engine;
        }
        if curr_engine.is_some() && opts.engine != curr_engine {
            error!("Wrong engine!");
            exit(1);
        }
        run(opts)
    });

    if let Err(e) = res {
        error!("{:?}", e);
        exit(1);
    }
}

fn run(opts: Opts) -> Result<()> {
    let engine = opts.engine.unwrap_or(DEFAULT_ENGINE);
    info!("kvs-server {:?}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {:?}", engine);
    info!("Listening on {:?}", opts.addr);

    // 写 engine 文件
    fs::write(current_dir()?.join(ENGINE_FILE), format!("{:?}", engine))?;

    match engine {
        Engine::kvs => run_with_engine(KvStore::open(current_dir()?)?, opts.addr),
        Engine::sled => run_with_engine(KvStore::open(current_dir()?)?, opts.addr),
    }
}

fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let mut server = KvsServer::new(engine);
    server.run(addr)
}

fn current_engine() -> Result<Option<Engine>> {
    let engine = current_dir()?.join(ENGINE_FILE);
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine)?.parse::<Engine>() {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!("The content of engine file is invalid: {:?}", e);
            Ok(None)
        }
    }
}
