use log::{error, info};
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::{KvsEngine, Result};

/// KvsServer
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    /// new a `KvsServer` with given backend `engine`
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    /// create a new TcpListener which is bound to `addr` and processes the connection
    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        // 建立 TcpListener
        let listener = TcpListener::bind(addr)?;
        info!("run on {:?}", listener.local_addr()?);
        // 处理 tcp 连接
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    info!("connection established, stream: {:?}", stream);
                    self.server(&stream)?;
                }
                Err(e) => {
                    error!("connection failed, {:?}", e);
                }
            }
        }

        Ok(())
    }

    /// server
    pub fn server(&mut self, tcp_stream: &TcpStream) -> Result<()> {
        let peer_addr = tcp_stream.peer_addr()?;
        let reader = BufReader::new(tcp_stream);
        let mut writer = BufWriter::new(tcp_stream);
        let req_stream = Deserializer::from_reader(reader).into_iter::<Request>();
        // while let Some(req) = stream.next() {
        // 语法糖
        for req in req_stream {
            match req? {
                Request::Set { key, value } => {
                    info!(
                        "recving set request from addr: {:?}, key: {:?}, value: {:?}",
                        peer_addr, key, value
                    );
                    match self.engine.set(key, value) {
                        Err(e) => {
                            serde_json::to_writer(
                                &mut writer,
                                &SetResponse::Err(format!("{}", e)),
                            )?;
                        }
                        Ok(_) => {
                            serde_json::to_writer(&mut writer, &SetResponse::Ok(()))?;
                        }
                    }
                    writer.flush()?;
                }
                Request::Get { key } => {
                    info!(
                        "recving get request from addr: {:?}, key: {:?}",
                        peer_addr, key
                    );
                    match self.engine.get(key) {
                        Err(e) => {
                            serde_json::to_writer(
                                &mut writer,
                                &GetResponse::Err(format!("{}", e)),
                            )?;
                        }
                        Ok(value) => {
                            serde_json::to_writer(&mut writer, &GetResponse::Ok(value))?;
                        }
                    }
                    writer.flush()?;
                }
                Request::Remove { key } => {
                    info!(
                        "recving rm request from addr: {:?}, key: {:?}",
                        peer_addr, key
                    );
                    match self.engine.remove(key) {
                        Err(e) => {
                            serde_json::to_writer(
                                &mut writer,
                                &RemoveResponse::Err(format!("{}", e)),
                            )?;
                        }
                        Ok(_) => {
                            serde_json::to_writer(&mut writer, &RemoveResponse::Ok(()))?;
                        }
                    }
                    writer.flush()?;
                }
            }
        }

        Ok(())
    }
}
