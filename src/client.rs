use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::{KvsError, Result};

use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

/// KvsClent
pub struct KvsClient {
    writer: BufWriter<TcpStream>,
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
}

impl KvsClient {
    /// connect to a remote hosts
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let tcp_writer = TcpStream::connect(addr)?;
        let tcp_reader = tcp_writer.try_clone()?;
        // println!("client local addr: {:?}", tcp_writer.local_addr()?);
        // println!("server addr: {:?}", tcp_writer.peer_addr()?);

        Ok(KvsClient {
            writer: BufWriter::new(tcp_writer),
            reader: Deserializer::from_reader(BufReader::new(tcp_reader)),
        })
    }

    /// set
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;

        let resp = SetResponse::deserialize(&mut self.reader)?;
        // println!("set response: {:?}", resp);
        match resp {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }

    /// get
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;

        let resp = GetResponse::deserialize(&mut self.reader)?;
        // println!("get response: {:?}", resp);
        match resp {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }

    /// remove
    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Remove { key })?;
        self.writer.flush()?;

        let resp = RemoveResponse::deserialize(&mut self.reader)?;
        // println!("remove response: {:?}", resp);
        match resp {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }
}
