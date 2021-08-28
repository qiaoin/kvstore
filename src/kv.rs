use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::{KvsError, Result};

/// value representing set/rm command
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Self {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Self {
        Command::Remove { key }
    }
}

/// The `KvStore` used HashMap, storing in memroy, not on a disk
///
/// Example:
///
/// ```rust
/// use kvs::KvStore;
/// let mut store = KvStore::new();
///
/// store.set("key1".to_owned(), "value1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
/// assert_eq!(store.get("key2".to_owned()), None);
///
/// store.remove("key1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), None);
/// ```
pub struct KvStore {
    // path: PathBuf,
    // reader of the current log
    reader: BufferReaderWithPos<File>,
    // writer of the current log.
    writer: BufferWriterWithPos<File>,
    // an in-memory [key -> log pointer] map.
    index: HashMap<String, CommandPos>,
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir = path.into();
        fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.log", 1123));
        let reader = BufferReaderWithPos::new(
            OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&path)?, 
        )?;
        let writer = BufferWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&path)?,
        )?;
        Ok(KvStore {
            // path
            reader,
            writer,
            // TODO: 需要在 startup 的时候 load 一下
            index: HashMap::new(),
        })
    }

    /// Set the value of a string key to a string
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        Ok(())
    }

    /// Get the string value of the a string key.
    ///
    /// If the key does not exist, return None.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // reads the entire log, one command at a time, recording the affected key and
        // file offset of the command to an in-memory [key -> log pointer] map
        if let Some(cmd_pos) = self.index.get(&key) {
            // key --> command's start postion
            self.reader.seek(SeekFrom::Start(cmd_pos.start))?;
            // key --> command's length
            let cmd_reader = self.reader.by_ref().take(cmd_pos.length);
            if let Command::Set { key: _, value } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;

            // TODO: 从 index 中删除 key 对应的 entry
            if let Command::Remove{ key } = cmd {
                self.index.remove(&key).expect("key not found");
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

struct CommandPos {
    start: u64,
    length: u64,
}

struct BufferWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    // TODO: writer 需要 pos 吗？
    pos: u64,
}

impl<W: Write + Seek> BufferWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufferWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufferWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

// TODO: BufferWriterWithPos 是否有必要实现 Seek trait？
impl<W: Write + Seek> Seek for BufferWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufferReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufferReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufferReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufferReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufferReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}
