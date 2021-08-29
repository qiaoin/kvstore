use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

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
    // directory for the log and other data.
    path: PathBuf,
    current_gen: u64,
    // map generation number to the file reader.
    readers: HashMap<u64, BufferReaderWithPos<File>>,
    // writer of the current log.
    writer: BufferWriterWithPos<File>,
    // an in-memory [key -> log pointer] map.
    index: HashMap<String, CommandPos>,
}

impl KvStore {
    /// Open the `KvStore` at a given path. Return the KvStore.
    ///
    /// This will create a new directory if the given dir does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialilzation errors during the log re-play.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = HashMap::new();

        let gen_list = sorted_gen_list(&path)?;
        for &gen in &gen_list {
            let mut reader = BufferReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            load(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;

        let writer = new_log_file(&path, current_gen, &mut readers)?;

        Ok(KvStore {
            path,
            current_gen,
            readers,
            writer,
            index,
        })
    }

    /// Set the value of a string key to a string
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        if let Command::Set { key, value: _ } = cmd {
            self.index.insert(
                key,
                CommandPos {
                    gen: self.current_gen,
                    start: pos,
                    length: self.writer.pos - pos,
                },
            );
        }

        Ok(())
    }

    /// Get the string value of the a string key.
    ///
    /// If the key does not exist, return `None`.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::UnexpectedCommandType` if the given command type unexpected.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            // key --> command's start postion
            reader.seek(SeekFrom::Start(cmd_pos.start))?;
            // key --> command's length
            let cmd_reader = reader.take(cmd_pos.length);
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
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;

            if let Command::Remove { key } = cmd {
                self.index.remove(&key);
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

/// Load the whole log file and store value locations in the index map.
fn load(
    gen: u64,
    reader: &mut BufferReaderWithPos<File>,
    index: &mut HashMap<String, CommandPos>,
) -> Result<u64> {
    //  make sure we read from the beginning of the file
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let next_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, value: _ } => {
                index.insert(
                    key,
                    CommandPos {
                        gen,
                        start: pos,
                        length: next_pos - pos,
                    },
                );
            }
            Command::Remove { key } => {
                index.remove(&key);
            }
        }
        pos = next_pos;
    }
    Ok(0)
}

/// Returns sorted generation numbers in the given directory.
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    // TODO: 文件查找与遍历，这个有空就看一下
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();

    gen_list.sort_unstable();
    Ok(gen_list)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

/// Create a new log file with given generation number and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_log_file(
    path: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufferReaderWithPos<File>>,
) -> Result<BufferWriterWithPos<File>> {
    let path = log_path(path, gen);
    let writer = BufferWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(gen, BufferReaderWithPos::new(File::open(&path)?)?);

    Ok(writer)
}

#[derive(Debug)]
/// Represents the positon and length of a json-serialized command in the log.
/// Include the command generation
struct CommandPos {
    gen: u64,
    start: u64,
    length: u64,
}

struct BufferWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
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
        self.pos += len as u64;
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
        // self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufferReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}
