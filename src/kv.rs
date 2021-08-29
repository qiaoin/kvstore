use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::{KvsError, Result};

// 1MB
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

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
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
///
/// let mut store = KvStore::open(current_dir()?)?;
///
/// store.set("key1".to_owned(), "value1".to_owned());
/// assert_eq!(store.get("key1".to_owned())?, Some("value1".to_owned()));
/// assert_eq!(store.get("key2".to_owned())?, None);
///
/// store.remove("key1".to_owned());
/// assert_eq!(store.get("key1".to_owned())?, None);
/// # Ok(())
/// # }
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
    // stale log size
    uncompacted: u64,
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
        let mut uncompacted = 0;
        for &gen in &gen_list {
            let mut reader = BufferReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load(gen, &mut reader, &mut index)?;
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
            uncompacted,
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
            if let Some(old_cmd) = self
                .index
                .insert(key, CommandPos::new(self.current_gen, pos, self.writer.pos))
            {
                self.uncompacted += old_cmd.length;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
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
                // key 在之前的 if 已经判断为存在，这里 remove 一定会返回 Some，否则可以直接 panic
                let old_cmd = self.index.remove(&key).expect("remove key not found");
                self.uncompacted += old_cmd.length;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    fn compact(&mut self) -> Result<()> {
        // compaction generateion
        let compaction_gen = self.current_gen + 1;

        // current generation number +2, +1 for compaction
        self.current_gen += 2;
        self.writer = self.new_log_file(self.current_gen)?;

        let mut compaction_writer = self.new_log_file(compaction_gen)?;

        // compaction log 从 pos = 0 开始写入
        let mut next_pos = 0;
        // 遍历目前 in-memory index 中保存的 key 对应的 CommandPos
        for active_cmd in &mut self.index.values_mut() {
            // 根据 gen 拿到对应的 reader
            let reader = self
                .readers
                .get_mut(&active_cmd.gen)
                .expect("Cannot find the reader");
            // 读取 log 中对应的 Command
            // 判断当前 reader 的游标位置，读取对应的 Command 是否需要移动游标
            if active_cmd.start != reader.pos {
                // 需要移动移动游标
                reader.seek(SeekFrom::Start(active_cmd.start))?;
            }
            let mut entry_reader = reader.take(active_cmd.length);
            // 将对应 reader 中的内容，copy 到 compaction_reader 中来
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;

            // 更新 in-memory index 中 CommandPos 对应的信息
            *active_cmd = CommandPos::new(compaction_gen, next_pos, next_pos + len);

            next_pos += len;
        }

        // 释放 stale 的空间
        let stale_gen_list: Vec<_> = self
            .readers
            .keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();
        for stale_gen in stale_gen_list {
            // 将 log 文件对应的 reader 释放掉
            self.readers.remove(&stale_gen);

            // 将 log file 也给释放掉
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }

        // 重置
        self.uncompacted = 0;

        Ok(())
    }

    fn new_log_file(&mut self, gen: u64) -> Result<BufferWriterWithPos<File>> {
        new_log_file(&self.path, gen, &mut self.readers)
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
    // number of bytes that can be saved after a compaction
    let mut uncompacted = 0;
    while let Some(cmd) = stream.next() {
        let next_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, value: _ } => {
                if let Some(old_cmd) = index.insert(key, CommandPos::new(gen, pos, next_pos)) {
                    uncompacted += old_cmd.length;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.length;
                }

                // 这里是一个优化
                // the "remove" command itself can be deleted in the next compaction.
                // so we add its length to `uncompacted`
                uncompacted += next_pos - pos;
            }
        }
        pos = next_pos;
    }
    Ok(uncompacted)
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

impl CommandPos {
    fn new(gen: u64, start: u64, end: u64) -> Self {
        CommandPos {
            gen,
            start,
            length: end - start,
        }
    }
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
