#![allow(dead_code)]
use anyhow::{Ok, Result};
use serde::{Deserialize, Serialize};
use std::io::BufRead;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Seek, Write},
    slice::Iter,
};

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Nop,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Transaction {
    id: usize,
    command: Command,
}

trait WriteAheadLog {
    fn append(&mut self, transaction: &Transaction) -> Result<()>;
}

// In memory

#[derive(Debug)]
struct InMemoryWriteAheadLog {
    data: Vec<Transaction>,
}

struct InMemoryReplayIterator<'a> {
    iter: Iter<'a, Transaction>,
}

impl Iterator for InMemoryReplayIterator<'_> {
    type Item = Result<Transaction>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().cloned().map(Ok)
    }
}

impl InMemoryWriteAheadLog {
    fn new() -> Self {
        Self { data: vec![] }
    }

    fn replay(&mut self) -> InMemoryReplayIterator {
        InMemoryReplayIterator {
            iter: self.data.iter(),
        }
    }
}

impl WriteAheadLog for InMemoryWriteAheadLog {
    fn append(&mut self, transaction: &Transaction) -> Result<()> {
        self.data.push(transaction.clone());
        Ok(())
    }
}

// On disk

#[derive(Debug)]
struct OnDiskWriteAheadLog {
    file: File,
}
struct OnDiskReplayIterator<'a> {
    reader: BufReader<&'a mut File>,
    error: bool,
}

impl Iterator for OnDiskReplayIterator<'_> {
    type Item = Result<Transaction>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.error {
            return None;
        }

        let mut buffer = String::new();
        match self.reader.read_line(&mut buffer) {
            Result::Ok(length) if length > 0 => match serde_json::from_str(&buffer) {
                Result::Ok(t) => Some(Result::Ok(t)),
                Result::Err(e) => {
                    self.error = true;
                    Some(Result::Err(e.into()))
                }
            },
            Result::Ok(_) => None,
            Result::Err(e) => {
                self.error = true;
                Some(Result::Err(e.into()))
            }
        }
    }
}

impl OnDiskWriteAheadLog {
    fn new(f: File) -> Self {
        Self { file: f }
    }
    fn replay(&mut self) -> OnDiskReplayIterator {
        let _ = self.file.rewind();
        OnDiskReplayIterator {
            reader: BufReader::new(&mut self.file),
            error: false,
        }
    }
}

impl WriteAheadLog for OnDiskWriteAheadLog {
    fn append(&mut self, transaction: &Transaction) -> Result<()> {
        writeln!(self.file, "{}", serde_json::to_string(transaction)?)?;
        self.file.sync_data()?;
        Ok(())
    }
}

#[derive(Debug)]
struct State {
    kv: HashMap<String, String>,
}

impl State {
    fn apply(&mut self, transaction: &Transaction) -> String {
        let default = "".into();
        match &transaction.command {
            Command::Set { key, value } => {
                self.kv.insert(key.into(), value.into());
                default
            }
            Command::Get { key } => self.kv.get(key).unwrap_or(&default).into(),
            _ => default,
        }
    }
}

#[derive(Debug)]
struct Server<W: WriteAheadLog> {
    transaction_id: usize,
    write_ahead_log: W,
    state: State,
}

impl<W: WriteAheadLog> Server<W> {
    fn new(write_ahead_log: W) -> Self {
        Self {
            transaction_id: 0,
            write_ahead_log,
            state: State { kv: HashMap::new() },
        }
    }

    fn execute(&mut self, query: &str) -> Result<String> {
        let command = Self::parse(query);
        let transaction = Transaction {
            id: self.transaction_id,
            command,
        };
        self.transaction_id += 1;
        self.write_ahead_log.append(&transaction)?;
        let result = self.state.apply(&transaction);
        Ok(result) //self.format(result)
    }

    fn parse(query: &str) -> Command {
        let parts: Vec<&str> = query.split_whitespace().collect();
        match parts[..] {
            ["GET", key] => Command::Get { key: key.into() },
            ["SET", key, value] => Command::Set {
                key: key.into(),
                value: value.into(),
            },
            _ => Command::Nop,
        }
    }
}

fn main() -> Result<()> {
    // In memory
    // let w = InMemoryWriteAheadLog::new();

    // On disk
    let w = OnDiskWriteAheadLog::new(
        File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open("wal.txt")?,
    );

    let mut s = Server::new(w);

    s.execute("SET my_key 3")?;
    println!("{}", s.execute("GET my_key")?);

    for x in s.write_ahead_log.replay() {
        println!("REPLAY: {:?}", x);
    }

    s.execute("SET somekey 4")?;
    println!("{}", s.execute("GET my_key")?);

    println!("{:?}", s);
    Ok(())
}
