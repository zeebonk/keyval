use anyhow::{Ok, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::{BufReader, Seek, SeekFrom, Write}};
use std::io::{self, BufRead};


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
    write_ahead_log: &'a InMemoryWriteAheadLog,
    cur: usize,
}

impl<'a> Iterator for InMemoryReplayIterator<'a> {
    type Item = Transaction;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur == self.write_ahead_log.data.len() {
            return None
        }
        let result = self.write_ahead_log.data[self.cur].clone();
        self.cur += 1;
        Some(result)
    }
}

impl InMemoryWriteAheadLog {
    fn new() -> Self {
        Self { data: vec![] }
    }

    fn replay(&mut self) -> InMemoryReplayIterator {
        InMemoryReplayIterator { write_ahead_log: self, cur: 0 }
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
}

impl Iterator for OnDiskReplayIterator<'_> {
    type Item = Transaction;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = String::new();
        let result = self.reader.read_line(&mut buffer);
        match result {
            Result::Ok(length) if length > 0 => {
                Some(serde_json::from_str(buffer.as_str()).unwrap())
            }
            _ => {
                println!("{:?}", result);
                None
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
        OnDiskReplayIterator { reader: BufReader::new(&mut self.file) }
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
        let result = match &transaction.command {
            Command::Set { key, value } => {
                self.kv.insert(key.into(), value.into());
                ""
            }
            Command::Get { key } => {
                let result = self.kv.get(key);
                result.unwrap()
            }
            _ => "",
        };
        result.into()
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
        match parts[0] {
            "GET" => Command::Get {
                key: parts[1].into(),
            },
            "SET" => Command::Set {
                key: parts[1].into(),
                value: parts[2].into(),
            },
            _ => Command::Nop,
        }
    }
}

fn main() -> Result<()> {
    // In memory
    //let w = InMemoryWriteAheadLog::new();

    // On disk
    let w = OnDiskWriteAheadLog::new(File::options().create(true).read(true).write(true).open("wal.txt")?);

    let mut s = Server::new(w);

    s.execute("SET my_key 3")?;
    println!("{}", s.execute("GET my_key")?);

    for x in s.write_ahead_log.replay() {
        println!("{:?}", x);
    }

    s.execute("SET somekey 4")?;
    println!("{}", s.execute("GET my_key")?);

    println!("{:?}", s);
    Ok(())
}
