use std::path::Path;

use anyhow::Result;
use bytes::Bytes;
use sled::Db;

const TOPIC_FMT: &str = "t-{}-{}";
const ACK_TOPIC_FMT: &str = "t-{}-ack-{}";

pub struct Value {
    dacks: usize,
    raw: Bytes,
}

impl Value {
    fn new(raw: Bytes) -> Self {
        Self { raw, dacks: 0 }
    }
}

pub trait Store {
    fn insert(&self, topic: &str, value: &Value) -> Result<()>;

    fn next(&self, topic: &str) -> Result<(Value, usize)>;

    fn ack(&self, topic: &str, offset: usize) -> Result<()>;

    fn nack(&self, topic: &str, offset: usize) -> Result<()>;

    fn back(&self, topic: &str, offset: usize) -> Result<()>;

    fn dack(&self, topic: &str, offset: usize) -> Result<()>;

    fn purge(&self, topic: &str) -> Result<()>;
}

pub struct SledStore {
    db: Db,
    path: Path,
}

impl Store for SledStore {
    fn ack(&self, topic: &str, offset: usize) -> Result<()> {
        let key = format!("t-{}-ack-{}", topic, offset);
        self.db.remove(key)?;

        Ok(())
    }

    fn nack(&self, topic: &str, offset: usize) -> Result<()> {
        Ok(())
    }

    fn purge(&self, topic: &str) -> Result<()> {
        let mut batch = sled::Batch::default();
        let prefix = format!("t-{}", topic).as_bytes();
        let mut prefix_iter = self.db.scan_prefix(prefix);

        loop {
            let entry = prefix_iter.next();
            if entry.is_none() {
                break;
            }

            let entry = entry.unwrap();
            if entry.is_err() {
                break;
            }
            let entry = entry.unwrap();
        }

        Ok(())
    }
}
