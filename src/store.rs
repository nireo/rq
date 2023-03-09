use anyhow::Result;
use bytes::Bytes;
use sled::Db;

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
}
