use coerce::actor::message::{FromBytes, ToBytes};

pub trait Key: 'static + Clone + Sync + Send + FromBytes + ToBytes {}

pub trait Value: 'static + Clone + Sync + Send + FromBytes + ToBytes {}

pub trait Snapshot {}

#[derive(Debug)]
pub enum StorageErr {}

#[async_trait]
pub trait Storage: 'static + Sync + Send {
    type Key: Key;

    type Value: Value;

    type Snapshot: Snapshot;

    fn set_last_commit_index(&mut self, commit_index: u64) -> Result<(), StorageErr>;

    fn last_commit_index(&self) -> Option<u64>;

    async fn read(&mut self, key: Self::Key) -> Result<Self::Value, StorageErr>;

    async fn write(&mut self, key: Self::Key, value: Self::Value) -> Result<(), StorageErr>;

    fn snapshot(&mut self) -> Result<Self::Snapshot, StorageErr>;

    fn recover_snapshot(&mut self, snapshot: Self::Snapshot) -> Result<(), StorageErr>;
}
