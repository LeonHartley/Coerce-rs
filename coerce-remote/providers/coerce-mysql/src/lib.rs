use crate::MySqlWorkerStoreErr::Database;
use coerce_remote::cluster::workers::{ClusterWorker, WorkerStore, WorkerStoreErr};
use mysql_async::{error::Error, Pool};

#[macro_use]
extern crate async_trait;

pub mod schema;

pub struct MySqlWorkerStore {
    pool: Pool,
}

impl MySqlWorkerStore {
    pub async fn new(mysql_url: String, create_tables: bool) -> Result<MySqlWorkerStore, MySqlWorkerStoreErr> {
        let pool = create_pool(mysql_url, create_tables).await?;

        Ok(MySqlWorkerStore { pool })
    }
}

#[async_trait]
impl WorkerStore for MySqlWorkerStore {
    async fn get_active(&mut self) -> Result<Vec<ClusterWorker>, WorkerStoreErr> {
        unimplemented!()
    }

    async fn put(&mut self, worker: &ClusterWorker) -> Result<(), WorkerStoreErr> {
        unimplemented!()
    }

    async fn remove(&mut self, worker: &ClusterWorker) -> Result<(), WorkerStoreErr> {
        unimplemented!()
    }
}

async fn create_pool(mysql_url: String, create_tables: bool) -> Result<Pool, MySqlWorkerStoreErr> {
    let pool = Pool::new(mysql_url);
    let conn = pool.get_conn().await?;

    Ok(pool)
}

pub enum MySqlWorkerStoreErr {
    Database(String),
}

impl From<Error> for MySqlWorkerStoreErr {
    fn from(e: Error) -> Self {
        MySqlWorkerStoreErr::Database(format!("{:?}", e))
    }
}

impl From<MySqlWorkerStoreErr> for WorkerStoreErr {
    fn from(e: MySqlWorkerStoreErr) -> Self {
        match e {
            Database(e) => WorkerStoreErr::Database(e),
        }
    }
}
