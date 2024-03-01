use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::Actor;
use coerce::persistent::journal::storage::JournalEntry;
use redis::aio::ConnectionLike;

use tokio::sync::oneshot::Sender;

pub(crate) struct RedisJournal<C>(pub C);

pub(crate) struct Write {
    pub key: String,
    pub entry: JournalEntry,
    pub result_channel: Sender<anyhow::Result<()>>,
}

impl Message for Write {
    type Result = ();
}

pub(crate) struct WriteBatch {
    pub key: String,
    pub entries: Vec<JournalEntry>,
    pub result_channel: Sender<anyhow::Result<()>>,
}

impl Message for WriteBatch {
    type Result = ();
}

pub(crate) struct ReadSnapshot(pub String, pub Sender<anyhow::Result<Option<JournalEntry>>>);

impl Message for ReadSnapshot {
    type Result = ();
}

pub(crate) struct ReadMessages {
    pub key: String,
    pub start_sequence: Option<i64>,
    pub end_sequence: Option<i64>,
    pub result_channel: Sender<anyhow::Result<Option<Vec<JournalEntry>>>>,
}

impl Message for ReadMessages {
    type Result = ();
}

pub(crate) struct ReadMessage {
    pub key: String,
    pub sequence_id: i64,
    pub result_channel: Sender<anyhow::Result<Option<JournalEntry>>>,
}

impl Message for ReadMessage {
    type Result = ();
}

pub(crate) struct Delete(pub Vec<String>);

impl Message for Delete {
    type Result = anyhow::Result<()>;
}

pub(crate) struct DeleteRange {
    pub key: String,
    pub start_sequence: i64,
    pub end_sequence: i64,
    pub result_channel: Sender<anyhow::Result<()>>,
}

impl Message for DeleteRange {
    type Result = ();
}

impl<C: 'static + Send + Sync> Actor for RedisJournal<C> where C: Clone {}

#[async_trait]
impl<C: 'static + ConnectionLike + Send + Sync> Handler<Write> for RedisJournal<C>
where
    C: Clone,
{
    async fn handle(&mut self, message: Write, _ctx: &mut ActorContext) {
        let connection = self.0.clone();
        let _ = tokio::spawn(async move {
            let mut connection = connection;
            if let Err(e) = redis::cmd("ZADD")
                .arg(message.key)
                .arg(message.entry.sequence)
                .arg(&message.entry.write_to_bytes().expect("serialized journal"))
                .query_async::<C, ()>(&mut connection)
                .await
            {
                let err = anyhow::Error::new(e);
                let _ = message.result_channel.send(Err(err));
            } else {
                let _ = message.result_channel.send(Ok(()));
            }
        });
    }
}

#[async_trait]
impl<C: 'static + ConnectionLike + Send + Sync> Handler<WriteBatch> for RedisJournal<C>
where
    C: Clone,
{
    async fn handle(&mut self, message: WriteBatch, _ctx: &mut ActorContext) {
        let connection = self.0.clone();
        let _ = tokio::spawn(async move {
            let mut connection = connection;

            let mut cmd = redis::cmd("ZADD");

            cmd.arg(message.key);

            for entry in message.entries {
                cmd.arg(entry.sequence)
                    .arg(entry.write_to_bytes().expect("serialized journal"));
            }

            if let Err(e) = cmd.query_async::<C, ()>(&mut connection).await {
                let _ = message.result_channel.send(Err(anyhow::Error::new(e)));
            } else {
                let _ = message.result_channel.send(Ok(()));
            }
        });
    }
}

#[async_trait]
impl<C: 'static + ConnectionLike + Send + Sync> Handler<DeleteRange> for RedisJournal<C>
where
    C: Clone,
{
    async fn handle(&mut self, message: DeleteRange, _ctx: &mut ActorContext) {
        let connection = self.0.clone();
        let _ = tokio::spawn(async move {
            let mut connection = connection;
            if let Err(e) = redis::cmd("ZREMRANGEBYSCORE")
                .arg(message.key)
                .arg(message.start_sequence)
                .arg(message.end_sequence)
                .query_async::<C, ()>(&mut connection)
                .await
            {
                let err = anyhow::Error::new(e);
                let _ = message.result_channel.send(Err(err));
            } else {
                let _ = message.result_channel.send(Ok(()));
            }
        });
    }
}

#[async_trait]
impl<C: 'static + ConnectionLike + Send + Sync> Handler<ReadSnapshot> for RedisJournal<C>
where
    C: Clone,
{
    async fn handle(&mut self, message: ReadSnapshot, _ctx: &mut ActorContext) {
        let connection = self.0.clone();
        let _ = tokio::spawn(async move {
            let mut connection = connection;

            let data = redis::cmd("ZRANGE")
                .arg(message.0)
                .arg("+inf")
                .arg("-inf")
                .arg("BYSCORE")
                .arg("REV")
                .arg(&["LIMIT", "0", "1"])
                .query_async::<C, Option<Vec<Vec<u8>>>>(&mut connection)
                .await;

            match data {
                Ok(data) => {
                    let _ = message.1.send(Ok(
                        data.and_then(|b| b.into_iter().next().and_then(read_journal_entry))
                    ));
                }
                Err(err) => {
                    let err = anyhow::Error::new(err);
                    let _ = message.1.send(Err(err));
                }
            }
        });
    }
}

#[async_trait]
impl<C: 'static + ConnectionLike + Send + Sync> Handler<ReadMessage> for RedisJournal<C>
where
    C: Clone,
{
    async fn handle(&mut self, message: ReadMessage, _ctx: &mut ActorContext) {
        let connection = self.0.clone();
        let _ = tokio::spawn(async move {
            let mut connection = connection;

            let data = redis::cmd("ZRANGE")
                .arg(message.key)
                .arg(message.sequence_id)
                .arg(message.sequence_id)
                .arg("BYSCORE")
                .query_async::<C, Option<Vec<Vec<u8>>>>(&mut connection)
                .await;

            match data {
                Ok(data) => {
                    let _ = message.result_channel.send(Ok(
                        data.and_then(|b| b.into_iter().next().and_then(read_journal_entry))
                    ));
                }
                Err(err) => {
                    let err = anyhow::Error::new(err);
                    let _ = message.result_channel.send(Err(err));
                }
            }
        });
    }
}

#[async_trait]
impl<C: 'static + ConnectionLike + Send + Sync> Handler<ReadMessages> for RedisJournal<C>
where
    C: Clone,
{
    async fn handle(&mut self, message: ReadMessages, _ctx: &mut ActorContext) {
        let connection = self.0.clone();
        let _ = tokio::spawn(async move {
            let mut connection = connection;

            let from_sequence = message.start_sequence.unwrap_or(0);
            let end_sequence = message
                .end_sequence
                .map_or("+inf".to_string(), |s| format!("{}", s));

            let data = redis::cmd("ZRANGE")
                .arg(message.key)
                .arg(from_sequence)
                .arg(end_sequence)
                .arg("BYSCORE")
                .query_async::<C, Option<Vec<Vec<u8>>>>(&mut connection)
                .await;

            match data {
                Ok(data) => {
                    let _ = message.result_channel.send(Ok(
                        data.and_then(|b| b.into_iter().map(read_journal_entry).collect())
                    ));
                }
                Err(err) => {
                    let err = anyhow::Error::new(err);
                    let _ = message.result_channel.send(Err(err));
                }
            }
        });
    }
}

#[async_trait]
impl<C: 'static + ConnectionLike + Send + Sync> Handler<Delete> for RedisJournal<C>
where
    C: Clone,
{
    async fn handle(&mut self, message: Delete, _ctx: &mut ActorContext) -> anyhow::Result<()> {
        let _ = redis::cmd("DEL")
            .arg(message.0)
            .query_async(&mut self.0)
            .await?;

        Ok(())
    }
}

fn read_journal_entry(redis_value: Vec<u8>) -> Option<JournalEntry> {
    Some(JournalEntry::read_from_bytes(redis_value).unwrap())
}
