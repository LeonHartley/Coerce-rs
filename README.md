# Coerce-rs
Coerce-rs is an asynchronous (async/await) Actor runtime for Rust. It allows for extremely simple yet powerful actor-based multithreaded application development.

### async/await Actors
An actor is just another word for a unit of computation. It can have mutable state, it can receive messages and perform actions.
One caveat though.. It can only do one thing at a time. This can be useful because it can alleviate the need for thread synchronisation,
usually achieved by locking (using `Mutex`, `RwLock` etc). 

#### How is this achieved in Coerce?
Coerce uses Tokio's MPSC channels ([tokio::sync::mpsc::channel][channel]), every actor created spawns a task listening to messages from a
`Receiver`, handling and awaiting the result of the message. Every reference (`ActorRef<A: Actor>`) holds a `Sender<M> where A: Handler<M>`, which can be cloned. 

### Example
```rust
pub struct EchoActor {}

#[async_trait]
impl Actor for EchoActor {}

pub struct EchoMessage(String);

impl Message for EchoMessage {
    type Result = String;
}

#[async_trait]
impl Handler<EchoMessage> for EchoActor {
    async fn handle(
        &mut self,
        message: EchoMessage,
        _ctx: &mut ActorHandlerContext,
    ) -> String {
        message.0.clone()
    }
}

pub async fn run() {
    let mut actor = new_actor(EchoActor {}).await;

    let hello_world = "hello, world".to_string();
    let result = actor.send(EchoMessage(hello_world.clone())).await;
    
    assert_eq!(result, Ok(hello_world));
}
```

*Distributed Actors currently in development*

[channel]: https://docs.rs/tokio/0.2.4/tokio/sync/mpsc/fn.channel.html
