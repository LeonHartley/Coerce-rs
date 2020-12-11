# Coerce-rs [![crates.io](http://meritbadge.herokuapp.com/coerce)](https://crates.io/crates/coerce) ![coerce-rs](https://action-badges.now.sh/leonhartley/coerce-rs)
Coerce-rs is an asynchronous (async/await) Actor runtime for Rust. It allows for extremely simple yet powerful actor-based multithreaded application development.

### async/await Actors
An actor is just another word for a unit of computation. It can have mutable state, it can receive messages and perform actions.
One caveat though.. It can only do one thing at a time. This can be useful because it can alleviate the need for thread synchronisation,
usually achieved by locking (using `Mutex`, `RwLock` etc). 

#### How is this achieved in Coerce?
Coerce uses Tokio's MPSC channels ([tokio::sync::mpsc::channel][channel]), every actor created spawns a task listening to messages from a
`Receiver`, handling and awaiting the result of the message. Every reference (`ActorRef<A: Actor>`) holds a `Sender<M> where A: Handler<M>`, which can be cloned. 

Actors can be stopped and actor references can be retrieved by ID from anywhere in your application. IDs are `String` but if an ID isn't provided upon creation, a new `Uuid` will be generated. Anonymous actors are automatically dropped (and `Stopped`)
when all references are dropped. Tracked actors (using global fn `new_actor`) must be stopped.


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
        _ctx: &mut ActorContext,
    ) -> String {
        message.0.clone()
    }
}

pub async fn run() {
    let mut actor = new_actor(EchoActor {}).await.unwrap();

    let hello_world = "hello, world".to_string();
    let result = actor.send(EchoMessage(hello_world.clone())).await;
    
    assert_eq!(result, Ok(hello_world));
}
```

### Timer Example
```rust
pub struct EchoActor {}

#[async_trait]
impl Actor for EchoActor {}

pub struct EchoMessage(String);

impl Message for EchoMessage {
    type Result = String;
}

pub struct PrintTimer(String);

impl TimerTick for PrintTimer {}

#[async_trait]
impl Handler<PrintTimer> for EchoActor {
    async fn handle(&mut self, msg: PrintTimer, _ctx: &mut ActorContext) {
        println!("{}", msg.0);
    }
}

pub async fn run() {
    let mut actor = new_actor(EchoActor {}).await.unwrap();
    let hello_world = "hello world!".to_string();

    // print "hello world!" every 5 seconds
    let timer = Timer::start(actor.clone(), Duration::from_secs(5), TimerTick(hello_world));
    
    // timer is stopped when handle is out of scope or can be stopped manually by calling `.stop()`
    delay_for(Duration::from_secs(20)).await;
    timer.stop();
}
```

[channel]: https://docs.rs/tokio/0.2.4/tokio/sync/mpsc/fn.channel.html
