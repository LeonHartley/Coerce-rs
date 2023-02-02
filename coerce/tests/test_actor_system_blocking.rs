use crate::util::TestActor;
use coerce::actor::scheduler::ActorType;
use coerce::actor::system::ActorSystem;
use coerce::actor::ToActorId;
use tokio::runtime::Runtime;

mod util;

#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

#[ignore]
#[test]
pub fn test_actor_system_blocking() {
    let runtime = Runtime::new().unwrap();
    let system = runtime.block_on(async {
        ActorSystem::new();
    });

    let actor = system.new_actor_blocking(
        "test".to_actor_id(),
        TestActor {
            status: None,
            counter: 0,
        },
        ActorType::Tracked,
    );

    assert!(actor.is_ok());
}
