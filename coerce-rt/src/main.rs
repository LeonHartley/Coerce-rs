use std::sync::Arc;

pub struct Aaa {}

impl Test for Aaa {}

pub trait Test {}

fn main() {
    let a: Arc<dyn Test> = Arc::new(Aaa {});
}
