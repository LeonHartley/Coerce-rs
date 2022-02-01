// #[macro_use]
// extern crate serde;
//
// #[macro_use]
// extern crate async_trait;
//
// use coerce::actor::system::ActorSystem;
// use coerce::remote::cluster::node::RemoteNode;
// use coerce::remote::cluster::workers::{
//     ClusterWorker, ClusterWorkers, GetActiveWorkers, WorkerStore, WorkerStoreErr,
// };
//
// pub mod util;
//
// pub struct TestWorkerStore;
//
// #[async_trait]
// impl WorkerStore for TestWorkerStore {
//     async fn get_active(&mut self) -> Result<Vec<ClusterWorker>, WorkerStoreErr> {
//         let node_1 = 1;
//         let node_2 = 2;
//
//         Ok(vec![
//             ClusterWorker::new(
//                 node_1,
//                 RemoteNode::new(node_1, "127.0.0.1:30101".to_owned(), None),
//                 None,
//             ),
//             ClusterWorker::new(
//                 node_2,
//                 RemoteNode::new(node_2, "127.0.0.1:30102".to_owned(), None),
//                 None,
//             ),
//         ])
//     }
//
//     async fn put(&mut self, _worker: &ClusterWorker) -> Result<(), WorkerStoreErr> {
//         unimplemented!()
//     }
//
//     async fn remove(&mut self, _worker: &ClusterWorker) -> Result<(), WorkerStoreErr> {
//         unimplemented!()
//     }
// }
//
// #[tokio::test]
// pub async fn test_remote_worker_store() {
//     let mut system = ActorSystem::new();
//     let workers = ClusterWorkers::new(TestWorkerStore, &mut system)
//         .await
//         .unwrap();
//
//     let active_workers = workers.send(GetActiveWorkers).await.unwrap().unwrap();
//
//     assert_eq!(active_workers.len(), 2);
// }
