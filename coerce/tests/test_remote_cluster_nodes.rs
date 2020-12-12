#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

use coerce::remote::cluster::node::{RemoteNode, RemoteNodeStore};

use uuid::Uuid;

pub mod util;

#[tokio::test]
pub async fn test_remote_node_store() {
    let node_1 = Uuid::parse_str("a211898f-0cd6-40c4-8faa-2baba8065708").unwrap();
    let node_2 = Uuid::parse_str("6b08a8b6-f57b-4d18-a1e7-effa5b12f487").unwrap();
    let node_3 = Uuid::parse_str("d3fe8ffc-5c03-4965-8ab4-62e7f2115b73").unwrap();
    let node_4 = Uuid::parse_str("c322e826-77ca-435c-9591-3bc25c886277").unwrap();

    let actors = vec![
        "f6445c80-c448-42e0-9aed-097c3e66d0cc",
        "94e54b7c-99ed-4db2-b233-ec39c5ed8fd4",
        "70d2ad99-2af4-4840-8e1e-10d7091e270b",
        "0bc89594-3f25-480f-ab8a-641e1f02f033",
    ];

    let mut nodes = RemoteNodeStore::new(vec![
        RemoteNode::new(node_1.clone(), "127.0.0.1:1024".to_owned()),
        RemoteNode::new(node_2.clone(), "127.0.0.2:1024".to_owned()),
        RemoteNode::new(node_3.clone(), "127.0.0.3:1024".to_owned()),
        RemoteNode::new(node_4.clone(), "127.0.0.4:1024".to_owned()),
    ]);

    let mut nodes_2 = RemoteNodeStore::new(vec![
        RemoteNode::new(node_2.clone(), "127.0.0.2:1024".to_owned()),
        RemoteNode::new(node_4.clone(), "127.0.0.4:1024".to_owned()),
        RemoteNode::new(node_1.clone(), "127.0.0.1:1024".to_owned()),
        RemoteNode::new(node_3.clone(), "127.0.0.3:1024".to_owned()),
    ]);

    assert_eq!(&nodes.get_by_key(actors[0].clone()).unwrap().id, &node_1);
    assert_eq!(&nodes_2.get_by_key(actors[0].clone()).unwrap().id, &node_1);
    assert_eq!(&nodes.get_by_key(actors[1].clone()).unwrap().id, &node_2);
    assert_eq!(&nodes_2.get_by_key(actors[1].clone()).unwrap().id, &node_2);
    assert_eq!(&nodes.get_by_key(actors[2].clone()).unwrap().id, &node_3);
    assert_eq!(&nodes_2.get_by_key(actors[2].clone()).unwrap().id, &node_3);
    assert_eq!(&nodes.get_by_key(actors[3].clone()).unwrap().id, &node_4);
    assert_eq!(&nodes_2.get_by_key(actors[3].clone()).unwrap().id, &node_4);

    //node 2 died!
    nodes.remove(&node_2);
    nodes_2.remove(&node_2);

    assert_eq!(&nodes.get_by_key(actors[0].clone()).unwrap().id, &node_1);
    assert_eq!(&nodes_2.get_by_key(actors[0].clone()).unwrap().id, &node_1);
    assert_ne!(&nodes.get_by_key(actors[1].clone()).unwrap().id, &node_2);
    assert_ne!(&nodes_2.get_by_key(actors[1].clone()).unwrap().id, &node_2);
    assert_eq!(&nodes.get_by_key(actors[2].clone()).unwrap().id, &node_3);
    assert_eq!(&nodes_2.get_by_key(actors[2].clone()).unwrap().id, &node_3);
    assert_eq!(&nodes.get_by_key(actors[3].clone()).unwrap().id, &node_4);
    assert_eq!(&nodes_2.get_by_key(actors[3].clone()).unwrap().id, &node_4);
}
