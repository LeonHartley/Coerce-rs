#[macro_use]
extern crate serde;

#[macro_use]
extern crate async_trait;

use coerce::remote::cluster::node::{RemoteNode, RemoteNodeStore};

pub mod util;

#[tokio::test]
pub async fn test_remote_node_store() {
    let node_1 = 1;
    let node_2 = 2;
    let node_3 = 3;
    let node_4 = 4;

    let actors = vec![
        "f6445c80-c448-42e0-9aed-097c3e66d0cc",
        "94e54b7c-99ed-4db2-b233-ec39c5ed8fd4",
        "70d2ad99-2af4-4840-8e1e-10d7091e270b",
        "abc89594-3f25-480f-ab8a-641e1f02f033",
    ];

    let mut nodes = RemoteNodeStore::new(vec![
        RemoteNode::new(node_1, "127.0.0.1:1024".to_owned()),
        RemoteNode::new(node_2, "127.0.0.2:1024".to_owned()),
        RemoteNode::new(node_3, "127.0.0.3:1024".to_owned()),
        RemoteNode::new(node_4, "127.0.0.4:1024".to_owned()),
    ]);

    let mut nodes_2 = RemoteNodeStore::new(vec![
        RemoteNode::new(node_2, "127.0.0.2:1024".to_owned()),
        RemoteNode::new(node_4, "127.0.0.4:1024".to_owned()),
        RemoteNode::new(node_1, "127.0.0.1:1024".to_owned()),
        RemoteNode::new(node_3, "127.0.0.3:1024".to_owned()),
    ]);

    assert_eq!(&nodes.get_by_key(actors[0]).unwrap().id, &node_1);
    assert_eq!(&nodes_2.get_by_key(actors[0]).unwrap().id, &node_1);
    assert_eq!(&nodes.get_by_key(actors[1]).unwrap().id, &node_1);
    assert_eq!(&nodes_2.get_by_key(actors[1]).unwrap().id, &node_1);
    assert_eq!(&nodes.get_by_key(actors[2]).unwrap().id, &node_3);
    assert_eq!(&nodes_2.get_by_key(actors[2]).unwrap().id, &node_3);
    assert_eq!(&nodes.get_by_key(actors[3]).unwrap().id, &node_4);
    assert_eq!(&nodes_2.get_by_key(actors[3]).unwrap().id, &node_4);

    //node 2 died!
    nodes.remove(&node_1);
    nodes_2.remove(&node_1);

    assert_eq!(&nodes.get_by_key(actors[0]).unwrap().id, &node_3);
    assert_eq!(&nodes_2.get_by_key(actors[0]).unwrap().id, &node_3);
    assert_ne!(&nodes.get_by_key(actors[1]).unwrap().id, &node_2);
    assert_ne!(&nodes_2.get_by_key(actors[1]).unwrap().id, &node_2);
    assert_eq!(&nodes.get_by_key(actors[2]).unwrap().id, &node_3);
    assert_eq!(&nodes_2.get_by_key(actors[2]).unwrap().id, &node_3);
    assert_eq!(&nodes.get_by_key(actors[3]).unwrap().id, &node_4);
    assert_eq!(&nodes_2.get_by_key(actors[3]).unwrap().id, &node_4);
}
