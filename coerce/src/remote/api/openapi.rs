use crate::remote::api::cluster;
use crate::remote::api::system;
use crate::remote::api::system::actors;

#[derive(OpenApi)]
#[openapi(
    paths(
        system::get_stats,
        actors::get_all,
        cluster::get_nodes,
        sharding::get_sharding_types,
        sharding::get_sharding_stats,
        sharding::get_node_stats,
    ),
    components(
        schemas(
            system::SystemStats,
            system::actors::GetAll,
            system::actors::Actors,
            system::actors::ActorDescription,
            system::actors::Status,
            system::actors::ActorTags,
            system::actors::SupervisedDescription,
            cluster::ClusterNodes,
            cluster::ClusterNode,
            cluster::NodeStatus,
            sharding::cluster::ShardingClusterStats,
            sharding::cluster::ShardingNode,
            sharding::cluster::ShardStats,
            sharding::cluster::ShardHostStatus,
            sharding::cluster::Entity,
            sharding::node::Stats,
            sharding::node::ShardStats,
            sharding::ShardTypes,
        )
    ),
    tags(
        (name = "cluster", description = "Cluster API"),
        (name = "system", description = "System API"),
        (name = "actors", description = "Actors API"),
        (name = "sharding", description = "Sharding API"),
    )
)]
pub struct ApiDoc;

pub mod sharding {
    use crate::remote::api::sharding as sharding_api;
    pub use sharding_api::ShardTypes;

    pub use sharding_api::cluster;
    pub use sharding_api::node;

    pub use sharding_api::cluster::__path_get_sharding_stats;
    pub use sharding_api::cluster::get_sharding_stats;

    pub use sharding_api::node::__path_get_node_stats;
    pub use sharding_api::node::get_node_stats;

    pub use sharding_api::__path_get_sharding_types;
    pub use sharding_api::get_sharding_types;
}
