use crate::remote::api::cluster;
use crate::remote::api::system;
use crate::remote::api::system::actors;

#[derive(OpenApi)]
#[openapi(
    paths(
        system::health,
        system::get_stats,
        actors::get_all,
        cluster::get_nodes,
    ),
    components(
        schemas(
            system::SystemHealth,
            system::HealthStatus,
            system::SystemStats,
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
        )
    ),
    tags(
        (name = "system", description = "System API"),
    )
)]
pub struct SystemApiDoc;

#[cfg(feature = "sharding")]
pub mod sharding {
    #[derive(OpenApi)]
    #[openapi(
        paths(
            get_sharding_types,
            get_sharding_stats,
            get_node_stats,
        ),
        components(
            schemas(
                cluster::ShardingClusterStats,
                cluster::ShardingNode,
                cluster::ShardStats,
                cluster::ShardHostStatus,
                cluster::Entity,
                node::Stats,
                node::ShardStats,
                ShardTypes,
            )
        ),
        tags(
            (name = "sharding", description = "Sharding API"),
        )
    )]
    pub struct ShardingApiDoc;

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
