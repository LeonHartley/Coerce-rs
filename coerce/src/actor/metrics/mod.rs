//! Actor Metrics
use std::time::Duration;

pub const METRIC_ACTOR_CREATED: &str = "coerce_actor_created";
pub const METRIC_ACTOR_STOPPED: &str = "coerce_actor_stopped";
pub const METRIC_ACTOR_MESSAGES_SENT_TOTAL: &str = "coerce_actor_msg_sent_total";
pub const METRIC_ACTOR_MESSAGE_WAIT_TIME: &str = "coerce_actor_msg_wait_time";
pub const METRIC_ACTOR_MESSAGE_PROCESSING_TIME: &str = "coerce_actor_msg_processing_time";
pub const METRIC_ACTOR_MESSAGES_PROCESSED_TOTAL: &str = "coerce_actor_msg_processed_total";

pub const LABEL_ACTOR_TYPE: &str = "actor_type";
pub const LABEL_MESSAGE_TYPE: &str = "msg_type";

pub struct ActorMetrics;

impl ActorMetrics {
    #[inline]
    pub fn incr_actor_created(actor_type: &'static str) {
        #[cfg(feature = "metrics")]
        increment_counter!(METRIC_ACTOR_CREATED,
            LABEL_ACTOR_TYPE => actor_type,
        );
    }

    #[inline]
    pub fn incr_actor_stopped(actor_type: &'static str) {
        #[cfg(feature = "metrics")]
        increment_counter!(METRIC_ACTOR_STOPPED,
            LABEL_ACTOR_TYPE => actor_type,
        );
    }

    #[inline]
    pub fn incr_messages_sent(actor_type: &'static str, msg_type: &'static str) {
        #[cfg(feature = "metrics")]
        increment_counter!(METRIC_ACTOR_MESSAGES_SENT_TOTAL,
            LABEL_ACTOR_TYPE => actor_type,
            LABEL_MESSAGE_TYPE => msg_type
        );
    }

    #[inline]
    pub fn incr_messages_processed(
        actor_type: &'static str,
        msg_type: &'static str,
        wait_time: Duration,
        processing_time: Duration,
    ) {
        #[cfg(feature = "metrics")]
        {
            increment_counter!(METRIC_ACTOR_MESSAGES_PROCESSED_TOTAL,
                LABEL_ACTOR_TYPE => actor_type,
                LABEL_MESSAGE_TYPE => msg_type
            );

            histogram!(METRIC_ACTOR_MESSAGE_WAIT_TIME,
                wait_time,
                LABEL_ACTOR_TYPE => actor_type,
                LABEL_MESSAGE_TYPE => msg_type);

            histogram!(METRIC_ACTOR_MESSAGE_PROCESSING_TIME,
                processing_time,
                LABEL_ACTOR_TYPE => actor_type,
                LABEL_MESSAGE_TYPE => msg_type)
        }
    }
}
