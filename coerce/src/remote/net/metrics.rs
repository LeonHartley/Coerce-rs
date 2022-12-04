pub const METRIC_NETWORK_BYTES_RECV: &str = "coerce_network_bytes_recv";
pub const METRIC_NETWORK_BYTES_SENT: &str = "coerce_network_bytes_sent";

pub struct NetworkMetrics;

impl NetworkMetrics {
    #[inline]
    pub fn incr_bytes_received(len: u64) {
        #[cfg(feature = "metrics")]
        counter!(METRIC_NETWORK_BYTES_RECV, len);
    }

    #[inline]
    pub fn incr_bytes_sent(len: u64) {
        #[cfg(feature = "metrics")]
        counter!(METRIC_NETWORK_BYTES_SENT, len);
    }
}
