pub const METRIC_NETWORK_BYTES_RECV: &str = "coerce_network_bytes_recv";
pub const METRIC_NETWORK_BYTES_SENT: &str = "coerce_network_bytes_sent";

pub const LABEL_SRC_ADDR: &str = "src_addr";
pub const LABEL_DEST_ADDR: &str = "dest_addr";

pub struct NetworkMetrics;

impl NetworkMetrics {
    #[inline]
    pub fn incr_bytes_received(len: u64, src_addr: &str) {
        #[cfg(feature = "metrics")]
        counter!(
            METRIC_NETWORK_BYTES_RECV,
            LABEL_SRC_ADDR => src_addr.to_owned()
        )
        .increment(len)
    }

    #[inline]
    pub fn incr_bytes_sent(len: u64, dest_addr: &str) {
        #[cfg(feature = "metrics")]
        counter!(
            METRIC_NETWORK_BYTES_SENT,
            LABEL_DEST_ADDR => dest_addr.to_owned()
        )
        .increment(len);
    }
}
