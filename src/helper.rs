use std::time::Duration;

use rand::{
    distr::{Alphanumeric, SampleString},
    rngs::SmallRng,
};

pub(crate) const CLIENT_ID_LENGTH: usize = 16;
const TOPIC_SEGMENT_LENGTH: usize = 8;
pub(crate) const KEEP_ALIVE_SECS: u16 = 60;
pub(crate) const CLEAN_SESSION: bool = false;
pub(crate) const CONNACK_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const EVENTLOOP_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) type Credentials = (String, String);

pub(crate) fn client_id() -> String {
    random_string(&mut rand::make_rng(), CLIENT_ID_LENGTH)
}

pub(crate) fn random_string(rng: &mut SmallRng, len: usize) -> String {
    Alphanumeric.sample_string(rng, len)
}

pub(crate) fn random_topic(rng: &mut SmallRng) -> String {
    format!(
        "{}/{}",
        random_string(rng, TOPIC_SEGMENT_LENGTH),
        random_string(rng, TOPIC_SEGMENT_LENGTH)
    )
}

pub(crate) fn packet_count_desc(total_packets: usize) -> String {
    if total_packets == 0 {
        "an unlimited number".into()
    } else {
        format!("a total of {total_packets}")
    }
}
