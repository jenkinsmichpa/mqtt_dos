use std::time::Duration;

use rand::{RngExt, distr::Alphanumeric, rngs::SmallRng};

pub const CLIENT_ID_LENGTH: usize = 16;
const TOPIC_SEGMENT_LENGTH: usize = 8;
pub const KEEP_ALIVE_SECS: u16 = 60;
pub const CLEAN_SESSION: bool = false;
pub const CONNACK_TIMEOUT: Duration = Duration::from_secs(10);
pub const EVENTLOOP_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub type Credentials = (String, String);

pub fn client_id() -> String {
    random_string(&mut rand::make_rng(), CLIENT_ID_LENGTH)
}

pub fn random_string(rng: &mut SmallRng, len: usize) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn random_topic(rng: &mut SmallRng) -> String {
    format!(
        "{}/{}",
        random_string(rng, TOPIC_SEGMENT_LENGTH),
        random_string(rng, TOPIC_SEGMENT_LENGTH)
    )
}

pub fn packet_count_desc(total_packets: usize) -> String {
    if total_packets == 0 {
        "an unlimited number".into()
    } else {
        format!("a total of {total_packets}")
    }
}
