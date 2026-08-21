use std::fmt;

use clap::{Parser, ValueEnum};
use rumqttc::QoS;

use crate::helper::Credentials;

pub const MAX_PAYLOAD_SIZE: usize = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum AttackScenario {
    #[value(alias = "connect_flood")]
    ConnectFlood,
    #[value(alias = "publish_flood")]
    PublishFlood,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum ProtocolVersion {
    /// MQTT 3.1.1
    #[value(name = "v4", alias = "4")]
    V4,
    /// MQTT 5
    #[value(name = "v5", alias = "5")]
    V5,
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::V4 => write!(formatter, "4"),
            Self::V5 => write!(formatter, "5"),
        }
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum QoSLevel {
    #[value(alias = "0")]
    AtMostOnce,
    #[value(alias = "1")]
    AtLeastOnce,
    #[value(alias = "2")]
    ExactlyOnce,
}

impl From<QoSLevel> for QoS {
    fn from(level: QoSLevel) -> Self {
        match level {
            QoSLevel::AtMostOnce => Self::AtMostOnce,
            QoSLevel::AtLeastOnce => Self::AtLeastOnce,
            QoSLevel::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Username for MQTT connection
    #[arg(short = 'u', long, requires = "password")]
    pub username: Option<String>,

    /// Password for MQTT connection
    #[arg(short = 'p', long, requires = "username")]
    pub password: Option<String>,

    /// Broker hostname
    #[arg(short = 'b', long, default_value = "localhost")]
    pub broker_hostname: String,

    /// Port of the MQTT broker
    #[arg(short = 'P', long, default_value_t = 1883)]
    pub port: u16,

    /// Number of simultaneous connections
    #[arg(short = 'c', long, default_value_t = 1000)]
    pub simultaneous_connections: usize,

    /// Total number of packets (0 for infinite)
    #[arg(short = 'n', long, default_value_t = 10000)]
    pub total_packets: usize,

    /// Delay between spawning connections in milliseconds
    #[arg(short = 's', long, default_value_t = 0)]
    pub spawn_delay: u64,

    /// Delay between sending packets in milliseconds
    #[arg(short = 'd', long, default_value_t = 0)]
    pub send_delay: u64,

    /// MQTT protocol version
    #[arg(short = 'v', long, default_value = "v4")]
    pub protocol_version: ProtocolVersion,

    /// Quality of Service level
    #[arg(short = 'q', long, default_value = "at-least-once")]
    pub qos_level: QoSLevel,

    /// Payload size in bytes
    #[arg(short = 'z', long, default_value_t = 1000)]
    pub payload_size: usize,

    /// Attack scenario
    #[arg(short = 'a', long, default_value = "connect-flood")]
    pub attack_scenario: AttackScenario,
}

impl Args {
    pub fn credentials(&self) -> Option<Credentials> {
        Some((self.username.clone()?, self.password.clone()?))
    }
}
