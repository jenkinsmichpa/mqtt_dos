mod cli;
mod connect_flood;
mod helper;
mod publish_flood;
mod stats;

use std::sync::Arc;

use anyhow::{Result, ensure};
use clap::Parser;

use crate::{
    cli::{Args, AttackScenario},
    stats::Stats,
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.simultaneous_connections > 0,
        "--simultaneous-connections must be greater than zero"
    );
    ensure!(
        args.payload_size <= cli::MAX_PAYLOAD_SIZE,
        "--payload-size must be at most {} bytes",
        cli::MAX_PAYLOAD_SIZE
    );

    let broker_addr = if args.broker_hostname.contains(':') {
        format!("[{}]:{}", args.broker_hostname, args.port)
    } else {
        format!("{}:{}", args.broker_hostname, args.port)
    };

    let stats = Arc::new(Stats::default());

    let workload = async {
        match args.attack_scenario {
            AttackScenario::ConnectFlood => {
                connect_flood::run_connect_flood(&args, &broker_addr, stats.clone()).await?;
            }
            AttackScenario::PublishFlood => {
                publish_flood::run_publish_flood(&args, stats.clone()).await?;
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    tokio::select! {
        result = workload => result?,
        _ = tokio::signal::ctrl_c() => println!("\nInterrupted"),
    }

    stats.print();
    Ok(())
}
