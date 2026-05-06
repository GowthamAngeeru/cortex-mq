use anyhow::{anyhow, Result};
use tracing_subscriber::{fmt, EnvFilter};

use cortex_mq::core::broker::{run_server, BrokerConfig};

#[tokio::main]
async fn main()-> Result<()>{
    dotenvy::dotenv().ok();
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("cortex_mq=debug,info"));

    let log_format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());
    let is_json = log_format == "json";

    if is_json {
        fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .with_thread_ids(true)
            .json()
            .init();
    } else {
        fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .with_thread_ids(true)
            .init();
    }

    tracing::info!("cortex_mq_starting_ignition");

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let port: u16 = std::env::var("GRPC_PORT")
        .unwrap_or_else(|_| "50051".to_string())
        .parse()
        .map_err(|_| anyhow!("GRPC_PORT must be a valid port number"))?;
    let virtual_nodes: usize = std::env::var("VIRTUAL_NODES")
        .unwrap_or_else(|_| "150".to_string())
        .parse()
        .unwrap_or(150);

    if virtual_nodes == 0 {
        anyhow::bail!("VIRTUAL_NODES must be strictly greater than 0");
    }

    let heartbeat_interval_secs: u64 = std::env::var("HEARTBEAT_INTERVAL")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);
        
    let missed_heartbeat_threshold: u32 = std::env::var("MISSED_HEARTBEAT_THRESHOLD")
        .unwrap_or_else(|_| "3".to_string())
        .parse()
        .unwrap_or(3);
        
    let max_msg_mb: usize = std::env::var("MAX_MSG_MB")
        .unwrap_or_else(|_| "4".to_string())
        .parse()
        .unwrap_or(4);
    
    if max_msg_mb == 0 || max_msg_mb > 512 {
        anyhow::bail!("MAX_MSG_MB must be safely bounded between 1 and 512 MB");
    }

    let config=BrokerConfig{
        redis_url,
        port,
        virtual_nodes,
        heartbeat_interval_secs,
        missed_heartbeat_threshold,
        max_msg_mb,
    };

    let mode = if is_json { "production" } else { "development" };

    tracing::info!(
        mode = %mode,
        json_logs = is_json,
        virtual_nodes = config.virtual_nodes,
        max_msg_mb = config.max_msg_mb,
        heartbeat_interval = config.heartbeat_interval_secs,
        "cortex_mq_startup_summary"
    );

    run_server(config).await?;

    tracing::info!("cortex_mq_shutdown_complete");
    Ok(())
}