use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tonic::transport::Server;
use tracing::{error, info, warn};

use crate::api::pb::broker_service_server::BrokerServiceServer;
use crate::api::BrokerServiceImpl;
use crate::core::state::GlobalState;

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub redis_url: String,
    pub port: u16,
    pub virtual_nodes: usize,
    pub heartbeat_interval_secs: u64,
    pub missed_heartbeat_threshold: u32,
    pub max_msg_mb: usize,
}

fn mask_url(url: &str) -> String {
    if let Some(start) = url.find(r"://") {
        if let Some(end) = url.find('@') {
            return format!("{}://***{}", &url[..start], &url[end..]);
        }
    }
    url.to_string()
}

pub async fn run_server(config: BrokerConfig) -> Result<()> {
    let addr = format!("0.0.0.0:{}", config.port).parse()?;

    info!(
        redis_url = %mask_url(&config.redis_url),
        port = config.port,
        virtual_nodes = config.virtual_nodes,
        "broker: bootstrapping_aetheros_runtime"
    );

    let pg_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/cortex".to_string());
    let state = GlobalState::new(&config.redis_url, &pg_url, config.virtual_nodes).await?;
    info!("broker: global_state_initialized");

    let monitor_state = state.clone();
    let interval_secs = config.heartbeat_interval_secs;
    let threshold = config.missed_heartbeat_threshold;

    tokio::spawn(async move {
        let handle = tokio::spawn(run_heartbeat_monitor(
            monitor_state,
            interval_secs,
            threshold,
        ));
        if let Err(e) = handle.await {
            error!(error = ?e, "broker: CRITICAL_heartbeat_monitor_panicked_or_crashed");
        }
    });

    let broker_service = BrokerServiceImpl::new(state);
    let max_bytes = config.max_msg_mb * 1024 * 1024;

    let svc = BrokerServiceServer::new(broker_service)
        .max_decoding_message_size(max_bytes)
        .max_encoding_message_size(max_bytes);

    info!(address = %addr, "broker: grpc_listener_active");

    Server::builder()
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .add_service(svc)
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;
    info!("broker: shutdown_sequence_complete");
    Ok(())
}

async fn run_heartbeat_monitor(state: GlobalState, interval_secs: u64, threshold: u32) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        interval_secs,
        miss_threshold = threshold,
        "broker: heartbeat_monitor_started"
    );

    loop {
        interval.tick().await;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let nodes = state.get_active_nodes().await;

        for node in nodes {
            if now - node.last_heartbeat_unix >= interval_secs as i64 {
                if let Some(missed) = state.record_missed_heartbeat(&node.node_id).await {
                    if missed >= threshold {
                        error!(
                            node_id = %node.node_id,
                            missed_heartbeats = missed,
                            "broker: multi_agent_node_dead_removing_from_ring"
                        );
                        state.remove_node(&node.node_id).await;
                    } else {
                        warn!(
                            node_id = %node.node_id,
                            missed_heartbeats = missed,
                            threshold = threshold,
                            "broker: missed_heartbeat_warning"
                        );
                    }
                }
            }
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => warn!("broker: SIGINT received, draining tasks..."),
        _ = sigterm => warn!("broker: SIGTERM received, draining tasks..."),
    }

    info!("broker: initiating_graceful_shutdown");
}
