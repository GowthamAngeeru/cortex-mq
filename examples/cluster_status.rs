//! Prints current cluster status: active nodes, task queues, DLQ.
//! Run with: cargo run --example cluster_status

pub mod pb {
    tonic::include_proto!("cortex");
}

use pb::broker_service_client::BrokerServiceClient;
use pb::StatusRequest;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = BrokerServiceClient::connect("http://localhost:50051")
        .await
        .map_err(|e| anyhow::anyhow!("Connection failed: {}. Is cortex-mq running?", e))?;

    let status = client
        .get_system_status(StatusRequest {})
        .await?
        .into_inner();

    println!("=== Cortex-MQ Cluster Status ===\n");
    println!("Active nodes:    {}", status.active_nodes);
    println!("Assigned tasks:  {}", status.assigned_tasks);
    println!("DLQ size:        {}", status.dlq_size);
    println!("Total completed: {}", status.total_completed);
    println!();

    if status.nodes.is_empty() {
        println!("No nodes connected. Start a worker: cd cortex-swarm && python worker.py");
        return Ok(());
    }

    println!("{:<20} {:<12} {:<14} {:<10} {:<10}", 
        "Node ID", "Status", "Active Tasks", "CPU %", "Completed");
    println!("{}", "-".repeat(70));

    for node in &status.nodes {
        println!(
            "{:<20} {:<12} {:<14} {:<10.1} {:<10}",
            &node.node_id,
            &node.status,
            node.active_tasks,
            node.cpu_percent,
            node.tasks_completed,
        );
    }

    Ok(())
}