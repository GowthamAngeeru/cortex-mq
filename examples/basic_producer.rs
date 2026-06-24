//! Demonstrates minimal task submission to Cortex-MQ broker.
//! Run with: cargo run --example basic_producer

use std::time::Duration;
use tokio::time::sleep;

pub mod pb {
    tonic::include_proto!("cortex");
}

use pb::broker_service_client::BrokerServiceClient;
use pb::TaskSubmission;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Cortex-MQ basic producer example");
    println!("Connecting to broker at localhost:50051...");

    let mut client = BrokerServiceClient::connect("http://localhost:50051")
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to connect to broker: {}. Is cortex-mq running? Try: docker compose up",
                e
            )
        })?;

    println!("Connected.");

    for i in 0..5 {
        let task_id = format!("example-task-{:04}", i);

        let response = client
            .submit_task(TaskSubmission {
                task_id: task_id.clone(),
                task_type: "example_job".to_string(),
                payload: format!("{{\"step\": {}, \"data\": \"hello from example\"}}", i),
                priority: 1,
                submitted_by: "basic_producer_example".to_string(),
                max_retries: 3,
                lease_seconds: 60,
            })
            .await?;

        let assignment = response.into_inner();
        println!(
            "Task {} → routed to node '{}' (lease expires: {})",
            task_id, assignment.assigned_node, assignment.lease_expires_at_unix
        );

        sleep(Duration::from_millis(100)).await;
    }

    println!("\n5 tasks submitted. Check your worker terminal or dashboard.");
    Ok(())
}
