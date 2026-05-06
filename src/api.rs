use std::time::{SystemTime, UNIX_EPOCH};

use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::core::state::GlobalState;

pub mod pb {
    tonic::include_proto!("cortex");
}

use pb::broker_service_server::BrokerService;
use pb::{
    Acknowledgement, ClaimRequest, DiscardRequest, DlqListRequest, DlqListResponse, HeartbeatPing,
    HeartbeatPong, NodeStatus as PbNodeStatus, ReplayRequest, StatusRequest, SystemStatus,
    TaskAssignment, TaskFailure, TaskHistoryRequest, TaskHistoryResponse, TaskResult, TaskState,
    TaskSubmission,
};

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn internal<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(e.to_string())
}

fn empty_assignment(node_id: String) -> TaskAssignment {
    TaskAssignment {
        task_id: String::new(),
        task_type: String::new(),
        payload: String::new(),
        state: TaskState::Pending as i32,
        assigned_node: node_id,
        lease_expires_at_unix: 0,
        attempt_number: 0,
        max_retries: 0,
    }
}

pub struct BrokerServiceImpl {
    state: GlobalState,
}

impl BrokerServiceImpl {
    pub fn new(state: GlobalState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl BrokerService for BrokerServiceImpl {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatPing>,
    ) -> Result<Response<HeartbeatPong>, Status> {
        let req = request.into_inner();

        if req.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id cannot be empty"));
        }

        self.state
            .update_heartbeat(
                &req.node_id,
                req.active_tasks.max(0) as u32,
                req.cpu_percent,
            )
            .await;

        debug!(
            node_id = %req.node_id,
            active_tasks = req.active_tasks,
            cpu_percent = req.cpu_percent,
            "api: heartbeat_processed"
        );

        Ok(Response::new(HeartbeatPong {
            acknowledged: true,
            broker_status: "healthy".to_string(),
            pending_tasks: 0,
        }))
    }

    async fn claim_task(
        &self,
        request: Request<ClaimRequest>,
    ) -> Result<Response<TaskAssignment>, Status> {
        let req = request.into_inner();

        if req.node_id.trim().is_empty() {
            return Err(Status::invalid_argument("node_id cannot be empty"));
        }

        let max_concurrent = req.max_concurrent.max(1) as u32;
        self.state.add_node(&req.node_id, max_concurrent).await;

        // THE FIX: Fetch as a generic JSON Value to bypass Protobuf Serialize restrictions!
        if let Ok(Some(val)) = self
            .state
            .fetch_assigned_task::<serde_json::Value>(&req.node_id)
            .await
        {
            let task_id = val["task_id"].as_str().unwrap_or_default().to_string();
            let lease_seconds = val["lease_seconds"].as_u64().unwrap_or(300);

            let assignment = TaskAssignment {
                task_id: task_id.clone(),
                task_type: val["task_type"].as_str().unwrap_or_default().to_string(),
                payload: val["payload"].as_str().unwrap_or_default().to_string(),
                state: TaskState::Assigned as i32,
                assigned_node: req.node_id.clone(),
                lease_expires_at_unix: now_unix() + lease_seconds as i64,
                attempt_number: 1,
                max_retries: val["max_retries"].as_i64().unwrap_or(3) as i32,
            };

            debug!(
                node_id = %req.node_id,
                task_id = %task_id,
                "api: task_claimed_from_queue"
            );
            return Ok(Response::new(assignment));
        }

        Ok(Response::new(empty_assignment(req.node_id)))
    }

    async fn submit_task(
        &self,
        request: Request<TaskSubmission>,
    ) -> Result<Response<TaskAssignment>, Status> {
        let req = request.into_inner();

        if req.task_id.trim().is_empty() {
            return Err(Status::invalid_argument("task_id cannot be empty"));
        }

        if req.task_type.trim().is_empty() {
            return Err(Status::invalid_argument("task_type cannot be empty"));
        }

        if let Ok(Some(_)) = self.state.load_task_snapshot::<String>(&req.task_id).await {
            warn!(task_id = %req.task_id, "api: duplicate_submission");
            return Err(Status::already_exists("task already exists"));
        }

        let node = self
            .state
            .route_task(&req.task_id)
            .await
            .ok_or_else(|| Status::resource_exhausted("no healthy nodes available"))?;

        let lease_seconds = req.lease_seconds.max(60) as u64;
        let lease_ms = lease_seconds * 1000;

        let _lock = match self
            .state
            .acquire_task_lock(&req.task_id, lease_ms)
            .await
            .map_err(internal)?
        {
            Some(lock) => lock,
            None => return Err(Status::aborted("task lock contention")),
        };

        self.state
            .save_task_snapshot(&req.task_id, &req.payload, lease_seconds + 3600)
            .await
            .map_err(internal)?;

        // THE FIX: Wrap the payload in raw JSON to send to Redis
        let queue_payload = serde_json::json!({
            "task_id": req.task_id,
            "task_type": req.task_type,
            "payload": req.payload,
            "max_retries": req.max_retries,
            "lease_seconds": lease_seconds
        });

        self.state
            .push_assigned_task(&req.task_id, &node, &queue_payload)
            .await
            .map_err(internal)?;

        let assignment = TaskAssignment {
            task_id: req.task_id.clone(),
            task_type: req.task_type.clone(),
            payload: req.payload.clone(),
            state: TaskState::Assigned as i32,
            assigned_node: node.clone(),
            lease_expires_at_unix: now_unix() + lease_seconds as i64,
            attempt_number: 1,
            max_retries: req.max_retries.max(1),
        };

        info!(
            task_id = %req.task_id,
            assigned_node = %node,
            "api: task_submitted"
        );

        Ok(Response::new(assignment))
    }

    async fn complete_task(
        &self,
        request: Request<TaskResult>,
    ) -> Result<Response<Acknowledgement>, Status> {
        let req = request.into_inner();

        if req.task_id.trim().is_empty() {
            return Err(Status::invalid_argument("task_id cannot be empty"));
        }

        let already_done = self
            .state
            .load_task_snapshot::<String>(&req.task_id)
            .await
            .ok()
            .flatten()
            .is_none();

        if already_done {
            tracing::warn!(task_id = %req.task_id, "api: complete_task_duplicate - already completed");
            return Ok(Response::new(Acknowledgement {
                success: true,
                message: "Already completed (idempotent)".to_string(),
                idempotent: true,
                task_id: req.task_id,
                new_state: TaskState::Completed as i32,
            }));
        }

        self.state.record_task_completed(&req.node_id).await;

        let _ = self.state.force_release_task_lock(&req.task_id).await;
        let _ = self.state.delete_task_snapshot(&req.task_id).await;

        tracing::info!(
            task_id = %req.task_id,
            node_id = %req.node_id,
            "api: task_completed"
        );

        Ok(Response::new(Acknowledgement {
            success: true,
            message: "task completed".to_string(),
            idempotent: false,
            task_id: req.task_id,
            new_state: TaskState::Completed as i32,
        }))
    }

    async fn fail_task(
        &self,
        request: Request<TaskFailure>,
    ) -> Result<Response<Acknowledgement>, Status> {
        let req = request.into_inner();

        if req.task_id.trim().is_empty() {
            return Err(Status::invalid_argument("task_id cannot be empty"));
        }

        if req.is_retryable {
            warn!(
                task_id = %req.task_id,
                "api: retryable failure moved to scheduler retry path"
            );
        }

        self.state
            .push_dlq(&req.task_id, &req.task_id)
            .await
            .map_err(internal)?;

        let _ = self.state.force_release_task_lock(&req.task_id).await;
        let _ = self.state.delete_task_snapshot(&req.task_id).await;

        warn!(
            task_id = %req.task_id,
            node_id = %req.node_id,
            error_type = %req.error_type,
            "api: task_failed_to_dlq"
        );

        Ok(Response::new(Acknowledgement {
            success: true,
            message: "task moved to dlq".to_string(),
            idempotent: false,
            task_id: req.task_id,
            new_state: TaskState::Dlq as i32,
        }))
    }

    async fn get_system_status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<SystemStatus>, Status> {
        let nodes = self.state.get_active_nodes().await;
        let dlq_size = self.state.dlq_size().await.unwrap_or(0);

        let pb_nodes = nodes
            .into_iter()
            .map(|n| PbNodeStatus {
                node_id: n.node_id,
                status: n.status.to_string(),
                active_tasks: n.active_tasks as i32,
                cpu_percent: n.cpu_percent,
                last_heartbeat_unix: n.last_heartbeat_unix,
                missed_heartbeats: n.missed_heartbeats as i32,
                tasks_completed: n.total_completed as i64,
            })
            .collect::<Vec<_>>();

        Ok(Response::new(SystemStatus {
            active_nodes: pb_nodes.len() as i32,
            pending_tasks: 0,
            assigned_tasks: 0,
            dlq_size: dlq_size as i32,
            total_completed: 0,
            total_failed: dlq_size as i64,
            nodes: pb_nodes,
        }))
    }

    async fn list_dlq_tasks(
        &self,
        _request: Request<DlqListRequest>,
    ) -> Result<Response<DlqListResponse>, Status> {
        Err(Status::unimplemented("DLQ listing available in V2"))
    }

    async fn replay_task(
        &self,
        _request: Request<ReplayRequest>,
    ) -> Result<Response<TaskAssignment>, Status> {
        Err(Status::unimplemented("Replay available in V2"))
    }

    async fn discard_dlq_task(
        &self,
        _request: Request<DiscardRequest>,
    ) -> Result<Response<Acknowledgement>, Status> {
        Err(Status::unimplemented("DLQ Discard API coming in V2"))
    }

    async fn get_task_history(
        &self,
        _request: Request<TaskHistoryRequest>,
    ) -> Result<Response<TaskHistoryResponse>, Status> {
        Err(Status::unimplemented("Task History API coming in V2"))
    }
}
