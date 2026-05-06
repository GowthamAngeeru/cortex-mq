use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::Utc;
use deadpool_redis::{
    redis::{cmd, Script},
    Config, Pool, Runtime,
};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{postgres::PgPoolOptions, Pool as PgPool, Postgres};
use tokio::{sync::RwLock, task::JoinHandle, time::timeout};
use uuid::Uuid;

use crate::core::hash_ring::{HashRing, NodeInfo};

const REDIS_CMD_TIMEOUT: Duration = Duration::from_millis(800);

#[derive(Clone)]
pub struct GlobalState {
    ring: Arc<RwLock<HashRing>>,
    redis_pool: Pool,
    pg_pool: PgPool<Postgres>,
    metrics: Arc<StateMetrics>,
}

#[derive(Debug, Clone)]
pub struct TaskLock {
    pub task_id: String,
    pub token: String,
    pub lease_ms: u64,
}

#[derive(Default)]
pub struct StateMetrics {
    pub locks_acquired_total: AtomicU64,
    pub renew_failures_total: AtomicU64,
    pub dlq_pushes_total: AtomicU64,
    pub snapshot_restores_total: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub locks_acquired: u64,
    pub renew_failures: u64,
    pub dlq_pushes: u64,
    pub snapshot_restores: u64,
}

impl GlobalState {
    pub async fn new(redis_url: &str, pg_url: &str, virtual_nodes: usize) -> Result<Self> {
        let mut cfg = Config::from_url(redis_url);

        cfg.pool = Some(deadpool_redis::PoolConfig {
            max_size: 64,
            timeouts: deadpool_redis::Timeouts {
                wait: Some(Duration::from_secs(2)),
                create: Some(Duration::from_secs(2)),
                recycle: Some(Duration::from_secs(2)),
            },
            ..Default::default()
        });

        let redis_pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| anyhow!("failed to create redis pool: {}", e))?;

        let pg_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(pg_url)
            .await
            .map_err(|e| anyhow!("failed to connect to postgres: {}", e))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS task_audit_logs (
                id SERIAL PRIMARY KEY,
                task_id VARCHAR(255) NOT NULL,
                node_id VARCHAR(255),
                event_type VARCHAR(50) NOT NULL,
                details TEXT,
                created_at TIMESTAMPTZ NOT NULL
            );
            "#,
        )
        .execute(&pg_pool)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create audit table: {}", e))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_task_id ON task_audit_logs(task_id);
            "#,
        )
        .execute(&pg_pool)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create audit index: {}", e))?;

        let state = Self {
            ring: Arc::new(RwLock::new(HashRing::new(virtual_nodes))),
            redis_pool,
            pg_pool,
            metrics: Arc::new(StateMetrics::default()),
        };

        for attempt in 1..=3 {
            match state.ping_redis().await {
                Ok(_) => {
                    tracing::info!(attempt, "state: redis_connected");
                    break;
                }
                Err(e) if attempt == 3 => {
                    anyhow::bail!("Redis unreachable after 3 attempts: {}", e);
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "state: redis_ping_failed, retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        Ok(state)
    }

    pub async fn route_task(&self, task_id: &str) -> Option<String> {
        let ring = self.ring.read().await;
        ring.get_node(task_id).map(ToString::to_string)
    }

    pub async fn force_route_task(&self, task_id: &str) -> Option<String> {
        let ring = self.ring.read().await;
        ring.get_node_force(task_id).map(ToString::to_string)
    }

    pub async fn add_node(&self, node_id: &str, max_concurrent: u32) {
        let mut ring = self.ring.write().await;
        ring.add_node(node_id, max_concurrent);
    }

    pub async fn remove_node(&self, node_id: &str) {
        let mut ring = self.ring.write().await;
        ring.remove_node(node_id);
    }

    pub async fn update_heartbeat(&self, node_id: &str, active_tasks: u32, cpu_percent: f32) {
        let mut ring = self.ring.write().await;
        ring.update_node_info(node_id, active_tasks, cpu_percent);
    }

    pub async fn mark_dead(&self, node_id: &str) -> bool {
        let mut ring = self.ring.write().await;
        ring.mark_dead(node_id)
    }

    pub async fn mark_draining(&self, node_id: &str) -> bool {
        let mut ring = self.ring.write().await;
        ring.mark_draining(node_id)
    }

    pub async fn record_missed_heartbeat(&self, node_id: &str) -> Option<u32> {
        let mut ring = self.ring.write().await;
        ring.record_missed_heartbeat(node_id)
    }

    pub async fn record_task_completed(&self, node_id: &str) {
        let mut ring = self.ring.write().await;
        ring.record_task_completed(node_id);
    }

    pub async fn active_node_count(&self) -> usize {
        let ring = self.ring.read().await;
        ring.active_nodes().len()
    }

    pub async fn get_active_nodes(&self) -> Vec<NodeInfo> {
        let ring = self.ring.read().await;
        ring.active_nodes().into_iter().cloned().collect()
    }

    pub async fn distribution_report(&self) -> HashMap<String, usize> {
        let ring = self.ring.read().await;
        ring.distribution_report()
    }

    pub async fn acquire_task_lock(
        &self,
        task_id: &str,
        lease_ms: u64,
    ) -> Result<Option<TaskLock>> {
        let mut conn = self.redis_pool.get().await?;

        let token = Uuid::new_v4().to_string();
        let key = Self::lock_key(task_id);

        let result: Option<String> = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("SET")
                .arg(&key)
                .arg(&token)
                .arg("NX")
                .arg("PX")
                .arg(lease_ms)
                .query_async(&mut conn),
        )
        .await??;

        match result {
            Some(_) => {
                self.metrics
                    .locks_acquired_total
                    .fetch_add(1, Ordering::Relaxed);

                tracing::debug!(task_id = %task_id, "state: lock_acquired");

                self.write_audit_log(
                    task_id,
                    None,
                    "PROCESSING",
                    "Node locked task for execution",
                )
                .await;

                Ok(Some(TaskLock {
                    task_id: task_id.to_string(),
                    token,
                    lease_ms,
                }))
            }
            None => {
                tracing::debug!(task_id = %task_id, "state: lock_denied_already_held");
                Ok(None)
            }
        }
    }

    pub async fn release_task_lock(&self, lock: &TaskLock) -> Result<bool> {
        let mut conn = self.redis_pool.get().await?;

        let script = Script::new(
            r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
            "#,
        );

        let deleted: i32 = timeout(
            REDIS_CMD_TIMEOUT,
            script
                .key(Self::lock_key(&lock.task_id))
                .arg(&lock.token)
                .invoke_async(&mut conn),
        )
        .await??;

        // 🚨 TRIGGER: Audit Log
        if deleted == 1 {
            self.write_audit_log(
                &lock.task_id,
                None,
                "COMPLETED",
                "Task successfully finished",
            )
            .await;
        }

        Ok(deleted == 1)
    }

    pub async fn force_release_task_lock(&self, task_id: &str) -> Result<()> {
        let mut conn = self.redis_pool.get().await?;

        let _: i32 = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("DEL")
                .arg(Self::lock_key(task_id))
                .query_async(&mut conn),
        )
        .await??;

        Ok(())
    }

    pub async fn renew_task_lock(&self, lock: &TaskLock, lease_ms: u64) -> Result<bool> {
        let mut conn = self.redis_pool.get().await?;

        let script = Script::new(
            r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("PEXPIRE", KEYS[1], ARGV[2])
            else
                return 0
            end
            "#,
        );

        let renewed: i32 = timeout(
            REDIS_CMD_TIMEOUT,
            script
                .key(Self::lock_key(&lock.task_id))
                .arg(&lock.token)
                .arg(lease_ms as usize)
                .invoke_async(&mut conn),
        )
        .await??;

        if renewed == 0 {
            self.metrics
                .renew_failures_total
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok(renewed == 1)
    }

    pub fn spawn_lock_renewer(&self, lock: TaskLock, mut base_every_ms: u64) -> JoinHandle<()> {
        if base_every_ms >= lock.lease_ms {
            tracing::warn!(
                task_id = %lock.task_id,
                base_every_ms,
                lease_ms = lock.lease_ms,
                "state: renew interval >= lease ttl, clamping to safely renew"
            );
            base_every_ms = lock.lease_ms / 2;
        }

        let state = self.clone();

        tokio::spawn(async move {
            loop {
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos();
                let jitter = (nanos % 250) as u64;

                tokio::time::sleep(Duration::from_millis(base_every_ms + jitter)).await;

                match state.renew_task_lock(&lock, lock.lease_ms).await {
                    Ok(true) => {}

                    Ok(false) => {
                        tracing::warn!(
                            task_id = %lock.task_id,
                            "state: renew_lost_ownership"
                        );
                        break;
                    }

                    Err(err) => {
                        tracing::error!(
                            task_id = %lock.task_id,
                            error = %err,
                            "state: renew_failed"
                        );
                        break;
                    }
                }
            }
        })
    }

    pub async fn save_task_snapshot<T>(
        &self,
        task_id: &str,
        payload: &T,
        ttl_secs: u64,
    ) -> Result<()>
    where
        T: Serialize,
    {
        let mut conn = self.redis_pool.get().await?;
        let json = serde_json::to_string(payload)?;

        let _: () = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("SET")
                .arg(Self::snapshot_key(task_id))
                .arg(json)
                .arg("EX")
                .arg(ttl_secs)
                .query_async(&mut conn),
        )
        .await??;

        self.write_audit_log(task_id, None, "PENDING", "Task injected into the cluster")
            .await;

        Ok(())
    }

    pub async fn load_task_snapshot<T>(&self, task_id: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let mut conn = self.redis_pool.get().await?;

        let raw: Option<String> = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("GET")
                .arg(Self::snapshot_key(task_id))
                .query_async(&mut conn),
        )
        .await??;

        match raw {
            Some(json) => {
                self.metrics
                    .snapshot_restores_total
                    .fetch_add(1, Ordering::Relaxed);

                tracing::debug!(task_id = %task_id, "state: snapshot_restored");

                let data = serde_json::from_str(&json)?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    pub async fn delete_task_snapshot(&self, task_id: &str) -> Result<()> {
        let mut conn = self.redis_pool.get().await?;

        let _: i32 = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("DEL")
                .arg(Self::snapshot_key(task_id))
                .query_async(&mut conn),
        )
        .await??;

        Ok(())
    }

    pub async fn push_assigned_task<T>(
        &self,
        task_id: &str,
        node_id: &str,
        payload: &T,
    ) -> Result<()>
    where
        T: Serialize,
    {
        let mut conn = self.redis_pool.get().await?;
        let json = serde_json::to_string(payload)?;
        let queue_key = format!("cortex:queue:{}", node_id);

        let _: i32 = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("RPUSH")
                .arg(&queue_key)
                .arg(json)
                .query_async(&mut conn),
        )
        .await??;

        // 🚨 TRIGGER: Audit Log
        self.write_audit_log(
            task_id,
            Some(node_id),
            "ASSIGNED",
            "Task routed to specific node queue",
        )
        .await;

        Ok(())
    }

    pub async fn fetch_assigned_task<T>(&self, node_id: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let mut conn = self.redis_pool.get().await?;
        let queue_key = format!("cortex:queue:{}", node_id);

        let raw: Option<String> = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("LPOP").arg(&queue_key).query_async(&mut conn),
        )
        .await??;

        match raw {
            Some(json) => {
                let data = serde_json::from_str(&json)?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    pub async fn push_dlq<T>(&self, task_id: &str, payload: &T) -> Result<()>
    where
        T: Serialize,
    {
        let mut conn = self.redis_pool.get().await?;
        let json = serde_json::to_string(payload)?;

        let _: i32 = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("RPUSH")
                .arg("cortex:dlq")
                .arg(json)
                .query_async(&mut conn),
        )
        .await??;

        self.metrics
            .dlq_pushes_total
            .fetch_add(1, Ordering::Relaxed);

        tracing::info!("state: task_pushed_to_dlq");

        // 🚨 TRIGGER: Audit Log
        self.write_audit_log(task_id, None, "DLQ", "Task permanently failed to process")
            .await;

        Ok(())
    }

    pub async fn pop_dlq<T>(&self) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let mut conn = self.redis_pool.get().await?;

        let raw: Option<String> = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("LPOP").arg("cortex:dlq").query_async(&mut conn),
        )
        .await??;

        match raw {
            Some(json) => {
                let data = serde_json::from_str(&json)?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    pub async fn list_dlq(&self, limit: i64) -> Result<Vec<String>> {
        let mut conn = self.redis_pool.get().await?;

        let items: Vec<String> = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("LRANGE")
                .arg("cortex:dlq")
                .arg(0)
                .arg(limit - 1)
                .query_async(&mut conn),
        )
        .await??;

        Ok(items)
    }

    pub async fn dlq_size(&self) -> Result<u64> {
        let mut conn = self.redis_pool.get().await?;

        let size: u64 = timeout(
            REDIS_CMD_TIMEOUT,
            cmd("LLEN").arg("cortex:dlq").query_async(&mut conn),
        )
        .await??;

        Ok(size)
    }

    pub async fn ping_redis(&self) -> Result<()> {
        let mut conn = self.redis_pool.get().await?;

        let pong: String = timeout(REDIS_CMD_TIMEOUT, cmd("PING").query_async(&mut conn)).await??;

        if pong == "PONG" {
            Ok(())
        } else {
            Err(anyhow!("unexpected redis ping response"))
        }
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            locks_acquired: self.metrics.locks_acquired_total.load(Ordering::Relaxed),
            renew_failures: self.metrics.renew_failures_total.load(Ordering::Relaxed),
            dlq_pushes: self.metrics.dlq_pushes_total.load(Ordering::Relaxed),
            snapshot_restores: self.metrics.snapshot_restores_total.load(Ordering::Relaxed),
        }
    }

    fn lock_key(task_id: &str) -> String {
        format!("cortex:lock:task:{}", task_id)
    }

    fn snapshot_key(task_id: &str) -> String {
        format!("cortex:snapshot:task:{}", task_id)
    }

    pub async fn write_audit_log(
        &self,
        task_id: &str,
        node_id: Option<&str>,
        event_type: &str,
        details: &str,
    ) {
        let timestamp = Utc::now();

        let result = sqlx::query(
            "INSERT INTO task_audit_logs (task_id, node_id, event_type, details, created_at) VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(task_id)
        .bind(node_id)
        .bind(event_type)
        .bind(details)
        .bind(timestamp)
        .execute(&self.pg_pool)
        .await;

        if let Err(e) = result {
            tracing::error!(
                task_id = %task_id,
                error = %e,
                "state: failed_to_write_audit_log"
            );
        }
    }
}
