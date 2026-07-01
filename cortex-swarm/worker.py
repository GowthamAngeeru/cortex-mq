import asyncio
import json
import logging
import os
import signal
import time
import uuid
from typing import Optional

import grpc
import psutil

import broker_pb2
import broker_pb2_grpc

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base_log = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, 'extra'):
            base_log.update(record.extra)
        elif isinstance(record.args, dict):
            base_log.update(record.args)
        return json.dumps(base_log)

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), handlers=[handler])
logger = logging.getLogger("cortex.worker")

BROKER_URL = os.getenv("BROKER_URL", "localhost:50051")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "5"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL_SECS", "5"))
CLAIM_INTERVAL = int(os.getenv("CLAIM_INTERVAL_SECS", "2"))
NODE_VERSION = os.getenv("NODE_VERSION", "1.0.0")

BACKOFF_BASE_SECS = 1.0
BACKOFF_MAX_SECS = 60.0
BACKOFF_MULTIPLIER = 2.0

class CortexWorker:
    def __init__(
        self,
        broker_url: str = BROKER_URL,
        max_concurrent: int = MAX_CONCURRENT,
    ):
        self.node_id = os.getenv("NODE_ID", f"node-{uuid.uuid4().hex[:8]}")
        self.broker_url = broker_url
        self.max_concurrent = max_concurrent
        
        self._active_tasks_count = 0
        self._task_counter_lock = asyncio.Lock()
        self._running_tasks: set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        
        self.channel: Optional[grpc.aio.Channel] = None
        self.stub: Optional[broker_pb2_grpc.BrokerServiceStub] = None

    async def start(self) -> None:
        self._install_signal_handlers()
        logger.info("worker_starting", extra={
            "node_id": self.node_id,
            "broker_url": self.broker_url,
            "max_concurrent": self.max_concurrent,
        })

        await self._connect_with_retry()

        heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="heartbeat")
        claim_task = asyncio.create_task(self._claim_loop(), name="claim")

        try:
            await self._shutdown_event.wait()
        finally:
            logger.info("worker_shutting_down", extra={"node_id": self.node_id})
            heartbeat_task.cancel()
            claim_task.cancel()
            await asyncio.gather(heartbeat_task, claim_task, return_exceptions=True)

            if self._running_tasks:
                logger.info("draining_active_tasks", extra={"count": len(self._running_tasks)})
                await asyncio.wait(self._running_tasks, timeout=30.0)

            if self.channel:
                await self.channel.close()
            logger.info("worker_stopped", extra={"node_id": self.node_id})

    async def _connect_with_retry(self) -> None:
        backoff = BACKOFF_BASE_SECS
        while not self._shutdown_event.is_set():
            try:
                self.channel = grpc.aio.insecure_channel(
                    self.broker_url,
                    options=[
                        ("grpc.keepalive_time_ms", 30_000),
                        ("grpc.keepalive_timeout_ms", 10_000),
                        ("grpc.keepalive_permit_without_calls", 1),
                        ("grpc.max_receive_message_length", 4 * 1024 * 1024),
                    ],
                )
                self.stub = broker_pb2_grpc.BrokerServiceStub(self.channel)
                await asyncio.wait_for(self.channel.channel_ready(), timeout=5.0)
                logger.info("broker_connected", extra={"broker_url": self.broker_url})
                return
            except (grpc.RpcError, asyncio.TimeoutError) as e:
                logger.warning("broker_connection_failed", extra={
                    "error": str(e),
                    "retry_secs": backoff,
                })
                await asyncio.sleep(backoff)
                backoff = min(backoff * BACKOFF_MULTIPLIER, BACKOFF_MAX_SECS)

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_event_loop()
        def _handle_signal():
            logger.warning("signal_received")
            self._shutdown_event.set()
        
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # Windows workaround for signal handlers
            pass

    async def _increment_tasks(self) -> None:
        async with self._task_counter_lock:
            self._active_tasks_count += 1

    async def _decrement_tasks(self) -> None:
        async with self._task_counter_lock:
            self._active_tasks_count = max(0, self._active_tasks_count - 1)

    @property
    def active_tasks(self) -> int:
        return self._active_tasks_count

    async def _heartbeat_loop(self) -> None:
        consecutive_failures = 0
        while not self._shutdown_event.is_set():
            if not self.stub:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                continue

            try:
                cpu_pct = psutil.cpu_percent(interval=None)
                mem_pct = psutil.virtual_memory().percent
                
                req = broker_pb2.HeartbeatPing(
                    node_id=self.node_id,
                    node_version=NODE_VERSION,
                    active_tasks=self.active_tasks,
                    cpu_percent=cpu_pct,
                    memory_percent=mem_pct,
                    timestamp_unix=int(time.time()),
                    active_task_ids=[]
                )
                
                res = await asyncio.wait_for(self.stub.Heartbeat(req), timeout=5.0)
                consecutive_failures = 0
                logger.debug("heartbeat_sent", extra={
                    "node_id": self.node_id,
                    "active_tasks": self.active_tasks,
                    "cpu_pct": cpu_pct,
                    "pending_tasks": res.pending_tasks,
                })
            except (grpc.RpcError, asyncio.TimeoutError) as e:
                consecutive_failures += 1
                logger.warning("heartbeat_failed", extra={
                    "node_id": self.node_id,
                    "consecutive_failures": consecutive_failures,
                    "error": str(e),
                })
                
                # 10/10 Upgrade: Autonomous Reconnection
                if consecutive_failures >= 3:
                    logger.error("heartbeat_critical_failure - forcing network reconnect", extra={"node_id": self.node_id})
                    self.stub = None
                    if self.channel:
                        await self.channel.close()
                    await self._connect_with_retry()
                    consecutive_failures = 0
            
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _claim_loop(self) -> None:
        backoff = BACKOFF_BASE_SECS
        while not self._shutdown_event.is_set():
            if not self.stub or self.active_tasks >= self.max_concurrent:
                await asyncio.sleep(1.0)
                continue
            
            try:
                req = broker_pb2.ClaimRequest(
                    node_id=self.node_id,
                    node_version=NODE_VERSION,
                    supported_task_types=["langgraph_agent", "code_review"],
                    max_concurrent=self.max_concurrent,
                )
                assignment = await asyncio.wait_for(self.stub.ClaimTask(req), timeout=10.0)
                backoff = BACKOFF_BASE_SECS
                
                if assignment.task_id:
                    logger.info("task_claimed", extra={
                        "node_id": self.node_id,
                        "task_id": assignment.task_id,
                        "task_type": assignment.task_type,
                        "attempt": assignment.attempt_number,
                    })
                    self._spawn_task(assignment)
            except (grpc.RpcError, asyncio.TimeoutError) as e:
                logger.debug("claim_failed", extra={"error": str(e)})
                await asyncio.sleep(backoff)
                backoff = min(backoff * BACKOFF_MULTIPLIER, BACKOFF_MAX_SECS)
                continue
                
            await asyncio.sleep(CLAIM_INTERVAL)

    def _spawn_task(self, assignment: broker_pb2.TaskAssignment) -> None:
        task = asyncio.create_task(
            self._execute_task(assignment),
            name=f"task-{assignment.task_id[:8]}"
        )
        self._running_tasks.add(task)
        
        # 10/10 Upgrade: Catch silent background task exceptions
        def _on_completion(t: asyncio.Task):
            self._running_tasks.discard(t)
            if not t.cancelled() and t.exception():
                logger.error("background_task_crashed", extra={
                    "task_id": assignment.task_id,
                    "error": str(t.exception())
                })
                
        task.add_done_callback(_on_completion)

    async def _execute_task(self, assignment: broker_pb2.TaskAssignment) -> None:
        await self._increment_tasks()
        task_id = assignment.task_id
        now = int(time.time())
        lease_expires = assignment.lease_expires_at_unix
        lease_remaining = max(0, lease_expires - now) if lease_expires > 0 else 300
        
        logger.info("task_executing", extra={
            "node_id": self.node_id,
            "task_id": task_id,
            "lease_remaining": lease_remaining,
        })
        
        try:
            await asyncio.wait_for(self._run_agent_pipeline(assignment), timeout=float(lease_remaining))
        except asyncio.TimeoutError:
            logger.error("task_lease_expired", extra={"node_id": self.node_id, "task_id": task_id})
            await self._report_failure(
                task_id=task_id,
                attempt_number=assignment.attempt_number,
                error_msg="Task lease expired before completion",
                error_type="lease_expired",
                is_retryable=True,
            )
        except Exception as e:
            logger.error("task_unexpected_error", extra={"node_id": self.node_id, "task_id": task_id, "error": str(e)})
            await self._report_failure(
                task_id=task_id,
                attempt_number=assignment.attempt_number,
                error_msg=str(e),
                error_type="python_agent_exception",
                is_retryable=True,
            )
        finally:
            await self._decrement_tasks()

    async def _run_agent_pipeline(
        self,
        assignment: broker_pb2.TaskAssignment,
    ) -> None:
        from brain import swarm_brain, validate_final_state, InvalidFinalStateError
        import time
        import os
        import json
        import asyncio
        
        start_ms = int(time.time() * 1000)
        logger.info("agent_pipeline_started", extra={
            "task_id": assignment.task_id,
            "payload": assignment.payload[:100],
        })
        
        # Initialize State
        initial_state = {
            "task": assignment.payload or "Analyze standard system architecture.",
            "research_data": "",
            "code_output": "",
            "reviewer_verdict": "",
            "revision_count": 0,
            "tokens_used": 0,
            "error": "",
        }
        
        # LangSmith Config
        langsmith_url = ""
        config = {}
        langsmith_key = os.getenv("LANGSMITH_API_KEY")
        if langsmith_key:
            import uuid as _uuid
            run_id = str(_uuid.uuid4())
            config = {
                "run_id": run_id,
                "metadata": {
                    "task_id": assignment.task_id,
                    "node_id": self.node_id,
                }
            }
            project = os.getenv("LANGSMITH_PROJECT", "cortex-mq")
            langsmith_url = f"https://smith.langchain.com/projects/{project}/runs/{run_id}"
            
        # Execute Graph
        final_state = await swarm_brain.ainvoke(initial_state, config=config)
        
        # Validate Before Sending to Rust
        try:
            validate_final_state(final_state)
        except InvalidFinalStateError as e:
            raise RuntimeError(f"LangGraph produced invalid state: {e}") from e
            
        # ====== DYNAMIC PAYLOAD PERSISTENCE ======
        final_code = final_state.get("code_output", "")
        output_dir = os.getenv("AGENT_OUTPUT_DIR", "./agent_outputs")
        os.makedirs(output_dir, exist_ok=True)
        
        output_path = os.path.join(output_dir, f"{assignment.task_id}.py")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(final_code)
            
        logger.info("agent_output_saved", extra={
            "task_id": assignment.task_id, 
            "path": output_path
        })
        # =========================================
            
        duration_ms = int(time.time() * 1000) - start_ms
        tokens_used = final_state.get("tokens_used", 0)
        revision_count = final_state.get("revision_count", 0)
        
        logger.info("agent_pipeline_complete", extra={
            "task_id": assignment.task_id,
            "duration_ms": duration_ms,
            "revisions": revision_count,
            "tokens_used": tokens_used,
            "verdict": final_state["reviewer_verdict"],
            "langsmith_url": langsmith_url,
        })
        
        # Package Result for Rust
        trace = broker_pb2.AgentTrace(
            researcher_output=final_state["research_data"],
            coder_output=final_state["code_output"],
            reviewer_verdict=final_state["reviewer_verdict"],
            revision_count=revision_count,
            langsmith_run_url=langsmith_url,
            total_duration_ms=duration_ms,
        )
        
        result = broker_pb2.TaskResult(
            task_id=assignment.task_id,
            node_id=self.node_id,
            result=json.dumps({
                "final_code": final_state["code_output"],
                "status": final_state["reviewer_verdict"],
                "revisions": revision_count,
            }),
            trace=trace,
            completed_at_unix=int(time.time()),
            tokens_used=tokens_used,
            attempt_number=assignment.attempt_number,
        )
        
        if not self.stub:
            raise RuntimeError("Broker connection lost during task execution.")
            
        await asyncio.wait_for(
            self.stub.CompleteTask(result),
            timeout=10.0
        )
        
        logger.info("task_completed", extra={
            "node_id": self.node_id,
            "task_id": assignment.task_id,
            "duration_ms": duration_ms,
        })
    async def _report_failure(self, task_id: str, attempt_number: int, error_msg: str, error_type: str, is_retryable: bool) -> None:
        if not self.stub:
            logger.error("failure_report_error", extra={"task_id": task_id, "error": "Cannot report failure; stub is None."})
            return
            
        try:
            await asyncio.wait_for(
                self.stub.FailTask(broker_pb2.TaskFailure(
                    task_id=task_id,
                    node_id=self.node_id,
                    error_msg=error_msg,
                    error_type=error_type,
                    is_retryable=is_retryable,
                    attempt_number=attempt_number,
                )),
                timeout=10.0
            )
            logger.warning("task_failure_reported", extra={"node_id": self.node_id, "task_id": task_id, "error_type": error_type})
        except Exception as e:
            logger.error("failure_report_error", extra={"task_id": task_id, "error": str(e)})

if __name__ == "__main__":
    worker = CortexWorker(broker_url=BROKER_URL, max_concurrent=MAX_CONCURRENT)
    asyncio.run(worker.start())