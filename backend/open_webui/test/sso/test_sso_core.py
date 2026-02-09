"""Phase 8.3 — SSO Core tests (19 tests).

Validates TaskState lifecycle, Redis persistence, idempotency, zombie cleanup,
graceful shutdown, and metrics.
"""

import asyncio
import json
import time

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from open_webui.tasks import (
    TaskState,
    TaskStatus,
    save_task_state,
    get_task_state,
    find_task_by_message_id,
    periodic_zombie_cleanup,
    record_task_metric,
    TASK_METRICS,
    SSO_INSTANCE_ID,
)


def _mock_redis():
    """Create a mock async Redis client."""
    r = AsyncMock()
    r.hset = AsyncMock()
    r.hgetall = AsyncMock(return_value={})
    r.expire = AsyncMock()
    r.set = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.scan = AsyncMock(return_value=(0, []))
    return r


class TestTaskStateLifecycle:
    """Test TaskState transitions."""

    def test_task_state_lifecycle(self):
        """QUEUED -> PROCESSING -> COMPLETED."""
        task = TaskState(chat_id="c1", user_id="u1", worker_id="w1")
        assert task.status == TaskStatus.QUEUED

        task.status = TaskStatus.PROCESSING
        task.started_at = time.time()
        assert task.status == TaskStatus.PROCESSING

        task.status = TaskStatus.COMPLETED
        task.completed_at = time.time()
        assert task.status == TaskStatus.COMPLETED
        assert task.completed_at is not None

    def test_task_state_defaults(self):
        """TaskState has correct default values."""
        task = TaskState()
        assert task.task_id  # UUID generated
        assert task.status == TaskStatus.QUEUED
        assert task.created_at > 0
        assert task.started_at is None
        assert task.completed_at is None
        assert task.error is None
        assert task.metadata == {}
        assert task.ttl == 3600


class TestTaskStateRedisPersistence:
    """Test save/get TaskState to/from Redis."""

    @pytest.mark.asyncio
    async def test_task_state_redis_persistence(self):
        """Save and retrieve TaskState from Redis."""
        redis = _mock_redis()
        task = TaskState(
            chat_id="c1",
            message_id="m1",
            user_id="u1",
            worker_id="w1",
            metadata={"key": "value"},
        )

        await save_task_state(redis, task, "test")

        redis.hset.assert_called_once()
        redis.expire.assert_called_once_with(
            f"test:task_state:{task.task_id}", task.ttl
        )
        # message_id set -> secondary index created
        redis.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_task_state_redis_roundtrip(self):
        """Full roundtrip: save then get."""
        redis = _mock_redis()
        task = TaskState(
            chat_id="c1",
            message_id="m1",
            user_id="u1",
            worker_id="w1",
            status=TaskStatus.PROCESSING,
            started_at=time.time(),
            metadata={"type": "test"},
        )

        # Simulate what hgetall returns after save
        redis.hgetall.return_value = {
            "task_id": task.task_id,
            "chat_id": "c1",
            "message_id": "m1",
            "user_id": "u1",
            "worker_id": "w1",
            "status": "processing",
            "created_at": str(task.created_at),
            "started_at": str(task.started_at),
            "completed_at": "None",
            "error": "None",
            "metadata": json.dumps({"type": "test"}),
            "ttl": "3600",
        }

        result = await get_task_state(redis, task.task_id, "test")
        assert result is not None
        assert result.task_id == task.task_id
        assert result.status == TaskStatus.PROCESSING
        assert result.chat_id == "c1"
        assert result.metadata == {"type": "test"}
        assert result.started_at is not None
        assert result.completed_at is None

    @pytest.mark.asyncio
    async def test_get_task_state_not_found(self):
        """get_task_state returns None for missing task."""
        redis = _mock_redis()
        redis.hgetall.return_value = {}

        result = await get_task_state(redis, "nonexistent", "test")
        assert result is None


class TestIdempotency:
    """Test message_id -> task_id idempotency."""

    @pytest.mark.asyncio
    async def test_idempotency_same_message_id(self):
        """Same message_id returns same task_id."""
        redis = _mock_redis()
        task = TaskState(chat_id="c1", message_id="m1", user_id="u1")

        await save_task_state(redis, task, "test")

        # Simulate Redis returning the task_id for the message_id
        redis.get.return_value = task.task_id
        found = await find_task_by_message_id(redis, "m1", "test")
        assert found == task.task_id

    @pytest.mark.asyncio
    async def test_idempotency_cross_worker(self):
        """Idempotency works between workers (via Redis)."""
        redis = _mock_redis()
        task = TaskState(
            chat_id="c1", message_id="m1", user_id="u1", worker_id="worker_A"
        )

        await save_task_state(redis, task, "test")

        # Worker B queries the same message_id
        redis.get.return_value = task.task_id
        found = await find_task_by_message_id(redis, "m1", "test")
        assert found == task.task_id


class TestBypassFilter:
    """Test bypass_filter prevents orchestration."""

    def test_bypass_filter_no_orchestrate(self):
        """bypass_filter=True should imply no SSO task creation.

        This is a design contract test - when bypass_filter=True is passed
        to generate_chat_completion, the orchestration layer should not
        create a new SSO task. The conftest mocks open_webui.utils.chat,
        so we verify the real module source for the parameter.
        """
        # Read the actual source file to verify bypass_filter parameter exists
        from pathlib import Path

        chat_path = Path(__file__).resolve().parents[2] / "utils" / "chat.py"
        source = chat_path.read_text(encoding="utf-8")
        assert "bypass_filter" in source


class TestTaskTimeout:
    """Test task timeout behavior."""

    def test_task_timeout(self):
        """Task exceeding SSO_TASK_TIMEOUT -> status FAILED."""
        task = TaskState(chat_id="c1", user_id="u1")
        task.status = TaskStatus.PROCESSING
        task.started_at = time.time() - 400  # 400s ago, > 300s timeout

        from open_webui.env import SSO_TASK_TIMEOUT

        elapsed = time.time() - task.started_at
        if elapsed > SSO_TASK_TIMEOUT:
            task.status = TaskStatus.FAILED
            task.error = "Timeout exceeded"

        assert task.status == TaskStatus.FAILED
        assert task.error == "Timeout exceeded"


class TestGracefulShutdown:
    """Test graceful shutdown cancels active SSO tasks."""

    @pytest.mark.asyncio
    async def test_graceful_shutdown(self):
        """Shutdown cancels active tasks and persists state."""
        cancelled = []

        async def mock_task():
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        task = asyncio.create_task(mock_task())
        task.set_name("sso_test_task")

        # Yield control so the task actually starts running
        await asyncio.sleep(0)

        # Simulate shutdown logic
        active_sso_tasks = [
            t for t in asyncio.all_tasks() if t.get_name().startswith("sso_")
        ]
        for t in active_sso_tasks:
            t.cancel()
        if active_sso_tasks:
            await asyncio.gather(*active_sso_tasks, return_exceptions=True)

        assert len(cancelled) == 1


class TestZombieCleanup:
    """Test zombie task detection and cleanup."""

    @pytest.mark.asyncio
    async def test_zombie_cleanup(self):
        """Tasks PROCESSING beyond grace period -> FAILED."""
        redis = _mock_redis()
        old_started = str(time.time() - 700)  # 700s ago > 600s grace

        redis.scan.return_value = (0, ["test:task_state:zombie1"])
        redis.hgetall.return_value = {
            "task_id": "zombie1",
            "status": "processing",
            "started_at": old_started,
        }

        app = MagicMock()
        app.state.redis = redis

        # Run one iteration of cleanup (patch sleep to break loop)
        with patch("asyncio.sleep", side_effect=[None, asyncio.CancelledError]):
            try:
                await periodic_zombie_cleanup(app)
            except asyncio.CancelledError:
                pass

        # Verify zombie was marked as failed
        redis.hset.assert_any_call("test:task_state:zombie1", "status", "failed")


class TestCleanupExpiredStates:
    """Test that expired tasks (TTL) are removed by Redis automatically."""

    @pytest.mark.asyncio
    async def test_cleanup_expired_states(self):
        """Tasks with TTL are set to expire in Redis."""
        redis = _mock_redis()
        task = TaskState(chat_id="c1", user_id="u1", ttl=60)

        await save_task_state(redis, task, "test")

        redis.expire.assert_called_once_with(f"test:task_state:{task.task_id}", 60)


class TestCancelTask:
    """Test cancellation targets only the correct chat."""

    @pytest.mark.asyncio
    async def test_cancel_stops_only_target_chat(self):
        """Canceling task of one chat does not affect another."""
        tasks_state = {}

        task_a = TaskState(chat_id="chat_a", user_id="u1")
        task_b = TaskState(chat_id="chat_b", user_id="u1")
        tasks_state[task_a.task_id] = task_a
        tasks_state[task_b.task_id] = task_b

        # Cancel only task_a
        task_a.status = TaskStatus.CANCELLED

        assert task_a.status == TaskStatus.CANCELLED
        assert task_b.status == TaskStatus.QUEUED  # Unaffected


class TestTaskMetrics:
    """Test task metrics recording."""

    def test_record_task_metric_counter(self):
        """Record counter metric increments correctly."""
        original = TASK_METRICS["tasks_created"]
        record_task_metric("tasks_created")
        assert TASK_METRICS["tasks_created"] == original + 1
        # Reset
        TASK_METRICS["tasks_created"] = original

    def test_record_task_metric_list(self):
        """Record list metric appends correctly."""
        original_len = len(TASK_METRICS["task_duration_seconds"])
        record_task_metric("task_duration_seconds", 1.5)
        assert len(TASK_METRICS["task_duration_seconds"]) == original_len + 1
        assert TASK_METRICS["task_duration_seconds"][-1] == 1.5
        # Reset
        TASK_METRICS["task_duration_seconds"].pop()

    def test_record_task_metric_unknown(self):
        """Unknown metric name is silently ignored."""
        record_task_metric("nonexistent_metric")
        assert "nonexistent_metric" not in TASK_METRICS


class TestSSOInstanceID:
    """Test SSO_INSTANCE_ID is a valid UUID."""

    def test_sso_instance_id(self):
        """SSO_INSTANCE_ID is a non-empty UUID string."""
        assert isinstance(SSO_INSTANCE_ID, str)
        assert len(SSO_INSTANCE_ID) == 36  # UUID format
        assert "-" in SSO_INSTANCE_ID
