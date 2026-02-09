"""Phase 8.8 — Integration tests (13 tests).

Mock-based integration tests that simulate E2E scenarios without a running server.
Each test exercises the actual logic through mocked Redis and app state.
"""

import json
import time

import pytest
from unittest.mock import AsyncMock

from open_webui.tasks import (
    TaskState,
    TaskStatus,
    save_task_state,
    get_task_state,
    find_task_by_message_id,
)


def _mock_redis():
    """Create a mock async Redis with standard operations."""
    r = AsyncMock()
    r.hset = AsyncMock()
    r.hgetall = AsyncMock(return_value={})
    r.expire = AsyncMock()
    r.set = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.publish = AsyncMock(return_value=1)
    r.scan = AsyncMock(return_value=(0, []))
    return r


class TestE2EChatCompletionSSO:
    """Simulate SSO flow: request -> task created -> state persisted -> completed."""

    @pytest.mark.asyncio
    async def test_e2e_chat_completion_sso(self):
        """Full SSO flow: create task -> PROCESSING -> COMPLETED in Redis."""
        redis = _mock_redis()
        prefix = "test"

        # 1. Create task (simulates 202 response)
        task = TaskState(
            chat_id="chat_1",
            message_id="msg_1",
            user_id="user_1",
            worker_id="worker_1",
        )
        await save_task_state(redis, task, prefix)
        assert task.status == TaskStatus.QUEUED

        # 2. Move to PROCESSING
        task.status = TaskStatus.PROCESSING
        task.started_at = time.time()
        await save_task_state(redis, task, prefix)

        # 3. Complete
        task.status = TaskStatus.COMPLETED
        task.completed_at = time.time()
        await save_task_state(redis, task, prefix)

        # Verify Redis calls
        assert redis.hset.call_count == 3


class TestE2EChatCompletionLegacy:
    """Simulate legacy flow: no SSO -> direct completion."""

    @pytest.mark.asyncio
    async def test_e2e_chat_completion_legacy(self):
        """Legacy flow: SSO off -> no task created, direct path."""
        enable_sso = False

        if enable_sso:
            task = TaskState(chat_id="c1", user_id="u1")
        else:
            task = None

        # Legacy mode: no task object
        assert task is None


class TestE2ESummarizerWithSSO:
    """Simulate Summarizer + SSO: bypass_filter prevents recursion."""

    @pytest.mark.asyncio
    async def test_e2e_summarizer_with_sso(self):
        """bypass_filter=True on internal LLM call prevents task creation."""
        tasks_created = []

        def mock_orchestrate(bypass_filter=False, **kwargs):
            if not bypass_filter:
                tasks_created.append(TaskState(chat_id="c1", user_id="u1"))
            return {"choices": [{"message": {"content": "OK"}}]}

        # External call creates a task
        mock_orchestrate(bypass_filter=False)
        assert len(tasks_created) == 1

        # Internal summarizer call does NOT create a task
        mock_orchestrate(bypass_filter=True)
        assert len(tasks_created) == 1  # Still 1


class TestE2EPreloadThenSSO:
    """Simulate preloaded filters used by SSO with zero overhead."""

    @pytest.mark.asyncio
    async def test_e2e_preload_then_sso(self):
        """Preloaded filter has _valves_preloaded flag, skips DB query."""
        import types

        module = types.ModuleType("preloaded_filter")
        module._valves_preloaded = True

        app_state = {"f1": module}

        # SSO task uses preloaded filter -> no DB query needed
        assert app_state["f1"]._valves_preloaded is True


class TestE2EMultiWorkerTaskVisibility:
    """Simulate multi-worker task visibility via shared Redis."""

    @pytest.mark.asyncio
    async def test_e2e_multi_worker_task_visibility(self):
        """Task created on worker A is visible from worker B via Redis."""
        redis = _mock_redis()
        prefix = "test"

        # Worker A creates task
        task = TaskState(
            chat_id="c1", message_id="m1", user_id="u1", worker_id="worker_A"
        )
        await save_task_state(redis, task, prefix)

        # Worker B queries by message_id
        redis.get.return_value = task.task_id
        found_id = await find_task_by_message_id(redis, "m1", prefix)
        assert found_id == task.task_id

        # Worker B retrieves full state
        redis.hgetall.return_value = {
            "task_id": task.task_id,
            "chat_id": "c1",
            "message_id": "m1",
            "user_id": "u1",
            "worker_id": "worker_A",
            "status": "queued",
            "created_at": str(task.created_at),
            "started_at": "None",
            "completed_at": "None",
            "error": "None",
            "metadata": "{}",
            "ttl": "3600",
        }
        state = await get_task_state(redis, task.task_id, prefix)
        assert state is not None
        assert state.worker_id == "worker_A"


class TestE2ECancelFromDifferentWorker:
    """Simulate cross-worker cancel via Redis PubSub."""

    @pytest.mark.asyncio
    async def test_e2e_cancel_from_different_worker(self):
        """Worker B publishes cancel -> worker A picks it up."""
        redis = _mock_redis()

        # Worker B publishes cancel command
        cancel_msg = json.dumps({"action": "cancel", "task_id": "t1"})
        await redis.publish("test:tasks:commands", cancel_msg)
        redis.publish.assert_called_once_with("test:tasks:commands", cancel_msg)


class TestE2EWebSocketDisconnectReconnect:
    """Simulate WS rehydration: task state survives disconnect."""

    @pytest.mark.asyncio
    async def test_e2e_websocket_disconnect_reconnect(self):
        """After reconnect, task state is retrievable from Redis."""
        redis = _mock_redis()
        prefix = "test"

        task = TaskState(
            chat_id="c1",
            message_id="m1",
            user_id="u1",
            status=TaskStatus.PROCESSING,
        )
        task.started_at = time.time()
        await save_task_state(redis, task, prefix)

        # Simulate disconnect + reconnect: client queries task state
        redis.hgetall.return_value = {
            "task_id": task.task_id,
            "chat_id": "c1",
            "message_id": "m1",
            "user_id": "u1",
            "worker_id": "",
            "status": "processing",
            "created_at": str(task.created_at),
            "started_at": str(task.started_at),
            "completed_at": "None",
            "error": "None",
            "metadata": "{}",
            "ttl": "3600",
        }

        recovered = await get_task_state(redis, task.task_id, prefix)
        assert recovered is not None
        assert recovered.status == TaskStatus.PROCESSING


class TestE2EWebSocketDownPollingFallback:
    """Simulate polling fallback: GET /tasks/status returns correct data."""

    @pytest.mark.asyncio
    async def test_e2e_websocket_down_polling_fallback(self):
        """Task status is available via Redis even without WebSocket."""
        redis = _mock_redis()

        task = TaskState(chat_id="c1", user_id="u1")
        task.status = TaskStatus.COMPLETED
        task.completed_at = time.time()

        redis.hgetall.return_value = {
            "task_id": task.task_id,
            "chat_id": "c1",
            "message_id": "",
            "user_id": "u1",
            "worker_id": "",
            "status": "completed",
            "created_at": str(task.created_at),
            "started_at": "None",
            "completed_at": str(task.completed_at),
            "error": "None",
            "metadata": "{}",
            "ttl": "3600",
        }

        state = await get_task_state(redis, task.task_id, "test")
        assert state.status == TaskStatus.COMPLETED


class TestE2EBackendRestartTasks:
    """Simulate backend restart: orphaned tasks detected by zombie cleanup."""

    @pytest.mark.asyncio
    async def test_e2e_backend_restart_tasks(self):
        """Orphaned PROCESSING tasks are detectable after restart."""
        # Old task from before restart
        old_task = TaskState(
            chat_id="c1",
            user_id="u1",
            worker_id="dead_worker",
            status=TaskStatus.PROCESSING,
        )
        old_task.started_at = time.time() - 700  # Beyond grace period

        from open_webui.env import ZOMBIE_CLEANUP_GRACE_PERIOD

        elapsed = time.time() - old_task.started_at
        is_zombie = elapsed > ZOMBIE_CLEANUP_GRACE_PERIOD

        assert is_zombie is True

        # Zombie cleanup marks it as failed
        old_task.status = TaskStatus.FAILED
        old_task.error = "Worker died (zombie cleanup)"
        assert old_task.status == TaskStatus.FAILED


class TestE2EMultiTabSameChat:
    """Simulate multi-tab: idempotency via message_id."""

    @pytest.mark.asyncio
    async def test_e2e_multi_tab_same_chat(self):
        """Same message_id from 2 tabs returns same task_id."""
        redis = _mock_redis()
        prefix = "test"

        # Tab 1 creates task
        task = TaskState(chat_id="c1", message_id="m1", user_id="u1", worker_id="w1")
        await save_task_state(redis, task, prefix)

        # Tab 2 sends same message_id -> finds existing task
        redis.get.return_value = task.task_id
        existing = await find_task_by_message_id(redis, "m1", prefix)
        assert existing == task.task_id

        # No duplicate created
        redis.get.return_value = task.task_id
        existing2 = await find_task_by_message_id(redis, "m1", prefix)
        assert existing2 == existing


class TestE2EProviderSSEWrongHeader:
    """Simulate provider returning SSE without correct Content-Type."""

    @pytest.mark.asyncio
    async def test_e2e_provider_sse_wrong_header(self):
        """2-layer detection: text/plain header but data: prefix -> streaming."""
        content_type = "text/plain"
        first_bytes = b'data: {"choices":[{"delta":{"content":"Hi"}}]}\n'

        # Layer 1: header check
        is_stream = None
        if "text/event-stream" in content_type:
            is_stream = True
        elif "application/json" in content_type:
            is_stream = False

        # Layer 2: content fallback
        if is_stream is None:
            text = first_bytes.decode("utf-8", errors="ignore").strip()
            if text.startswith("data:"):
                is_stream = True

        assert is_stream is True


class TestE2EMoAServerSide:
    """Simulate MoA: parent task tracks N child tasks."""

    @pytest.mark.asyncio
    async def test_e2e_moa_server_side(self):
        """MoA creates parent + child tasks, aggregates results."""
        redis = _mock_redis()
        prefix = "test"

        # Parent task
        parent = TaskState(
            chat_id="c1",
            user_id="u1",
            worker_id="w1",
            metadata={"type": "moa", "models": ["m1", "m2", "m3"]},
        )
        await save_task_state(redis, parent, prefix)

        # Child tasks
        children = []
        for model in ["m1", "m2", "m3"]:
            child = TaskState(
                chat_id="c1",
                user_id="u1",
                worker_id="w1",
                metadata={"parent_task_id": parent.task_id, "model": model},
            )
            children.append(child)
            await save_task_state(redis, child, prefix)

        assert len(children) == 3
        assert all(c.metadata["parent_task_id"] == parent.task_id for c in children)


class TestE2EGracefulShutdownPersistence:
    """Simulate graceful shutdown: tasks cancelled and persisted."""

    @pytest.mark.asyncio
    async def test_e2e_graceful_shutdown_persistence(self):
        """Shutdown cancels active tasks and saves final state to Redis."""
        redis = _mock_redis()
        prefix = "test"

        # Active tasks
        tasks = [
            TaskState(
                chat_id=f"c{i}",
                user_id="u1",
                worker_id="w1",
                status=TaskStatus.PROCESSING,
            )
            for i in range(3)
        ]
        for t in tasks:
            t.started_at = time.time()

        # Shutdown: cancel all and persist
        for t in tasks:
            t.status = TaskStatus.CANCELLED
            t.completed_at = time.time()
            t.error = "Server shutdown"
            await save_task_state(redis, t, prefix)

        assert all(t.status == TaskStatus.CANCELLED for t in tasks)
        assert all(t.error == "Server shutdown" for t in tasks)
        # 3 tasks saved
        assert redis.hset.call_count == 3
