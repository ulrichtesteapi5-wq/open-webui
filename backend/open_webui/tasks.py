# tasks.py
import asyncio
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Dict, List, Optional
from uuid import uuid4
import json
import logging
from redis.asyncio import Redis

from open_webui.env import (
    REDIS_KEY_PREFIX,
    SSO_TASK_TTL,
    ZOMBIE_CLEANUP_GRACE_PERIOD,
    ZOMBIE_CLEANUP_SCAN_INTERVAL,
)
log = logging.getLogger(__name__)

# A dictionary to keep track of active tasks
tasks: Dict[str, asyncio.Task] = {}
item_tasks = {}


REDIS_TASKS_KEY = f"{REDIS_KEY_PREFIX}:tasks"
REDIS_ITEM_TASKS_KEY = f"{REDIS_KEY_PREFIX}:tasks:item"
REDIS_PUBSUB_CHANNEL = f"{REDIS_KEY_PREFIX}:tasks:commands"


####################################
# SSO Task State
####################################


class TaskStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskState:
    task_id: str = field(default_factory=lambda: str(uuid4()))
    chat_id: str = ""
    message_id: str = ""
    user_id: str = ""
    worker_id: str = ""
    status: TaskStatus = TaskStatus.QUEUED
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None
    metadata: dict = field(default_factory=dict)
    ttl: int = field(default_factory=lambda: SSO_TASK_TTL)


SSO_INSTANCE_ID = str(uuid4())


TASK_METRICS = {
    "tasks_created": 0,
    "tasks_completed": 0,
    "tasks_failed": 0,
    "tasks_cancelled": 0,
    "tasks_timed_out": 0,
    "tasks_zombie_cleaned": 0,
    "task_duration_seconds": [],
}


def record_task_metric(metric: str, value=1):
    """Record a task metric (per-worker)."""
    if metric in TASK_METRICS:
        if isinstance(TASK_METRICS[metric], list):
            TASK_METRICS[metric].append(value)
        else:
            TASK_METRICS[metric] += value


async def save_task_state(redis: Redis, task: TaskState, prefix: str):
    """Save TaskState to Redis with TTL."""
    key = f"{prefix}:task_state:{task.task_id}"
    task_dict = asdict(task)
    task_dict["metadata"] = json.dumps(task_dict["metadata"])
    task_dict["status"] = task.status.value
    await redis.hset(key, mapping={k: str(v) for k, v in task_dict.items()})
    await redis.expire(key, task.ttl)
    if task.message_id:
        msg_key = f"{prefix}:task_by_message:{task.message_id}"
        await redis.set(msg_key, task.task_id, ex=task.ttl)


async def get_task_state(
    redis: Redis, task_id: str, prefix: str
) -> Optional[TaskState]:
    """Retrieve TaskState from Redis. Converts types (Redis returns everything as str)."""
    key = f"{prefix}:task_state:{task_id}"
    data = await redis.hgetall(key)
    if not data:
        return None
    return TaskState(
        task_id=data.get("task_id", ""),
        chat_id=data.get("chat_id", ""),
        message_id=data.get("message_id", ""),
        user_id=data.get("user_id", ""),
        worker_id=data.get("worker_id", ""),
        status=TaskStatus(data.get("status", "queued")),
        created_at=float(data.get("created_at", 0)),
        started_at=(
            float(data["started_at"])
            if data.get("started_at") not in (None, "None")
            else None
        ),
        completed_at=(
            float(data["completed_at"])
            if data.get("completed_at") not in (None, "None")
            else None
        ),
        error=data.get("error") if data.get("error") not in (None, "None") else None,
        metadata=json.loads(data.get("metadata", "{}")),
        ttl=int(data.get("ttl", 3600)),
    )


async def find_task_by_message_id(
    redis: Redis, message_id: str, prefix: str
) -> Optional[str]:
    """Idempotency: return existing task_id for a message_id."""
    key = f"{prefix}:task_by_message:{message_id}"
    return await redis.get(key)


async def periodic_zombie_cleanup(app):
    """Scan Redis for PROCESSING tasks older than GRACE_PERIOD."""
    while True:
        await asyncio.sleep(ZOMBIE_CLEANUP_SCAN_INTERVAL)
        try:
            redis = app.state.redis
            if not redis:
                continue
            prefix = REDIS_KEY_PREFIX
            cursor = 0
            while True:
                cursor, keys = await redis.scan(
                    cursor, match=f"{prefix}:task_state:*", count=100
                )
                for key in keys:
                    data = await redis.hgetall(key)
                    if data.get("status") == "processing":
                        started = float(data.get("started_at", 0))
                        if time.time() - started > ZOMBIE_CLEANUP_GRACE_PERIOD:
                            task_id = data.get("task_id", "")
                            log.warning(f"Zombie task detected: {task_id}")
                            await redis.hset(key, "status", "failed")
                            await redis.hset(
                                key, "error", "Zombie: exceeded grace period"
                            )
                            await redis.hset(key, "completed_at", str(time.time()))
                if cursor == 0:
                    break
        except Exception as e:
            log.error(f"Zombie cleanup error: {e}")


async def redis_task_command_listener(app):
    redis: Redis = app.state.redis
    pubsub = redis.pubsub()
    await pubsub.subscribe(REDIS_PUBSUB_CHANNEL)

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            command = json.loads(message["data"])
            if command.get("action") == "stop":
                task_id = command.get("task_id")
                local_task = tasks.get(task_id)
                if local_task:
                    local_task.cancel()
        except Exception as e:
            log.exception(f"Error handling distributed task command: {e}")


### ------------------------------
### REDIS-ENABLED HANDLERS
### ------------------------------


async def redis_save_task(redis: Redis, task_id: str, item_id: Optional[str]):
    pipe = redis.pipeline()
    pipe.hset(REDIS_TASKS_KEY, task_id, item_id or "")
    if item_id:
        pipe.sadd(f"{REDIS_ITEM_TASKS_KEY}:{item_id}", task_id)
    await pipe.execute()


async def redis_cleanup_task(redis: Redis, task_id: str, item_id: Optional[str]):
    pipe = redis.pipeline()
    pipe.hdel(REDIS_TASKS_KEY, task_id)
    if item_id:
        pipe.srem(f"{REDIS_ITEM_TASKS_KEY}:{item_id}", task_id)
        if (await pipe.scard(f"{REDIS_ITEM_TASKS_KEY}:{item_id}").execute())[-1] == 0:
            pipe.delete(f"{REDIS_ITEM_TASKS_KEY}:{item_id}")  # Remove if empty set
    await pipe.execute()


async def redis_list_tasks(redis: Redis) -> List[str]:
    return list(await redis.hkeys(REDIS_TASKS_KEY))


async def redis_list_item_tasks(redis: Redis, item_id: str) -> List[str]:
    return list(await redis.smembers(f"{REDIS_ITEM_TASKS_KEY}:{item_id}"))


async def redis_send_command(redis: Redis, command: dict):
    command_json = json.dumps(command)
    # RedisCluster doesn't expose publish() directly, but the
    # PUBLISH command broadcasts across all cluster nodes server-side.
    if hasattr(redis, "nodes_manager"):
        await redis.execute_command("PUBLISH", REDIS_PUBSUB_CHANNEL, command_json)
    else:
        await redis.publish(REDIS_PUBSUB_CHANNEL, command_json)


async def cleanup_task(redis, task_id: str, id=None):
    """
    Remove a completed or canceled task from the global `tasks` dictionary.
    """
    if redis:
        await redis_cleanup_task(redis, task_id, id)

    tasks.pop(task_id, None)  # Remove the task if it exists

    # If an ID is provided, remove the task from the item_tasks dictionary
    if id and task_id in item_tasks.get(id, []):
        item_tasks[id].remove(task_id)
        if not item_tasks[id]:  # If no tasks left for this ID, remove the entry
            item_tasks.pop(id, None)


async def create_task(redis, coroutine, id=None):
    """
    Create a new asyncio task and add it to the global task dictionary.
    """
    task_id = str(uuid4())  # Generate a unique ID for the task
    task = asyncio.create_task(coroutine)  # Create the task

    # Add a done callback for cleanup
    task.add_done_callback(
        lambda t: asyncio.create_task(cleanup_task(redis, task_id, id))
    )
    tasks[task_id] = task

    # If an ID is provided, associate the task with that ID
    if item_tasks.get(id):
        item_tasks[id].append(task_id)
    else:
        item_tasks[id] = [task_id]

    if redis:
        await redis_save_task(redis, task_id, id)

    return task_id, task


async def list_tasks(redis):
    """
    List all currently active task IDs.
    """
    if redis:
        return await redis_list_tasks(redis)
    return list(tasks.keys())


async def list_task_ids_by_item_id(redis, id):
    """
    List all tasks associated with a specific ID.
    """
    if redis:
        return await redis_list_item_tasks(redis, id)
    return item_tasks.get(id, [])


async def stop_task(redis, task_id: str):
    """
    Cancel a running task and remove it from the global task list.
    """
    if redis:
        # PUBSUB: All instances check if they have this task, and stop if so.
        await redis_send_command(
            redis,
            {
                "action": "stop",
                "task_id": task_id,
            },
        )
        # Optionally check if task_id still in Redis a few moments later for feedback?
        return {"status": True, "message": f"Stop signal sent for {task_id}"}

    task = tasks.pop(task_id, None)
    if not task:
        return {"status": False, "message": f"Task with ID {task_id} not found."}

    task.cancel()  # Request task cancellation
    try:
        await task  # Wait for the task to handle the cancellation
    except asyncio.CancelledError:
        # Task successfully canceled
        return {"status": True, "message": f"Task {task_id} successfully stopped."}

    if task.cancelled() or task.done():
        return {"status": True, "message": f"Task {task_id} successfully cancelled."}

    return {"status": True, "message": f"Cancellation requested for {task_id}."}


async def stop_item_tasks(redis: Redis, item_id: str):
    """
    Stop all tasks associated with a specific item ID.
    """
    task_ids = await list_task_ids_by_item_id(redis, item_id)
    if not task_ids:
        return {"status": True, "message": f"No tasks found for item {item_id}."}

    for task_id in task_ids:
        result = await stop_task(redis, task_id)
        if not result["status"]:
            return result  # Return the first failure

    return {"status": True, "message": f"All tasks for item {item_id} stopped."}


async def has_active_tasks(redis, chat_id: str) -> bool:
    """Check if a chat has any active tasks."""
    task_ids = await list_task_ids_by_item_id(redis, chat_id)
    return len(task_ids) > 0


async def get_active_chat_ids(redis, chat_ids: List[str]) -> List[str]:
    """Filter a list of chat_ids to only those with active tasks."""
    active = []
    for chat_id in chat_ids:
        if await has_active_tasks(redis, chat_id):
            active.append(chat_id)
    return active
