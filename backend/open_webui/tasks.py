# tasks.py
import asyncio
from typing import Dict
from uuid import uuid4
import json
import logging
import time
from enum import Enum
from redis.asyncio import Redis
from fastapi import Request
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict

from open_webui.env import REDIS_KEY_PREFIX


log = logging.getLogger(__name__)


# ============================================================
# Task Status Enum
# ============================================================
class TaskStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ============================================================
# Task State Data Class
# ============================================================
@dataclass
class TaskState:
    task_id: str
    chat_id: Optional[str] = None
    message_id: Optional[str] = None
    user_id: Optional[str] = None
    status: TaskStatus = TaskStatus.QUEUED
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None
    result: Optional[dict] = None
    metadata: Optional[dict] = None
    ttl: int = 3600  # Default 1 hour TTL

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "chat_id": self.chat_id,
            "message_id": self.message_id,
            "user_id": self.user_id,
            "status": self.status.value if isinstance(self.status, TaskStatus) else self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "result": self.result,
            "metadata": self.metadata,
            "ttl": self.ttl,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TaskState":
        status = data.get("status", TaskStatus.QUEUED)
        if isinstance(status, str):
            status = TaskStatus(status)
        return cls(
            task_id=data["task_id"],
            chat_id=data.get("chat_id"),
            message_id=data.get("message_id"),
            user_id=data.get("user_id"),
            status=status,
            created_at=data.get("created_at", time.time()),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            error=data.get("error"),
            result=data.get("result"),
            metadata=data.get("metadata"),
            ttl=data.get("ttl", 3600),
        )


# A dictionary to keep track of active tasks
tasks: Dict[str, asyncio.Task] = {}
item_tasks = {}
# In-memory task state store (fallback when Redis is unavailable)
task_states: Dict[str, TaskState] = {}


REDIS_TASKS_KEY = f"{REDIS_KEY_PREFIX}:tasks"
REDIS_ITEM_TASKS_KEY = f"{REDIS_KEY_PREFIX}:tasks:item"
REDIS_TASK_STATE_KEY = f"{REDIS_KEY_PREFIX}:tasks:state"
REDIS_PUBSUB_CHANNEL = f"{REDIS_KEY_PREFIX}:tasks:commands"

# Task TTL configuration (in seconds)
DEFAULT_TASK_TTL = 3600  # 1 hour
TASK_STATE_EXPIRY = 86400  # 24 hours for completed task states


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
                    # Update task state to cancelled
                    await update_task_state(
                        redis, task_id,
                        status=TaskStatus.CANCELLED,
                        completed_at=time.time()
                    )
        except Exception as e:
            log.exception(f"Error handling distributed task command: {e}")


### ------------------------------
### TASK STATE PERSISTENCE
### ------------------------------


async def save_task_state(redis: Optional[Redis], state: TaskState) -> None:
    """
    Persist task state to Redis or in-memory store.
    """
    if redis:
        try:
            state_json = json.dumps(state.to_dict())
            await redis.hset(REDIS_TASK_STATE_KEY, state.task_id, state_json)
            # Set expiry on individual task state
            await redis.expire(REDIS_TASK_STATE_KEY, TASK_STATE_EXPIRY)
        except Exception as e:
            log.error(f"Failed to save task state to Redis: {e}")
            # Fallback to in-memory
            task_states[state.task_id] = state
    else:
        task_states[state.task_id] = state


async def get_task_state(redis: Optional[Redis], task_id: str) -> Optional[TaskState]:
    """
    Retrieve task state from Redis or in-memory store.
    """
    if redis:
        try:
            state_json = await redis.hget(REDIS_TASK_STATE_KEY, task_id)
            if state_json:
                return TaskState.from_dict(json.loads(state_json))
        except Exception as e:
            log.error(f"Failed to get task state from Redis: {e}")
    
    # Fallback to in-memory
    return task_states.get(task_id)


async def update_task_state(
    redis: Optional[Redis],
    task_id: str,
    status: Optional[TaskStatus] = None,
    started_at: Optional[float] = None,
    completed_at: Optional[float] = None,
    error: Optional[str] = None,
    result: Optional[dict] = None,
) -> Optional[TaskState]:
    """
    Update specific fields of a task state.
    """
    state = await get_task_state(redis, task_id)
    if not state:
        return None

    if status is not None:
        state.status = status
    if started_at is not None:
        state.started_at = started_at
    if completed_at is not None:
        state.completed_at = completed_at
    if error is not None:
        state.error = error
    if result is not None:
        state.result = result

    await save_task_state(redis, state)
    return state


async def delete_task_state(redis: Optional[Redis], task_id: str) -> None:
    """
    Delete task state from Redis or in-memory store.
    """
    if redis:
        try:
            await redis.hdel(REDIS_TASK_STATE_KEY, task_id)
        except Exception as e:
            log.error(f"Failed to delete task state from Redis: {e}")
    
    task_states.pop(task_id, None)


async def get_task_state_by_chat_id(redis: Optional[Redis], chat_id: str) -> List[TaskState]:
    """
    Get all task states for a specific chat.
    """
    task_ids = await list_task_ids_by_item_id(redis, chat_id)
    states = []
    for task_id in task_ids:
        state = await get_task_state(redis, task_id)
        if state:
            states.append(state)
    return states


async def cleanup_expired_task_states(redis: Optional[Redis]) -> int:
    """
    Clean up expired task states. Returns the number of cleaned up tasks.
    """
    cleaned = 0
    current_time = time.time()

    if redis:
        try:
            all_states = await redis.hgetall(REDIS_TASK_STATE_KEY)
            for task_id, state_json in all_states.items():
                try:
                    state = TaskState.from_dict(json.loads(state_json))
                    # Clean up completed/failed/cancelled tasks older than TTL
                    if state.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                        age = current_time - (state.completed_at or state.created_at)
                        if age > state.ttl:
                            await delete_task_state(redis, task_id)
                            cleaned += 1
                except Exception as e:
                    log.error(f"Error processing task state during cleanup: {e}")
        except Exception as e:
            log.error(f"Failed to cleanup expired task states from Redis: {e}")
    else:
        # In-memory cleanup
        expired_tasks = []
        for task_id, state in task_states.items():
            if state.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                age = current_time - (state.completed_at or state.created_at)
                if age > state.ttl:
                    expired_tasks.append(task_id)
        
        for task_id in expired_tasks:
            task_states.pop(task_id, None)
            cleaned += 1

    if cleaned > 0:
        log.info(f"Cleaned up {cleaned} expired task states")
    return cleaned


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
    await redis.publish(REDIS_PUBSUB_CHANNEL, json.dumps(command))


async def cleanup_task(redis, task_id: str, id=None):
    """
    Remove a completed or canceled task from the global `tasks` dictionary.
    Also updates the task state if it exists.
    """
    # Check if task completed with error
    task = tasks.get(task_id)
    if task:
        try:
            if task.done() and not task.cancelled():
                exc = task.exception()
                if exc:
                    await update_task_state(
                        redis, task_id,
                        status=TaskStatus.FAILED,
                        completed_at=time.time(),
                        error=str(exc)
                    )
                else:
                    await update_task_state(
                        redis, task_id,
                        status=TaskStatus.COMPLETED,
                        completed_at=time.time()
                    )
            elif task.cancelled():
                await update_task_state(
                    redis, task_id,
                    status=TaskStatus.CANCELLED,
                    completed_at=time.time()
                )
        except Exception as e:
            log.error(f"Error updating task state on cleanup: {e}")

    if redis:
        await redis_cleanup_task(redis, task_id, id)

    tasks.pop(task_id, None)  # Remove the task if it exists

    # If an ID is provided, remove the task from the item_tasks dictionary
    if id and task_id in item_tasks.get(id, []):
        item_tasks[id].remove(task_id)
        if not item_tasks[id]:  # If no tasks left for this ID, remove the entry
            item_tasks.pop(id, None)


async def create_task(
    redis,
    coroutine,
    id=None,
    user_id: Optional[str] = None,
    message_id: Optional[str] = None,
    metadata: Optional[dict] = None,
    ttl: int = DEFAULT_TASK_TTL
):
    """
    Create a new asyncio task and add it to the global task dictionary.
    Also creates and persists the task state.
    """
    task_id = str(uuid4())  # Generate a unique ID for the task
    
    # Create task state with initial status
    task_state = TaskState(
        task_id=task_id,
        chat_id=id,
        message_id=message_id,
        user_id=user_id,
        status=TaskStatus.QUEUED,
        created_at=time.time(),
        metadata=metadata,
        ttl=ttl,
    )
    await save_task_state(redis, task_state)
    
    # Create wrapper coroutine to track processing state
    async def tracked_coroutine():
        await update_task_state(
            redis, task_id,
            status=TaskStatus.PROCESSING,
            started_at=time.time()
        )
        return await coroutine
    
    task = asyncio.create_task(tracked_coroutine())  # Create the task

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
    Also updates the task state to cancelled.
    """
    if redis:
        # Update state first
        await update_task_state(
            redis, task_id,
            status=TaskStatus.CANCELLED,
            completed_at=time.time()
        )
        # PUBSUB: All instances check if they have this task, and stop if so.
        await redis_send_command(
            redis,
            {
                "action": "stop",
                "task_id": task_id,
            },
        )
        return {"status": True, "message": f"Stop signal sent for {task_id}"}

    task = tasks.get(task_id)
    if not task:
        # Check if we have state for this task
        state = await get_task_state(None, task_id)
        if state:
            await update_task_state(
                None, task_id,
                status=TaskStatus.CANCELLED,
                completed_at=time.time()
            )
            return {"status": True, "message": f"Task {task_id} marked as cancelled."}
        return {"status": False, "message": f"Task with ID {task_id} not found."}

    # Update state
    await update_task_state(
        None, task_id,
        status=TaskStatus.CANCELLED,
        completed_at=time.time()
    )
    
    tasks.pop(task_id, None)
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


### ------------------------------
### PERIODIC CLEANUP
### ------------------------------


async def periodic_task_cleanup(app):
    """
    Periodically clean up expired task states.
    Runs every 15 minutes.
    """
    while True:
        await asyncio.sleep(900)  # 15 minutes
        try:
            redis = getattr(app.state, "redis", None)
            cleaned = await cleanup_expired_task_states(redis)
            if cleaned > 0:
                log.debug(f"Periodic cleanup: removed {cleaned} expired task states")
        except Exception as e:
            log.error(f"Error in periodic task cleanup: {e}")
