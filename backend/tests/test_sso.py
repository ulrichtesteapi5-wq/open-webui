import sys
from unittest.mock import MagicMock, AsyncMock

# Helper to create mock packages that support submodules
def mock_package(name):
    mock = MagicMock()
    sys.modules[name] = mock
    return mock

# ALL MOCKS
mock_package("markdown")
mock_package("requests")
mock_package("aiohttp")
mock_package("aiofiles")
mock_package("uvicorn")
mock_package("typer")
mock_package("bcrypt")
mock_package("passlib")
mock_package("passlib.context")
mock_package("passlib.hash")
mock_package("jwt")
mock_package("psutil")
mock_package("apscheduler")
mock_package("apscheduler.schedulers")
mock_package("apscheduler.schedulers.background")
mock_package("cryptography")
mock_package("cryptography.hazmat")
mock_package("cryptography.hazmat.primitives")
mock_package("cryptography.hazmat.primitives.serialization")
mock_package("cryptography.hazmat.backends")
mock_package("aiocache")
mock_package("starlette_compress")
mock_package("peewee")
mock_package("playhouse")
mock_package("playhouse.db_url")
mock_package("playhouse.pool")
mock_package("playhouse.shortcuts")
mock_package("playhouse.cockroachdb")
mock_package("peewee_migrate")
mock_package("starsessions")
mock_package("socketio")
mock_package("authlib")
mock_package("authlib.integrations")
mock_package("authlib.integrations.starlette_client")
mock_package("jose")
mock_package("jose.jwt")
mock_package("nltk")
mock_package("pandas")
mock_package("cv2")
mock_package("PIL")
mock_package("sentence_transformers")
mock_package("tiktoken")
mock_package("google")
mock_package("google.genai")
mock_package("anthropic")
mock_package("openai")
mock_package("langchain")
mock_package("langchain_community")
mock_package("langchain_classic")
mock_package("langchain_text_split_ters")
mock_package("azure")
mock_package("azure.identity")
mock_package("azure.storage")
mock_package("azure.storage.blob")
mock_package("azure.search")
mock_package("azure.search.documents")
mock_package("azure.ai")
mock_package("azure.ai.documentintelligence")
mock_package("bs4")
mock_package("bs4.BeautifulSoup")
mock_package("chromadb")
mock_package("chromadb.utils")
mock_package("chromadb.utils.embedding_functions")
mock_package("weaviate")
mock_package("weaviate.classes")
mock_package("pydantic_settings")
mock_package("argon2")
mock_package("pycrdt")
mock_package("mimeparse")

# Open WebUI internals mocks
mock_package("open_webui.internal.db")
mock_package("open_webui.internal.wrappers")
mock_package("open_webui.models.users")
mock_package("open_webui.models.chats")
mock_package("open_webui.models.models")
mock_package("open_webui.models.functions")
mock_package("open_webui.models.files")
mock_package("open_webui.models.tools")
mock_package("open_webui.utils.task")
mock_package("open_webui.utils.auth")
mock_package("open_webui.utils.middleware")
mock_package("open_webui.utils.plugin")
mock_package("open_webui.utils.access_control")
mock_package("open_webui.utils.payload")
mock_package("open_webui.utils.misc")
mock_package("open_webui.routers.pipelines")
mock_package("open_webui.socket.main")
mock_package("open_webui.functions")
mock_package("open_webui.routers.openai")
mock_package("open_webui.routers.ollama")

# Mock config and env to avoid database access and complex logic
config_mock = mock_package("open_webui.config")
config_mock.get_config.return_value = MagicMock()
config_mock.DEFAULT_USER_PERMISSIONS = {}

env_mock = mock_package("open_webui.env")
env_mock.REDIS_KEY_PREFIX = "test_prefix"
env_mock.ENABLE_SERVER_SIDE_ORCHESTRATION = True
env_mock.GLOBAL_LOG_LEVEL = "DEBUG"
env_mock.BYPASS_MODEL_ACCESS_CONTROL = False

import pytest
import asyncio
import time
import json
from unittest.mock import patch
from fastapi import FastAPI, Request
from httpx import AsyncClient, ASGITransport

# Mock components that are hard to initialize
from open_webui.tasks import (
    TaskState, TaskStatus, create_task, get_task_state, 
    update_task_state, stop_task, stop_item_tasks, cleanup_task,
    cleanup_expired_task_states, tasks as active_tasks, 
    task_states as memory_task_states, item_tasks as memory_item_tasks
)
from open_webui.routers.tasks import router as tasks_router
from open_webui.utils.chat import ServerSideChatManager, generate_chat_completion

@pytest.fixture(autouse=True)
def clear_stores():
    active_tasks.clear()
    memory_task_states.clear()
    memory_item_tasks.clear()

@pytest.mark.asyncio
async def test_task_state_serialization():
    """Teste a serialização e desserialização de TaskState."""
    state = TaskState(
        task_id="test-task",
        chat_id="test-chat",
        user_id="test-user",
        status=TaskStatus.PROCESSING,
        metadata={"model": "gpt-4"}
    )
    data = state.to_dict()
    assert data["task_id"] == "test-task"
    assert data["status"] == "processing"
    assert data["metadata"]["model"] == "gpt-4"
    
    new_state = TaskState.from_dict(data)
    assert new_state.task_id == "test-task"
    assert str(new_state.status) == "TaskStatus.PROCESSING" or new_state.status == TaskStatus.PROCESSING
    assert new_state.metadata["model"] == "gpt-4"

@pytest.mark.asyncio
async def test_create_task_flow():
    """Teste o fluxo completo de criação de task e atualização automática de estado."""
    redis = None
    
    async def mock_coro():
        await asyncio.sleep(0.1)
        return "success"
        
    task_id, task = await create_task(
        redis, mock_coro(), id="chat-1", user_id="user-1"
    )
    
    state = await get_task_state(redis, task_id)
    assert state is not None
    assert state.status == TaskStatus.QUEUED
    assert state.chat_id == "chat-1"
    
    # Aguarda a execução
    await task
    # A atualização do estado acontece no callback, aguarda um pouco para processar
    await asyncio.sleep(0.05) 
    
    state = await get_task_state(redis, task_id)
    assert state.status == TaskStatus.COMPLETED
    assert state.completed_at is not None
    assert state.started_at is not None

@pytest.mark.asyncio
async def test_stop_task_logical():
    """Teste o cancelamento lógico de uma task."""
    redis = None
    
    async def long_coro():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            # Re-raise to trigger cancellation logic in cleanup
            raise
            
    task_id, task = await create_task(redis, long_coro())
    
    result = await stop_task(redis, task_id)
    assert result["status"] is True
    
    # Aguarda cleanup
    await asyncio.sleep(0.1)
    
    state = await get_task_state(redis, task_id)
    assert state.status == TaskStatus.CANCELLED
    assert task.cancelled()

@pytest.mark.asyncio
async def test_stop_item_tasks():
    """Teste cancelar todas as tasks de um chat."""
    redis = None
    
    async def long_coro():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise
            
    tid1, _ = await create_task(redis, long_coro(), id="chat-1")
    tid2, _ = await create_task(redis, long_coro(), id="chat-1")
    tid3, _ = await create_task(redis, long_coro(), id="chat-other")
    
    result = await stop_item_tasks(redis, "chat-1")
    assert result["status"] is True
    
    await asyncio.sleep(0.1)
    
    s1 = await get_task_state(redis, tid1)
    s2 = await get_task_state(redis, tid2)
    s3 = await get_task_state(redis, tid3)
    
    assert s1.status == TaskStatus.CANCELLED
    assert s2.status == TaskStatus.CANCELLED
    assert s3.status in [TaskStatus.QUEUED, TaskStatus.PROCESSING]  # Não deve ser cancelado

@pytest.mark.asyncio
async def test_cleanup_expired_states():
    """Teste limpeza de estados expirados."""
    redis = None
    
    # Criar uma task expirada (feita há 2 horas com TTL de 1 hora)
    expired_task = TaskState(
        task_id="expired",
        status=TaskStatus.COMPLETED,
        completed_at=time.time() - 7200,
        ttl=3600
    )
    # Criar uma task recente
    fresh_task = TaskState(
        task_id="fresh",
        status=TaskStatus.COMPLETED,
        completed_at=time.time(),
        ttl=3600
    )
    
    memory_task_states["expired"] = expired_task
    memory_task_states["fresh"] = fresh_task
    
    cleaned = await cleanup_expired_task_states(redis)
    assert cleaned == 1
    assert "expired" not in memory_task_states
    assert "fresh" in memory_task_states

# --- API Endpoints Tests ---

@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(tasks_router, prefix="/api/v1/tasks")
    app.state.redis = None
    app.state.config = MagicMock()
    app.state.MODELS = {}
    return app

@pytest.mark.asyncio
async def test_api_get_task_status(app):
    task_id = "test-api-task"
    state = TaskState(task_id=task_id, status=TaskStatus.PROCESSING, user_id="user-1")
    memory_task_states[task_id] = state
    
    from open_webui.utils.auth import get_verified_user
    app.dependency_overrides[get_verified_user] = lambda: MagicMock(id="user-1", role="user")
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get(f"/api/v1/tasks/status/{task_id}")
        
    assert response.status_code == 200
    data = response.json()
    assert data["task_id"] == task_id
    assert data["status"] == "processing"

@pytest.mark.asyncio
async def test_api_cancel_task(app):
    task_id = "cancel-api-task"
    
    async def long_coro():
        await asyncio.sleep(5)
        
    tid, task = await create_task(None, long_coro(), user_id="user-1")
    # Substituir ID gerado pelo fixo do teste para facilitar
    memory_task_states.pop(tid)
    tid = task_id
    memory_task_states[tid] = TaskState(task_id=tid, status=TaskStatus.QUEUED, user_id="user-1")
    active_tasks[tid] = task

    from open_webui.utils.auth import get_verified_user
    app.dependency_overrides[get_verified_user] = lambda: MagicMock(id="user-1", role="user")
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post(f"/api/v1/tasks/status/{tid}/cancel")
        
    assert response.status_code == 200
    assert response.json()["status"] is True
    assert task.cancelled()

@pytest.mark.asyncio
async def test_api_get_chat_tasks(app):
    chat_id = "chat-api-test"
    tid1, _ = await create_task(None, asyncio.sleep(0.1), id=chat_id, user_id="user-1")
    
    from open_webui.utils.auth import get_verified_user
    from open_webui.models.chats import Chats
    app.dependency_overrides[get_verified_user] = lambda: MagicMock(id="user-1", role="user")
    
    with patch("open_webui.models.chats.Chats.get_chat_by_id") as mock_get_chat:
        mock_get_chat.return_value = MagicMock(user_id="user-1")
        
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.get(f"/api/v1/tasks/status/chat/{chat_id}")
            
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["task_id"] == tid1

# --- ServerSide Orchestration Tests ---

@pytest.mark.asyncio
async def test_orchestration_trigger():
    """Teste se o generate_chat_completion dispara a orquestração (202 Accepted)."""
    app = FastAPI()
    app.state.redis = None
    app.state.config = MagicMock()
    app.state.MODELS = {"model-1": {"id": "model-1"}}
    
    mock_request = MagicMock(spec=Request)
    mock_request.app = app
    mock_request.state.metadata = {"chat_id": "real-chat", "message_id": "msg-1"}
    mock_request.headers = {"authorization": "Bearer test"}
    mock_request.cookies = {}
    
    user = MagicMock()
    user.id = "user-1"
    
    form_data = {
        "model": "model-1",
        "messages": [{"role": "user", "content": "hi"}]
    }
    
    with patch("open_webui.env.ENABLE_SERVER_SIDE_ORCHESTRATION", True):
        # Mock create_task to return a fixed ID
        with patch("open_webui.tasks.create_task", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = ("task-123", MagicMock())
            
            response = await generate_chat_completion(
                mock_request, form_data, user, orchestrate=True
            )
            
            assert response.status_code == 202
            content = json.loads(response.body)
            assert content["task_id"] == "task-123"
            assert content["status"] == "processing"

@pytest.mark.asyncio
async def test_orchestrate_logic():
    """Teste a lógica interna do orchestrate (snapshot para execução)."""
    app_state = MagicMock()
    app_state.redis = None
    
    snapshot = {
        "chat_id": "chat-1",
        "message_id": "msg-1",
        "user_id": "user-1",
        "user_role": "user",
        "metadata": {"chat_id": "chat-1", "message_id": "msg-1"},
        "form_data": {"model": "m1", "messages": []},
        "model": {"id": "m1"},
        "headers": {},
        "cookies": {}
    }
    
    # Mock das funções chamadas internamente pelo orchestrate
    with patch("open_webui.utils.middleware.process_chat_payload", new_callable=AsyncMock) as p_payload, \
         patch("open_webui.utils.chat._generate_chat_completion_direct", new_callable=AsyncMock) as p_gen, \
         patch("open_webui.utils.middleware.process_chat_response", new_callable=AsyncMock) as p_resp:
        
        p_payload.return_value = (snapshot["form_data"], snapshot["metadata"], [])
        p_gen.return_value = MagicMock()
        p_resp.return_value = AsyncMock()
        
        await ServerSideChatManager.orchestrate(snapshot, app_state)
        
        assert p_payload.called
        assert p_gen.called
        assert p_resp.called
