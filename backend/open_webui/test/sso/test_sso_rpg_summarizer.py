"""Phase 8.5 — RPG Summarizer tests (6 tests).

Validates Redis cache, cache TTL, backend LLM calls, and no aiohttp session.
conftest.py adds Fork/Function to sys.path and mocks open_webui.main.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rpg_summarizer_tool import Filter


def _new_filter():
    """Create a bare Filter instance without __init__."""
    f = Filter.__new__(Filter)
    f._cache_locks = {}
    return f


class TestSummarizerRedisCache:
    """Test Redis cache integration for RPG Summarizer."""

    @pytest.mark.asyncio
    async def test_summarizer_redis_cache(self):
        """Cache saves and retrieves from Redis (HSET/HGETALL)."""
        f = _new_filter()

        mock_redis = AsyncMock()
        mock_redis.hgetall = AsyncMock(return_value={"key1": "summary1"})
        mock_redis.hset = AsyncMock()
        mock_redis.expire = AsyncMock()

        mock_request = MagicMock()
        mock_request.app.state.redis = mock_redis
        f.__request__ = mock_request
        f.valves = MagicMock()
        f.valves.enable_cache = True
        f.valves.cache_ttl = 86400

        # Test load
        result = await f._load_cache("test_chat")
        assert result == {"key1": "summary1"}
        mock_redis.hgetall.assert_called_once_with("open-webui:rpg_cache:test_chat")

        # Test save
        await f._save_cache("test_chat", {"key2": "summary2"})
        mock_redis.hset.assert_called_once_with(
            "open-webui:rpg_cache:test_chat", mapping={"key2": "summary2"}
        )
        mock_redis.expire.assert_called_once_with(
            "open-webui:rpg_cache:test_chat", 86400
        )

    @pytest.mark.asyncio
    async def test_summarizer_cache_ttl(self):
        """Cache expires after cache_ttl seconds."""
        f = _new_filter()

        mock_redis = AsyncMock()
        mock_redis.hset = AsyncMock()
        mock_redis.expire = AsyncMock()

        mock_request = MagicMock()
        mock_request.app.state.redis = mock_redis
        f.__request__ = mock_request
        f.valves = MagicMock()
        f.valves.enable_cache = True
        f.valves.cache_ttl = 3600

        await f._save_cache("test_chat", {"k": "v"})

        mock_redis.expire.assert_called_once_with(
            "open-webui:rpg_cache:test_chat", 3600
        )


class TestSummarizerBackendLLM:
    """Test internal backend LLM calls."""

    @pytest.mark.asyncio
    async def test_summarizer_backend_llm(self):
        """LLM called via generate_chat_completion(bypass_filter=True)."""
        f = _new_filter()
        f.valves = MagicMock()
        f.valves.model = "test-model"
        f.valves.temperature = 0.7
        f.__request__ = MagicMock()
        f.__user__ = MagicMock()

        mock_response = MagicMock()
        mock_response.body = json.dumps(
            {"choices": [{"message": {"content": "Test summary"}}]}
        ).encode()

        with patch(
            "open_webui.main.generate_chat_completion",
            new_callable=AsyncMock,
            return_value=mock_response,
        ) as mock_gen:
            result = await f._chat_completion_via_backend(
                [{"role": "user", "content": "Summarize this"}]
            )

            assert result == "Test summary"
            mock_gen.assert_called_once()
            assert mock_gen.call_args.kwargs.get("bypass_filter") is True

    @pytest.mark.asyncio
    async def test_summarizer_backend_llm_no_task(self):
        """Internal LLM call does NOT create SSO task (bypass_filter=True)."""
        f = _new_filter()
        f.valves = MagicMock()
        f.valves.model = "test-model"
        f.valves.temperature = 0.7
        f.__request__ = MagicMock()
        f.__user__ = MagicMock()

        mock_response = {"choices": [{"message": {"content": "OK"}}]}

        with patch(
            "open_webui.main.generate_chat_completion",
            new_callable=AsyncMock,
            return_value=mock_response,
        ) as mock_gen:
            await f._chat_completion_via_backend([{"role": "user", "content": "test"}])
            assert mock_gen.call_args.kwargs.get("bypass_filter") is True

    @pytest.mark.asyncio
    async def test_summarizer_bypass_filter(self):
        """Internal LLM call does NOT re-execute filters (avoids loop)."""
        f = _new_filter()
        f.valves = MagicMock()
        f.valves.model = "test-model"
        f.valves.temperature = 0.7
        f.__request__ = MagicMock()
        f.__user__ = MagicMock()

        mock_response = {"choices": [{"message": {"content": "OK"}}]}

        with patch(
            "open_webui.main.generate_chat_completion",
            new_callable=AsyncMock,
            return_value=mock_response,
        ) as mock_gen:
            await f._chat_completion_via_backend([{"role": "user", "content": "test"}])
            assert mock_gen.call_args.kwargs.get("bypass_filter") is True


class TestSummarizerNoAiohttp:
    """Test that Summarizer prefers backend over aiohttp."""

    @pytest.mark.asyncio
    async def test_summarizer_no_aiohttp(self):
        """When __request__ is available, no aiohttp.ClientSession is created."""
        f = _new_filter()
        f.valves = MagicMock()
        f.valves.model = "test-model"
        f.valves.temperature = 0.7
        f.__request__ = MagicMock()
        f.__user__ = MagicMock()

        mock_response = {"choices": [{"message": {"content": "OK"}}]}

        with patch(
            "open_webui.main.generate_chat_completion",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            with patch("aiohttp.ClientSession") as mock_session_cls:
                result = await f._chat_completion([{"role": "user", "content": "test"}])
                mock_session_cls.assert_not_called()
                assert result == "OK"
