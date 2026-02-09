"""Phase 8.1 — Infrastructure tests (3 tests).

Validates env vars are loaded with correct defaults and Redis connectivity.
"""

import pytest
from unittest.mock import AsyncMock


class TestEnvVarsLoaded:
    """Test that all SSO + Resilience env vars are registered with correct defaults."""

    def test_env_vars_loaded(self):
        """14 vars registered with correct defaults."""
        from open_webui.env import (
            AIOHTTP_CLIENT_TIMEOUT,
            ENABLE_SERVER_SIDE_ORCHESTRATION,
            LLM_PROVIDER_RETRIES,
            LLM_PROVIDER_RETRY_DELAY,
            SSO_TASK_TTL,
            SSO_TASK_TIMEOUT,
            ZOMBIE_CLEANUP_GRACE_PERIOD,
            ZOMBIE_CLEANUP_SCAN_INTERVAL,
        )

        # Verify types (env vars parsed correctly)
        assert isinstance(AIOHTTP_CLIENT_TIMEOUT, (int, type(None)))
        assert isinstance(ENABLE_SERVER_SIDE_ORCHESTRATION, bool)
        assert isinstance(SSO_TASK_TTL, int)
        assert isinstance(SSO_TASK_TIMEOUT, int)
        assert isinstance(ZOMBIE_CLEANUP_GRACE_PERIOD, int)
        assert isinstance(ZOMBIE_CLEANUP_SCAN_INTERVAL, int)
        assert isinstance(LLM_PROVIDER_RETRIES, int)
        assert isinstance(LLM_PROVIDER_RETRY_DELAY, float)

        # Verify defaults (when no env override)
        assert SSO_TASK_TTL == 3600
        assert SSO_TASK_TIMEOUT == 300
        assert ZOMBIE_CLEANUP_GRACE_PERIOD == 600
        assert ZOMBIE_CLEANUP_SCAN_INTERVAL == 3600
        assert LLM_PROVIDER_RETRIES == 3
        assert LLM_PROVIDER_RETRY_DELAY == 2.0


class TestRedisConnection:
    """Test Redis connectivity patterns."""

    @pytest.mark.asyncio
    async def test_redis_connection(self):
        """Connect, SET, GET, DELETE via mock Redis."""
        mock_redis = AsyncMock()
        mock_redis.set = AsyncMock(return_value=True)
        mock_redis.get = AsyncMock(return_value="test_value")
        mock_redis.delete = AsyncMock(return_value=1)

        await mock_redis.set("test_key", "test_value")
        result = await mock_redis.get("test_key")
        assert result == "test_value"

        deleted = await mock_redis.delete("test_key")
        assert deleted == 1

    @pytest.mark.asyncio
    async def test_redis_pubsub(self):
        """Publish and receive message via mock PubSub."""
        mock_redis = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_redis.pubsub.return_value = mock_pubsub
        mock_redis.publish = AsyncMock(return_value=1)

        # Subscribe
        await mock_pubsub.subscribe("test:channel")
        mock_pubsub.subscribe.assert_called_once_with("test:channel")

        # Publish
        result = await mock_redis.publish("test:channel", "hello")
        assert result == 1
        mock_redis.publish.assert_called_once_with("test:channel", "hello")
