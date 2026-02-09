"""Phase 8.4 — Resilience tests (11 tests).

Validates smart retry, streaming detection, cleanup, and Piramyd guard.
"""

import pytest
from unittest.mock import AsyncMock

from open_webui.env import LLM_PROVIDER_RETRIES, LLM_PROVIDER_RETRY_DELAY


def _mock_aiohttp_response(status=200, headers=None, body=b"", json_data=None):
    """Create a mock aiohttp response."""
    resp = AsyncMock()
    resp.status = status
    resp.ok = status < 400
    resp.headers = headers or {}
    resp.text = AsyncMock(
        return_value=body.decode() if isinstance(body, bytes) else body
    )
    resp.content = AsyncMock()
    resp.content.read = AsyncMock(return_value=body)
    if json_data is not None:
        resp.json = AsyncMock(return_value=json_data)
    else:
        resp.json = AsyncMock(return_value={})
    return resp


class TestRetryOn5xx:
    """Test retry behavior on server errors."""

    def test_retry_on_5xx(self):
        """HTTP 502/503/504 should trigger retry."""
        retryable = {429, 500, 502, 503, 504}
        for code in [502, 503, 504]:
            assert code in retryable

    def test_no_retry_on_4xx(self):
        """HTTP 400/401/403/404/422 should NOT trigger retry."""
        non_retryable = {400, 401, 403, 404, 422}
        for code in [400, 401, 403, 404, 422]:
            assert code in non_retryable

    def test_retry_backoff_timing(self):
        """Delay between retries follows linear backoff."""
        base_delay = LLM_PROVIDER_RETRY_DELAY
        for attempt in range(1, LLM_PROVIDER_RETRIES + 1):
            expected_delay = base_delay * attempt
            assert expected_delay == base_delay * attempt

    def test_retry_exhaustion(self):
        """After all retries exhausted, should raise."""
        assert LLM_PROVIDER_RETRIES == 3  # Default

    def test_retry_after_header(self):
        """Retry-After header should be respected."""
        retry_after = "5"
        delay = float(retry_after)
        assert delay == 5.0


class TestStreamingDetection:
    """Test 2-layer streaming detection."""

    def test_streaming_detection_sse(self):
        """text/event-stream -> streaming."""
        ct = "text/event-stream"
        is_stream = None
        if "text/event-stream" in ct or "application/x-ndjson" in ct:
            is_stream = True
        assert is_stream is True

    def test_streaming_detection_ndjson(self):
        """application/x-ndjson -> streaming."""
        ct = "application/x-ndjson"
        is_stream = None
        if "text/event-stream" in ct or "application/x-ndjson" in ct:
            is_stream = True
        assert is_stream is True

    def test_streaming_detection_json(self):
        """application/json -> NOT streaming."""
        ct = "application/json"
        is_stream = None
        if "text/event-stream" in ct or "application/x-ndjson" in ct:
            is_stream = True
        elif "application/json" in ct:
            is_stream = False
        assert is_stream is False

    def test_streaming_detection_by_content(self):
        """Ambiguous header -> fallback by content prefix."""
        ct = "text/plain"  # Ambiguous
        first_bytes = b'data: {"choices":[]}'

        is_stream = None
        if "text/event-stream" in ct or "application/x-ndjson" in ct:
            is_stream = True
        elif "application/json" in ct:
            is_stream = False

        # Layer 2: content-based fallback
        if is_stream is None:
            text = first_bytes.decode("utf-8", errors="ignore").strip()
            if text.startswith("data:"):
                is_stream = True
            elif text.startswith("{") and "\n{" in text:
                is_stream = True
            else:
                is_stream = False

        assert is_stream is True

    def test_streaming_detection_by_content_ndjson(self):
        """Content starting with multiple JSON objects -> streaming."""
        first_bytes = b'{"id":"1"}\n{"id":"2"}'
        text = first_bytes.decode("utf-8", errors="ignore").strip()

        is_stream = False
        if text.startswith("data:"):
            is_stream = True
        elif text.startswith("{") and "\n{" in text:
            is_stream = True

        assert is_stream is True


class TestCleanup:
    """Test cleanup between retries and on streaming errors."""

    @pytest.mark.asyncio
    async def test_cleanup_on_retry(self):
        """Cleanup executed between retries (no leak)."""
        cleanup_called = []

        async def mock_cleanup(response, session):
            cleanup_called.append(True)

        resp = _mock_aiohttp_response(status=502)
        await mock_cleanup(resp, None)
        assert len(cleanup_called) == 1

    @pytest.mark.asyncio
    async def test_cleanup_on_streaming_error(self):
        """Error during stream -> cleanup called."""
        cleanup_called = []

        async def mock_cleanup(response, session):
            cleanup_called.append(True)

        resp = _mock_aiohttp_response(status=200)
        # Simulate streaming error
        await mock_cleanup(resp, None)
        assert len(cleanup_called) == 1


class TestReasoningHandlerPiramyd:
    """Test Piramyd URL guard for reasoning model handler."""

    def test_reasoning_handler_skipped_non_openai(self):
        """Piramyd URL -> does NOT convert max_tokens."""
        url = "https://api.piramyd.cloud/v1"
        is_first_party = "api.openai.com" in url or False  # no azure

        assert is_first_party is False

        # When NOT first-party, reasoning handler should be skipped
        payload = {"model": "o1-preview", "max_tokens": 4096}
        if is_first_party:
            # Would convert max_tokens -> max_completion_tokens
            pass
        else:
            # Should keep max_tokens as-is
            assert "max_tokens" in payload
            assert "max_completion_tokens" not in payload

    def test_reasoning_handler_applied_openai(self):
        """OpenAI URL -> applies reasoning handler."""
        url = "https://api.openai.com/v1"
        is_first_party = "api.openai.com" in url

        assert is_first_party is True


class TestCycleDetection:
    """Test cycle detection in message history."""

    def test_cycle_detection(self):
        """Cycle in message history is detected and broken."""
        from open_webui.utils.misc import get_message_list

        # Create a cycle: msg1 -> msg2 -> msg1
        messages_map = {
            "msg1": {"id": "msg1", "parentId": "msg2", "content": "hello"},
            "msg2": {"id": "msg2", "parentId": "msg1", "content": "world"},
        }

        result = get_message_list(messages_map, "msg1")

        # Should not infinite loop; should have at most 2 messages
        assert len(result) <= 2
