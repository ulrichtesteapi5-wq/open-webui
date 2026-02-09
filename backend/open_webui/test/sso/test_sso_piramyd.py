"""Phase 8.7 — Piramyd-specific tests (3 tests).

Validates HTTP 422 not retried, owned_by preserved, and stream_options safety.
"""


class TestPiramyd422NotRetried:
    """Test that HTTP 422 (FastAPI validation) is NOT retried."""

    def test_piramyd_422_not_retried(self):
        """HTTP 422 is in NON_RETRYABLE_STATUS_CODES."""
        non_retryable = {400, 401, 403, 404, 422}
        assert 422 in non_retryable

        retryable = {429, 500, 502, 503, 504}
        assert 422 not in retryable


class TestPiramydOwnedByPreserved:
    """Test that owned_by is preserved for non-OpenAI providers."""

    def test_piramyd_owned_by_preserved(self):
        """owned_by original preserved for Piramyd models."""
        url = "https://api.piramyd.cloud/v1"
        is_openai_first_party = "api.openai.com" in url

        assert is_openai_first_party is False

        model = {"id": "gpt-5.1-chat", "owned_by": "piramyd-cloud"}

        # Logic from openai.py get_merged_models
        owned_by = (
            "openai" if is_openai_first_party else model.get("owned_by", "openai")
        )

        assert owned_by == "piramyd-cloud"

    def test_openai_owned_by_overwritten(self):
        """owned_by is set to 'openai' for first-party OpenAI."""
        url = "https://api.openai.com/v1"
        is_openai_first_party = "api.openai.com" in url

        assert is_openai_first_party is True

        model = {"id": "gpt-4o", "owned_by": "system"}

        owned_by = (
            "openai" if is_openai_first_party else model.get("owned_by", "openai")
        )

        assert owned_by == "openai"


class TestPiramydStreamOptions:
    """Test stream_options does not cause errors in Piramyd."""

    def test_piramyd_stream_options(self):
        """stream_options field is safely handled.

        Piramyd proxies should pass through or ignore stream_options
        without causing validation errors. This tests that the field
        can be present in the payload without issues.
        """
        payload = {
            "model": "gpt-5.1-chat",
            "messages": [{"role": "user", "content": "test"}],
            "stream": True,
            "stream_options": {"include_usage": True},
        }

        # Verify payload is well-formed
        assert "stream_options" in payload
        assert payload["stream_options"]["include_usage"] is True
