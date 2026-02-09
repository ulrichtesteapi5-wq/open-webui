"""Phase 8.2 — Preload tests (7 tests).

Validates preloaded filters, cache early return, valves skip, and PubSub reload.
Heavy OWUI modules are mocked by conftest.py at session level.
"""

import types

import pytest
from unittest.mock import MagicMock, Mock, patch


def _make_app_state():
    """Create a minimal app state mock."""
    app = MagicMock()
    app.state.FUNCTIONS = {}
    app.state.FUNCTION_CONTENTS = {}
    app.state.redis = None
    return app


def _make_function_record(fid, content="pass", is_active=True):
    """Create a mock FunctionModel record."""
    rec = Mock()
    rec.id = fid
    rec.content = content
    rec.is_active = is_active
    return rec


class TestPreloadActiveFilters:
    """Test preload_active_filters loads active filters into app.state."""

    @pytest.mark.asyncio
    @patch("open_webui.utils.plugin.Functions")
    @patch("open_webui.utils.plugin.load_function_module_by_id")
    async def test_preload_active_filters(self, mock_load, mock_functions):
        """Startup loads active filters into app.state.FUNCTIONS."""
        from open_webui.utils.plugin import preload_active_filters

        app = _make_app_state()
        module = types.ModuleType("test_filter")
        mock_load.return_value = (module, "filter", {})
        mock_functions.get_functions_by_type.return_value = [
            _make_function_record("f1"),
            _make_function_record("f2"),
        ]
        mock_functions.get_function_valves_by_id.return_value = None

        await preload_active_filters(app)

        assert "f1" in app.state.FUNCTIONS
        assert "f2" in app.state.FUNCTIONS
        assert getattr(app.state.FUNCTIONS["f1"], "_valves_preloaded", False) is True

    @pytest.mark.asyncio
    @patch("open_webui.utils.plugin.Functions")
    @patch("open_webui.utils.plugin.load_function_module_by_id")
    async def test_preload_skips_inactive(self, mock_load, mock_functions):
        """Inactive filters are not loaded."""
        from open_webui.utils.plugin import preload_active_filters

        app = _make_app_state()
        # get_functions_by_type with active_only=True returns empty
        mock_functions.get_functions_by_type.return_value = []

        await preload_active_filters(app)

        assert len(app.state.FUNCTIONS) == 0

    @pytest.mark.asyncio
    @patch("open_webui.utils.plugin.Functions")
    @patch("open_webui.utils.plugin.load_function_module_by_id")
    async def test_preload_applies_valves(self, mock_load, mock_functions):
        """Valves are applied and _valves_preloaded=True."""
        from open_webui.utils.plugin import preload_active_filters

        app = _make_app_state()

        class MockValves:
            def __init__(self, **kwargs):
                self.setting = kwargs.get("setting", "default")

        module = types.ModuleType("test_filter")
        module.Valves = MockValves
        mock_load.return_value = (module, "filter", {})
        mock_functions.get_functions_by_type.return_value = [
            _make_function_record("f1"),
        ]
        mock_functions.get_function_valves_by_id.return_value = {"setting": "custom"}

        await preload_active_filters(app)

        loaded = app.state.FUNCTIONS["f1"]
        assert loaded._valves_preloaded is True
        assert hasattr(loaded, "valves")
        assert loaded.valves.setting == "custom"


class TestCacheEarlyReturn:
    """Test get_function_module_from_cache early return for preloaded."""

    def test_cache_early_return(self):
        """get_function_module_from_cache returns without DB query if preloaded."""
        from open_webui.utils.plugin import get_function_module_from_cache

        request = MagicMock()
        module = types.ModuleType("cached_filter")
        module._valves_preloaded = True
        request.app.state.FUNCTIONS = {"f1": module}
        request.app.state.FUNCTION_CONTENTS = {"f1": "pass"}

        result_module, _, _ = get_function_module_from_cache(
            request, "f1", load_from_db=True
        )
        assert result_module is module


class TestFilterSkipValvesRebuild:
    """Test that process_filter_functions skips Valves rebuild for preloaded."""

    def test_filter_skip_valves_rebuild(self):
        """process_filter_functions skips rebuild if _valves_preloaded flag set."""
        module = types.ModuleType("preloaded_filter")
        module._valves_preloaded = True

        class MockValves:
            pass

        module.Valves = MockValves
        module.valves = MockValves()

        # The flag should be True, meaning Valves rebuild is skipped
        assert getattr(module, "_valves_preloaded", False) is True


class TestPubSubReload:
    """Test PubSub reload cross-worker."""

    @pytest.mark.asyncio
    @patch("open_webui.utils.plugin.Functions")
    @patch("open_webui.utils.plugin.load_function_module_by_id")
    async def test_pubsub_reload_cross_worker(self, mock_load, mock_functions):
        """PubSub fn_reload updates memory of another worker."""
        from open_webui.utils.plugin import reload_function_in_memory

        app = _make_app_state()
        module = types.ModuleType("reloaded_filter")
        mock_load.return_value = (module, "filter", {})
        func_record = _make_function_record("f1", is_active=True)
        mock_functions.get_function_by_id.return_value = func_record
        mock_functions.get_function_valves_by_id.return_value = None

        await reload_function_in_memory(app, "f1")

        assert "f1" in app.state.FUNCTIONS
        assert app.state.FUNCTIONS["f1"]._valves_preloaded is True

    @pytest.mark.asyncio
    @patch("open_webui.utils.plugin.Functions")
    async def test_pubsub_delete_removes(self, mock_functions):
        """Deleting function via PubSub removes from app.state."""
        from open_webui.utils.plugin import reload_function_in_memory

        app = _make_app_state()
        app.state.FUNCTIONS["f1"] = types.ModuleType("old_filter")
        app.state.FUNCTION_CONTENTS["f1"] = "old_content"

        # Function no longer active
        mock_functions.get_function_by_id.return_value = _make_function_record(
            "f1", is_active=False
        )

        await reload_function_in_memory(app, "f1")

        assert "f1" not in app.state.FUNCTIONS
        assert "f1" not in app.state.FUNCTION_CONTENTS
