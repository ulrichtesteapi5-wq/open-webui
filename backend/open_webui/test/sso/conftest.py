"""SSO test suite configuration.

Installs lightweight mocks into sys.modules for heavy OWUI dependencies
(DB, ORM, config, migrations) so unit tests can import open_webui.utils.*
without triggering the full application bootstrap.
"""

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Module-level mock injection (runs at import time, before test collection)
# ---------------------------------------------------------------------------

_HEAVY_MODULES = [
    "open_webui.internal",
    "open_webui.internal.db",
    "open_webui.internal.wrappers",
    "open_webui.models",
    "open_webui.models.functions",
    "open_webui.models.users",
    "open_webui.models.chats",
    "open_webui.models.configs",
    "open_webui.models.groups",
    "open_webui.models.tools",
    "open_webui.config",
    "open_webui.migrations",
    "open_webui.migrations.util",
    "open_webui.routers",
    "open_webui.routers.retrieval",
    "open_webui.utils.access_control",
    "open_webui.utils.auth",
    "open_webui.utils.chat",
    "open_webui.utils.middleware",
    # open_webui.constants is lightweight (enums only) — NOT mocked
    "open_webui.socket",
    "open_webui.socket.main",
    "open_webui.functions",
    "open_webui.retrieval",
    "open_webui.retrieval.web",
    "open_webui.retrieval.web.main",
    "open_webui.storage",
    "open_webui.storage.provider",
    "open_webui.main",
]

_installed_mocks = {}

for _mod_name in _HEAVY_MODULES:
    if _mod_name not in sys.modules:
        _mock = types.ModuleType(_mod_name)
        _mock.__path__ = []
        _mock.__package__ = _mod_name
        sys.modules[_mod_name] = _mock
        _installed_mocks[_mod_name] = _mock

# Ensure Functions/Tools mocks have methods plugin.py expects
_functions_mod = sys.modules["open_webui.models.functions"]
_functions_mod.Functions = MagicMock()
_tools_mod = sys.modules["open_webui.models.tools"]
_tools_mod.Tools = MagicMock()

# Ensure open_webui.main has generate_chat_completion for rpg_summarizer
_main_mod = sys.modules["open_webui.main"]
_main_mod.generate_chat_completion = MagicMock()

# Ensure open_webui.utils.chat has generate_chat_completion
_chat_mod = sys.modules["open_webui.utils.chat"]
_chat_mod.generate_chat_completion = MagicMock()

# Force-import open_webui.utils.plugin now that all its deps are mocked,
# so that @patch("open_webui.utils.plugin.Functions") can resolve the path.
import open_webui.utils.plugin  # noqa: E402, F401

# Add Fork/Function to sys.path for rpg_summarizer_tool imports
# parents[5] = OWUI/ (Fork is sibling of open-webui/, not inside it)
_fork_function_dir = str(Path(__file__).resolve().parents[5] / "Fork" / "Function")
if _fork_function_dir not in sys.path:
    sys.path.insert(0, _fork_function_dir)


# ---------------------------------------------------------------------------
# Pytest hooks
# ---------------------------------------------------------------------------


def pytest_addoption(parser):
    """Add --run-integration CLI option."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires running server + Redis)",
    )


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration (requires running server + Redis)",
    )


def pytest_collection_modifyitems(config, items):
    """Skip integration tests unless --run-integration is passed."""
    if config.getoption("--run-integration"):
        return

    skip_integration = __import__("pytest").mark.skip(
        reason="Integration tests disabled. Use --run-integration to enable."
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
