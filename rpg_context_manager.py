"""
title: 🧠 RPG Context Manager (V5.2.1 Optimized)
description: Unified Schema, Professional-Grade Robustness, Micro-Optimized.
author: opencode
version: 5.2.1
requirements: qdrant-client>=1.9.0, aiohttp

Medical Grade Application:
    - All operations are designed for reliability and safety
    - Comprehensive error handling and logging
    - Thread-safe lock management with automatic cleanup

Optimizations (V5.2.1):
    - Enhanced extraction prompts with importance levels
    - Improved summary prompt for better RAG retrieval
    - Explicit entity extraction rules
    - Deduplication instructions in prompts
    - Structured output sections in summaries
    - Pre-compiled regex patterns for JSON cleanup
    - Multiplication instead of division for token estimation
    - Frozensets for O(1) membership tests
    - Cached event loop references
    - Set operations for keyword extraction
    - Optimized comprehensions and generators
    - LRU cache for HTTP headers
    - Reduced object allocations
    - Loop-invariant code motion
    - Extracted magic numbers to named constants
    - Frozenset for HIGH_TENSION_VALUES (O(1) lookup)
    - Operator.itemgetter for faster sorting
    - Role constants (ROLE_USER, ROLE_ASSISTANT, ROLE_SYSTEM)
    - EMPTY_DICT_LIST for API fallbacks
    - Generator expressions instead of list comprehensions in joins
"""

import asyncio
import os
import json
import re
import random
import uuid
import logging
from time import time as time_now, monotonic as time_monotonic
from functools import partial, lru_cache
from operator import itemgetter
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    List,
    Optional,
    Dict,
    Any,
    Set,
    FrozenSet,
    Tuple,
    Final,
    cast,
    Union,
)

import aiohttp
from pydantic import BaseModel, Field

# Conditional Redis Import
try:
    import redis.asyncio as redis

    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False
    if not TYPE_CHECKING:
        redis = Any

if TYPE_CHECKING:
    from qdrant_client import QdrantClient
    from qdrant_client.models import (
        Direction,
        Distance,
        FieldCondition,
        MatchAny,
        MatchValue,
        OrderBy,
        Filter as QFilter,
        PayloadSchemaType,
        VectorParams,
        PointStruct,
        PointIdsList,
    )

    if HAS_REDIS:
        from redis.asyncio import Lock as RedisLock

# ══════════════════════════════════════════════════════════════════
# LOGGING & SETUP
# ══════════════════════════════════════════════════════════════════

logger = logging.getLogger("rpg_manager")
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Adicionar Handler de Arquivo para debug persistente
    try:
        file_handler = logging.FileHandler("rpg_debug.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.warning(f"Falha ao configurar log em arquivo: {e}")

    logger.setLevel(logging.INFO)

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import (
        Direction,
        Distance,
        FieldCondition,
        MatchAny,
        MatchValue,
        OrderBy,
        Filter as QFilter,
        PayloadSchemaType,
        VectorParams,
        PointStruct,
        PointIdsList,
    )

    HAS_QDRANT = True
except ImportError:
    HAS_QDRANT = False
    logger.critical("qdrant-client not installed. Functionality disabled.")

    # Placeholders for type checking robustness when not TYPE_CHECKING
    if not TYPE_CHECKING:
        QdrantClient = Any  # type: ignore
        Direction = Any  # type: ignore
        Distance = Any  # type: ignore
        FieldCondition = Any  # type: ignore
        MatchAny = Any  # type: ignore
        MatchValue = Any  # type: ignore
        OrderBy = Any  # type: ignore
        QFilter = Any  # type: ignore
        PayloadSchemaType = Any  # type: ignore
        VectorParams = Any  # type: ignore
        PointStruct = Any  # type: ignore
        PointIdsList = Any  # type: ignore


# ══════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════

# Truncation limits (named constants for maintainability)
MAX_TEXT_FOR_EMBEDDING: Final[int] = 8000
MAX_USER_TEXT_EXTRACT: Final[int] = 1500
MAX_ASSISTANT_TEXT_EXTRACT: Final[int] = 4000
MAX_USER_TEXT_MEMORY: Final[int] = 2000
MAX_ASSISTANT_TEXT_MEMORY: Final[int] = 5000
MAX_FILE_CONTENT_CHARS: Final[int] = 15000
MAX_NGRAM_TEXT_LEN: Final[int] = 400

# Token estimation divisor (chars per token approximation)
TOKEN_ESTIMATION_MULTIPLIER: Final[float] = 1 / 2.7

# Tension icons mapping (immutable)
TENSION_ICONS: Final[Dict[str, str]] = {
    "low": "🟢",
    "mid": "🟡",
    "high": "🔴",
    "crit": "🔥",
}

# Default tension value (avoid repeated string literals)
DEFAULT_TENSION: Final[str] = "low"

ICON_URL = (
    "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAw"
    "MC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0IiBm"
    "aWxsPSJub25lIiBzdHJva2U9ImN1cnJlbnRDb2xvciIgc3Ryb2tlLXdpZHRoPSIyIiBz"
    "dHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiPjxwYXRo"
    "IGQ9Ik0xMiAyTDIgN2wxMCA1IDEwLTUtMTAtNXpNMiAxN2wxMCA1IDEwLTVNMiAxMmwx"
    "MCA1IDEwLTUiLz48L3N2Zz4="
)

EXTRACTION_USER_TEMPLATE = (
    "**USER MESSAGE:**\n{user_message}\n\n"
    "**ASSISTANT RESPONSE:**\n{assistant_response}"
)


# Common HTTP headers (cached to avoid repeated dict creation)
@lru_cache(maxsize=4)
def _make_auth_headers(api_key: str) -> Dict[str, str]:
    """Create authorization headers for API calls (cached)."""
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


# Global Compiled Regex for Optimization
KEYWORD_REGEX = re.compile(r"\b[A-ZÀ-Ú][a-zà-ú]{2,}\b")
CODE_BLOCK_PATTERN = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)
# Pre-compiled regex for JSON cleanup (used in multiple places)
JSON_FENCE_PATTERN = re.compile(r"```(?:json)?\n?|\n?```", re.IGNORECASE)

# Important subtypes for score boosting (frozenset for O(1) lookup)
IMPORTANT_SUBTYPES: FrozenSet[str] = frozenset(
    {"INV", "PSYCHE", "DIALOG", "TIME", "GOAL", "REL", "PLOT"}
)

# Retryable HTTP status codes (frozenset for O(1) lookup)
RETRYABLE_STATUS_CODES: FrozenSet[int] = frozenset({429, 500, 502, 503, 504})

# High tension values for adaptive RAG (frozenset for O(1) membership test)
HIGH_TENSION_VALUES: FrozenSet[str] = frozenset({"high", "crit"})

# Combat-relevant memory types for tension boost
COMBAT_RELEVANT_TYPES: FrozenSet[str] = frozenset({"fact", "mech"})
COMBAT_RELEVANT_SUBTYPES: FrozenSet[str] = frozenset({"MECH", "INV", "PSYCHE"})

# Scroll limit for pruning operations
PRUNE_SCROLL_LIMIT: int = 1000

# Lock manager maintenance interval
LOCK_PRUNE_INTERVAL: int = 50

# Similarity threshold for deduplication
DEDUPE_SIMILARITY_THRESHOLD: float = 0.8

# Default category for facts without category
DEFAULT_FACT_CATEGORY: str = "GENERIC"

# Important subtype boost multiplier
IMPORTANT_SUBTYPE_BOOST: Final[float] = 1.15

# Importance high boost multiplier
IMPORTANCE_HIGH_BOOST: Final[float] = 1.1

# Tension boost for combat-relevant memories
TENSION_BOOST_VALUE: Final[float] = 1.2

# Recency bonus maximum value
RECENCY_BONUS_MAX: Final[float] = 0.15

# Version constant (DRY - used in status messages)
VERSION: Final[str] = "5.2.1"

# Empty frozen containers (avoid repeated allocations)
EMPTY_DICT: Dict[str, Any] = {}
EMPTY_DICT_LIST: Final[List[Dict[str, Any]]] = [{}]  # For API response fallbacks

# Fallback summary text template (DRY - used in 3 places)
FALLBACK_SUMMARY_TEMPLATE: Final[str] = (
    "Registro de Sistema: Falha no processamento de dados narrativos "
    "para os turnos {t_start}-{t_end}. Dados corrompidos ou ilegíveis."
)

# Truncation limit for summary context prefix (DRY - used in 3 places)
SUMMARY_CONTEXT_TRUNCATE: Final[int] = 2000

# Fallback text for embedding when primary fails (DRY - used in 2 places)
FALLBACK_EMBEDDING_TEXT: Final[str] = "System Error Data Corruption"

# Role constants (avoid repeated string literals)
ROLE_USER: Final[str] = "user"
ROLE_ASSISTANT: Final[str] = "assistant"
ROLE_SYSTEM: Final[str] = "system"

# Retry jitter bounds for exponential backoff
RETRY_JITTER_MIN: float = 0.1
RETRY_JITTER_MAX: float = 0.5

DEFAULT_STOPWORDS: FrozenSet[str] = frozenset(
    {
        "O",
        "A",
        "Os",
        "As",
        "Um",
        "Uma",
        "Uns",
        "Umas",
        "Eu",
        "Tu",
        "Ele",
        "Ela",
        "Nós",
        "Vós",
        "Eles",
        "Elas",
        "Você",
        "De",
        "Da",
        "Do",
        "Das",
        "Dos",
        "Em",
        "Na",
        "No",
        "Nas",
        "Nos",
        "Para",
        "Por",
        "Com",
        "Sem",
        "Sobre",
        "Sob",
        "Entre",
        "Até",
        "E",
        "Ou",
        "Mas",
        "Porém",
        "Contudo",
        "Todavia",
        "Se",
        "Quando",
        "Porque",
        "Pois",
        "Como",
        "Onde",
        "Que",
        "Quem",
        "Qual",
        "Mestre",
        "Jogador",
        "Então",
        "Logo",
        "Assim",
        "Portanto",
        "Talvez",
        "Sim",
        "Não",
        "Agora",
        "Depois",
        "Tudo",
        "Nada",
        "Isso",
        "Aquilo",
        "Aqui",
        "Ali",
        "Lá",
    }
)

# ══════════════════════════════════════════════════════════════════
# GLOBAL STATE MANAGER (SAFE & CLEAN)
# ══════════════════════════════════════════════════════════════════


class AbstractLockManager:
    """Interface for lock managers."""

    async def get_lock(self, chat_id: str) -> Union[asyncio.Lock, "RedisLock"]:
        raise NotImplementedError

    async def close(self):
        pass


class MemoryLockManager(AbstractLockManager):
    """
    Manages chat locks with automatic cleanup to prevent memory leaks.

    Medical Grade:
        - Uses time.monotonic() for temporal robustness (immune to NTP drift).
        - Safety checks to avoid removing active locks.
        - Thread-safe access counter with guard lock.
    """

    __slots__ = (
        "_locks",
        "_last_access",
        "_guard",
        "_stale_timeout",
        "_access_counter",
    )

    def __init__(self, stale_timeout_sec: int = 3600):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._last_access: Dict[str, float] = {}
        self._guard = asyncio.Lock()
        self._stale_timeout = stale_timeout_sec
        self._access_counter = 0

    async def get_lock(self, chat_id: str) -> asyncio.Lock:
        """Get or create a lock for the given chat_id."""
        async with self._guard:
            now = time_monotonic()
            self._access_counter += 1

            # Maintenance every N calls (protected by guard lock)
            if self._access_counter >= LOCK_PRUNE_INTERVAL:
                self._prune_stale(now)
                self._access_counter = 0

            if chat_id not in self._locks:
                self._locks[chat_id] = asyncio.Lock()

            self._last_access[chat_id] = now
            return self._locks[chat_id]

    def _prune_stale(self, now: float) -> None:
        """Remove stale locks."""
        keys_to_remove = [
            k for k, ts in self._last_access.items() if (now - ts) > self._stale_timeout
        ]
        for k in keys_to_remove:
            lock = self._locks.get(k)
            # Safety: Only remove if NOT currently locked
            if lock is not None and not lock.locked():
                self._locks.pop(k, None)
                self._last_access.pop(k, None)


class RedisLockManager(AbstractLockManager):
    """Distributed lock manager using Redis."""

    def __init__(self, redis_url: str):
        if not HAS_REDIS:
            raise ImportError("redis-py is required for RedisLockManager")
        self._redis = redis.from_url(redis_url)
        self._expire = 60  # Lock expiration in seconds

    async def get_lock(self, chat_id: str) -> "RedisLock":
        # Returns a redis lock object which is an async context manager
        return self._redis.lock(f"rpg_lock:{chat_id}", timeout=self._expire)

    async def close(self):
        await self._redis.close()


_GLOBAL_LOCK_MANAGER: Optional[AbstractLockManager] = None
_GLOBAL_LOCK_INIT_GUARD = asyncio.Lock()
_GLOBAL_INIT_LOCK = asyncio.Lock()


# ══════════════════════════════════════════════════════════════════
# ROBUST HELPERS
# ══════════════════════════════════════════════════════════════════


def _safe_truncate(text: str, max_len: int) -> str:
    """
    Safely truncate text to max_len characters.

    Args:
        text: Text to truncate.
        max_len: Maximum character length.

    Returns:
        Truncated text.
    """
    return text[:max_len] if len(text) > max_len else text


def estimate_tokens(text: str) -> int:
    """Roughly estimate tokens (char count * multiplier, faster than division)."""
    return int(len(text or "") * TOKEN_ESTIMATION_MULTIPLIER)


def extract_json_object(text: str) -> Dict[str, Any]:
    """
    Extracts JSON from text using regex and try-parse fallback.

    Args:
        text: Input text potentially containing JSON.

    Returns:
        Parsed JSON as dict, or empty dict on failure.
    """
    if not text or not isinstance(text, str):
        return {}

    # Fast path: try to find a code block first using global compiled regex
    if match := CODE_BLOCK_PATTERN.search(text):
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass  # Fall through to bracket matching

    # Fallback: Bracket matching (optimized with early exit)
    start_idx = text.find("{")
    if start_idx == -1:
        return {}

    end_idx = text.rfind("}")
    if end_idx > start_idx:
        try:
            return json.loads(text[start_idx : end_idx + 1])
        except json.JSONDecodeError:
            pass

    return {}


# ═══════════════════════════════════════════════════════════════════════════════
# IMPORTANCE POST-PROCESSING (keyword-based reclassification)
# ═══════════════════════════════════════════════════════════════════════════════

# Keywords that indicate CRITICAL importance (5-10% of facts)
CRITICAL_KEYWORDS = frozenset(
    {
        "morte",
        "morrer",
        "morrendo",
        "morto",
        "matar",
        "matou",
        "san",
        "sanidade",
        "colapso",
        "insanidade",
        "loucura",
        "apaixonada",
        "apaixonado",
        "amo você",
        "te amo",
        "amor",
        "primeira vez",
        "revelou segredo",
        "confessou",
        "traição",
        "traidor",
        "agente duplo",
        "ritual",
        "selo",
        "koth",
        "marca",
        "formigamento",
        "perdeu",
        "pontos",
        "trauma",
    }
)

# Keywords that indicate HIGH importance (20-30% of facts)
HIGH_KEYWORDS = frozenset(
    {
        "confessou",
        "revelou",
        "admitiu",
        "declarou",
        "lealdade",
        "submissão",
        "entrega",
        "posse",
        "possuída",
        "fantasias",
        "desejo",
        "desejos",
        "quero",
        "preciso",
        "relacionamento",
        "dinâmica",
        "poder",
        "controle",
        "vulnerável",
        "vulnerabilidade",
        "exposta",
        "exposto",
        "segredo",
        "nunca contei",
        "nunca disse",
        "primeira pessoa",
        "medo",
        "terror",
        "pânico",
        "ansiedade",
        "feliz",
        "felicidade",
        "alegria",
        "prazer",
        "chorar",
        "lágrimas",
        "emoção",
        "emocional",
        "piercing",
        "tatuagem",
        "marcação",
        "permanente",
        "biquíni",
        "nu",
        "nua",
        "sexual",
        "sexo",
        "psicóloga",
        "delta green",
        "missão",
        "operação",
    }
)

# Keywords that indicate MEDIUM importance (30-40% of facts)
MEDIUM_KEYWORDS = frozenset(
    {
        "usando",
        "vestindo",
        "roupa",
        "camiseta",
        "apartamento",
        "local",
        "lugar",
        "planos",
        "planejando",
        "pretende",
        "consulta",
        "paciente",
        "trabalho",
        "física",
        "corpo",
        "pele",
        "mão",
        "dedos",
        "praia",
        "viagem",
        "resort",
        "arma",
        "pistola",
        "hk45",
        "honey badger",
    }
)


def classify_importance_postprocess(
    fact_text: str, original_importance: str = "low"
) -> str:
    """
    Reclassify fact importance based on keyword analysis.

    This is a post-processing step to correct LLM's tendency to mark everything as 'low'.

    Args:
        fact_text: The text of the extracted fact
        original_importance: The LLM's original classification

    Returns:
        Corrected importance level: 'critical', 'high', 'medium', or 'low'
    """
    text_lower = fact_text.lower()

    # Check for critical keywords (highest priority)
    for keyword in CRITICAL_KEYWORDS:
        if keyword in text_lower:
            return "critical"

    # Check for high keywords
    for keyword in HIGH_KEYWORDS:
        if keyword in text_lower:
            return "high"

    # Check for medium keywords
    for keyword in MEDIUM_KEYWORDS:
        if keyword in text_lower:
            return "medium"

    # Keep original classification if no keywords match
    return original_importance


async def call_llm_api(
    api_key: str,
    base_url: str,
    model: str,
    system: str,
    user: str,
    timeout: int = 60,
    temperature: float = 0.3,
    session: Optional[aiohttp.ClientSession] = None,
) -> Optional[str]:
    """
    Call LLM API for chat completions.

    Args:
        api_key: API authentication key.
        base_url: Base URL for the API.
        model: Model identifier.
        system: System prompt.
        user: User message.
        timeout: Request timeout in seconds.
        temperature: LLM temperature (0.0-1.0).
        session: Optional reusable aiohttp session.

    Returns:
        Generated content string or None on failure.
    """
    url = f"{base_url.rstrip('/')}/chat/completions"
    headers = _make_auth_headers(api_key)
    payload = {
        "model": model,
        "messages": [
            {"role": ROLE_SYSTEM, "content": system},
            {"role": ROLE_USER, "content": user},
        ],
        "temperature": temperature,
    }

    async def _request(s: aiohttp.ClientSession) -> Optional[str]:
        try:
            async with s.post(url, headers=headers, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    choices = data.get("choices") or EMPTY_DICT_LIST
                    message = choices[0].get("message") or EMPTY_DICT
                    content = message.get("content", "")
                    return content or None
                elif resp.status in RETRYABLE_STATUS_CODES:
                    # Retryable errors
                    raise ConnectionError(f"RETRYABLE_ERROR_{resp.status}")
                else:
                    error_text = await resp.text()
                    logger.error(
                        f"LLM API Terminal Error: {resp.status} - {error_text}"
                    )
                    return None
        except Exception as e:
            if "RETRYABLE_ERROR" in str(e):
                raise e
            logger.error(f"LLM Request Exception: {e}")
            raise ConnectionError("LLM_EXCEPTION") from e

    MAX_RETRIES = 3

    async def _execute_with_retry(s: aiohttp.ClientSession) -> Optional[str]:
        for attempt in range(MAX_RETRIES + 1):
            try:
                result = await _request(s)
                if result is not None:
                    return result
                # If result is None, it was a terminal error
                return None
            except ConnectionError:
                if attempt < MAX_RETRIES:
                    jitter = random.uniform(RETRY_JITTER_MIN, RETRY_JITTER_MAX)
                    delay = (2**attempt) + jitter
                    logger.warning(
                        "LLM retryable failure. Retrying in %.2fs... (Attempt %d/%d)",
                        delay,
                        attempt + 1,
                        MAX_RETRIES,
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"LLM failing after {MAX_RETRIES} retries.")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error in LLM retry loop: {e}")
                return None
        return None

    if session and not session.closed:
        return await _execute_with_retry(session)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as s:
        return await _execute_with_retry(s)


async def embed_with_retry(
    api_key: str,
    base_url: str,
    model: str,
    text: str,
    dimensions: int = 1536,
    max_retries: int = 3,
    timeout: int = 15,
    session: Optional[aiohttp.ClientSession] = None,
) -> Optional[List[float]]:
    """
    Get embeddings with retry logic and exponential backoff with jitter.

    Args:
        api_key: API authentication key.
        base_url: Base URL for the API.
        model: Embedding model identifier.
        text: Text to embed.
        dimensions: Vector dimensions.
        max_retries: Maximum retry attempts.
        timeout: Request timeout in seconds.
        session: Optional reusable aiohttp session.

    Returns:
        Embedding vector or None on failure.
    """
    url = f"{base_url.rstrip('/')}/embeddings"
    headers = _make_auth_headers(api_key)
    safe_text = _safe_truncate(text, MAX_TEXT_FOR_EMBEDDING)
    if not safe_text.strip():
        return None

    payload = {"model": model, "input": safe_text, "dimensions": dimensions}

    async def _request(s: aiohttp.ClientSession) -> Optional[List[float]]:
        async with s.post(url, headers=headers, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                data_list = data.get("data") or EMPTY_DICT_LIST
                return data_list[0].get("embedding")
            elif resp.status == 429:
                raise ConnectionError("RATE_LIMITED")
            else:
                error_text = await resp.text()
                logger.warning(f"Embedding Error {resp.status}: {error_text}")
                return None

    for attempt in range(max_retries):
        try:
            if session and not session.closed:
                return await _request(session)
            else:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as s:
                    return await _request(s)
        except ConnectionError:
            # Rate limited - exponential backoff with jitter
            jitter = random.uniform(RETRY_JITTER_MIN, RETRY_JITTER_MAX)
            await asyncio.sleep((2**attempt) + jitter)
        except Exception as e:
            logger.warning(f"Embedding attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt == max_retries - 1:
                return None
            await asyncio.sleep(1)
    return None


def extract_keywords(text: str, stopwords: FrozenSet[str]) -> List[str]:
    """
    Extract meaningful keywords from text.

    Args:
        text: Source text.
        stopwords: Frozenset of words to exclude.

    Returns:
        List of unique keywords.
    """
    if not text:
        return []
    # Use set difference for cleaner code
    return list(set(KEYWORD_REGEX.findall(text)) - stopwords)


def _get_ngrams(text: str, n: int) -> Set[str]:
    """Extract n-grams from text (helper for similarity)."""
    if not text:
        return set()
    t = text[:MAX_NGRAM_TEXT_LEN].lower()
    length = len(t)
    if length < n:
        return set()
    return {t[i : i + n] for i in range(length - n + 1)}


def semantic_similarity_ngram(text_a: str, text_b: str, n: int = 3) -> float:
    """
    Calculate n-gram based semantic similarity between two texts.

    Args:
        text_a: First text.
        text_b: Second text.
        n: N-gram size.

    Returns:
        Jaccard similarity score between 0.0 and 1.0.
    """
    a, b = _get_ngrams(text_a, n), _get_ngrams(text_b, n)
    if not a or not b:
        return 0.0
    # Early exit: if no intersection, similarity is 0
    intersection = a & b
    if not intersection:
        return 0.0
    return len(intersection) / len(a | b)


def parse_uploaded_files(files: Optional[list]) -> str:
    """
    Parse uploaded files and extract content.

    Args:
        files: List of file dictionaries.

    Returns:
        Formatted string of file contents.
    """
    if not files:
        return ""

    def _process_file(f: Any) -> str:
        if not isinstance(f, dict):
            return ""
        file_obj = f.get("file") or f
        content = (
            file_obj.get("data", EMPTY_DICT).get("content") or f.get("content") or ""
        )
        if not content:
            return ""
        name = file_obj.get("filename") or f.get("name") or "SEM_NOME"
        return (
            f"\n### ARQUIVO: {name.upper()}\n"
            f"{_safe_truncate(content, MAX_FILE_CONTENT_CHARS)}\n"
        )

    return "".join(filter(None, map(_process_file, files)))


def _get_next_turn_number_sync(
    client: "QdrantClient", collection: str, chat_id: str
) -> int:
    """Get the next turn number for a chat (synchronous)."""
    try:
        res, _ = client.scroll(
            collection,
            scroll_filter=QFilter(
                must=[
                    FieldCondition(key="chat_id", match=MatchValue(value=chat_id)),
                    FieldCondition(
                        key="memory_type", match=MatchAny(any=["turn", "fact"])
                    ),
                ]
            ),
            limit=1,
            order_by=OrderBy(key="turn_number", direction=Direction.DESC),
            with_payload=["turn_number"],
            with_vectors=False,
        )
        if res:
            turn = (res[0].payload or {}).get("turn_number", 0)
            return turn + 1
        return 1
    except Exception as e:
        logger.error(f"Error getting next turn: {e}")
        return 1


async def get_next_turn_number_async(
    client: "QdrantClient", collection: str, chat_id: str
) -> int:
    """Get the next turn number for a chat (async wrapper)."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, partial(_get_next_turn_number_sync, client, collection, chat_id)
    )


def _smart_prune_sync(
    client: "QdrantClient", collection: str, chat_id: str, max_memories: int
) -> None:
    """
    Prune old memories to keep within limits.

    Medical Grade:
        - Prioritizes removing old 'turn' memories first.
        - Then prunes 'fact' memories starting with low importance and oldest timestamps.
        - Uses server-side counting and scrolling for memory efficiency.
    """
    try:
        count_filter = QFilter(
            must=[
                FieldCondition(key="chat_id", match=MatchValue(value=chat_id)),
                FieldCondition(key="memory_type", match=MatchAny(any=["turn", "fact"])),
            ]
        )
        total = client.count(collection, count_filter=count_filter).count
        if max_memories == 0 or total <= max_memories:
            return

        to_del_count = total - max_memories
        ids_to_del: List[Union[str, int, uuid.UUID]] = []

        # Get oldest turns first
        turns, _ = client.scroll(
            collection,
            scroll_filter=QFilter(
                must=[
                    FieldCondition(key="chat_id", match=MatchValue(value=chat_id)),
                    FieldCondition(key="memory_type", match=MatchValue(value="turn")),
                ]
            ),
            limit=PRUNE_SCROLL_LIMIT,
            order_by=OrderBy(key="turn_number", direction=Direction.ASC),
            with_payload=False,
            with_vectors=False,
        )
        ids_to_del.extend([p.id for p in turns[:to_del_count]])

        remaining = to_del_count - len(ids_to_del)
        if remaining > 0:
            facts, _ = client.scroll(
                collection,
                scroll_filter=QFilter(
                    must=[
                        FieldCondition(key="chat_id", match=MatchValue(value=chat_id)),
                        FieldCondition(
                            key="memory_type", match=MatchValue(value="fact")
                        ),
                    ]
                ),
                limit=PRUNE_SCROLL_LIMIT,
                with_payload=["importance", "ts"],
                with_vectors=False,
            )
            # Sort by importance (low first) then by timestamp (oldest first)
            sorted_facts = sorted(
                facts,
                key=lambda p: (
                    0 if (p.payload or {}).get("importance") == "low" else 1,
                    (p.payload or {}).get("ts") or 0,
                ),
            )
            ids_to_del.extend([p.id for p in sorted_facts[:remaining]])

        if ids_to_del:
            client.delete(
                collection,
                points_selector=PointIdsList(
                    points=cast(List[Union[str, int, uuid.UUID]], ids_to_del)
                ),
            )
            logger.info(f"Pruned {len(ids_to_del)} memories for chat {chat_id}")
    except Exception as e:
        logger.error(f"Pruning error: {e}")


def _extract_chat_id(metadata: Optional[dict], body: dict) -> str:
    """Extract chat_id from metadata or body with fallback."""
    if metadata:
        if chat_id := metadata.get("chat_id"):
            return str(chat_id)
    if chat_id := body.get("chat_id"):
        return str(chat_id)
    return "default"


def _log_task_exception(task: "asyncio.Future[Any]") -> None:
    """Callback to log exceptions from background tasks."""
    if task.cancelled():
        return
    try:
        exc = task.exception()
        if exc:
            logger.error(f"Background task error: {exc}")
    except asyncio.InvalidStateError:
        pass  # Task not done yet


# ══════════════════════════════════════════════════════════════════
# MAIN FILTER CLASS (V5.2.1)
# ══════════════════════════════════════════════════════════════════


class Filter:
    """
    Main RPG Context Manager filter for Open WebUI.

    Provides automatic context injection (inlet) and memory extraction (outlet)
    for RPG sessions with persistent vector storage.

    Note: __slots__ not used to maintain compatibility with mock-based testing.
    """

    class Valves(BaseModel):
        """Configuration valves for the filter."""

        # API Configuration
        llm_api_key: str = Field(default="", description="API Key for LLM")
        llm_api_base_url: str = Field(
            default="https://api.openai.com/v1", description="Base URL for LLM API"
        )
        embedding_api_key: str = Field(default="", description="API Key for Embeddings")
        embedding_api_base_url: str = Field(
            default="https://api.openai.com/v1",
            description="Base URL for Embeddings API",
        )

        # Qdrant Configuration
        qdrant_host: str = Field(
            default="a64f67f7-532e-4d80-8c77-2f53a291cea0.us-east4-0.gcp.cloud.qdrant.io"
        )
        qdrant_port: int = Field(default=6333)
        qdrant_api_key: str = Field(default="")
        qdrant_collection: str = Field(default="rpg_memory_v4")

        # Redis Configuration (Optional for Distributed Lock)
        redis_url: str = Field(
            default=os.getenv("REDIS_URL", ""),
            description="Redis URL for distributed locking (e.g., 'redis://localhost:6379/0'). If empty, uses memory lock.",
        )

        # Model Configuration
        summarizer_model: str = Field(
            default="gpt-4o-mini", description="Model for summaries"
        )
        extractor_model: str = Field(
            default="gpt-4o-mini", description="Model for fact extraction"
        )
        embedding_model: str = Field(default="text-embedding-3-small")
        embedding_dimensions: int = Field(
            default=1536, ge=1, le=8192, description="Vector dimensions (1-8192)"
        )

        # RAG Configuration
        summary_interval_turns: int = Field(default=5, ge=1)
        rag_max_facts: int = Field(default=12, ge=1)
        rag_score_threshold: float = Field(default=0.42, ge=0.0, le=1.0)
        rag_keyword_boost: float = Field(default=1.5, ge=1.0, le=10.0)

        # Summarization Configuration
        recent_turns_to_protect: int = Field(
            default=6, ge=1, description="Recent turns kept verbatim (not summarized)"
        )
        summary_target_tokens: int = Field(
            default=1200, ge=100, description="Target tokens per summary"
        )
        enable_conversation_truncation: bool = Field(
            default=True, description="Replace old messages with summaries"
        )
        max_backfill_text_chars: int = Field(
            default=25000,
            ge=5000,
            description="Max chars for older_text in backfill/summary",
        )

        # Feature Flags
        enable_fact_extraction: bool = Field(default=True)
        enable_summarization: bool = Field(default=True)
        enable_suffix: bool = Field(default=True)
        narrative_suffix: str = Field(default="")
        narrative_suffix_marker: str = Field(default="# SYSTEM: PRE-PROCESSING")

        # Memory Management (0 = unlimited)
        max_memories_per_chat: int = Field(
            default=600, ge=0, description="Max memories per chat (0=unlimited)"
        )
        max_summaries_in_conv: int = Field(default=12, ge=1)
        api_timeout: int = Field(default=180, ge=5)
        qdrant_timeout: int = Field(
            default=15, ge=5, description="Qdrant client timeout"
        )
        debug_logging: bool = Field(default=False)
        default_language: str = Field(
            default="pt-br", description="Default language tag for memories"
        )

        # Outlet Quality Control
        min_tokens_for_extraction: int = Field(
            default=100,
            ge=10,
            description="Minimum estimated tokens to trigger extraction",
        )
        outlet_status_style: str = Field(
            default="detailed", description="'compact' or 'detailed' status messages"
        )
        llm_temperature: float = Field(
            default=0.1,
            ge=0.0,
            le=1.0,
            description="Temperature for extraction LLM calls",
        )
        turn_embed_max_retries: int = Field(
            default=5, ge=1, description="Max retries for turn embeddings"
        )
        fact_embed_max_retries: int = Field(
            default=3, ge=1, description="Max retries for fact embeddings"
        )

        # Ignored phrases for skipping extraction (checked via substring match)
        ignored_response_phrases: List[str] = Field(
            default_factory=lambda: [
                "error generating",
                "apologize",
                "sorry",
                "cannot continue",
            ],
            description="Phrases that trigger extraction skip",
        )
        stopwords_csv: str = Field(
            default="", description="Comma-separated list of additional stopwords"
        )

        # Backfill Configuration
        backfill_batch_extraction_enabled: bool = Field(
            default=True,
            description="Use batch extraction for backfill (faster) vs single turn (precise)",
        )
        backfill_batch_size: int = Field(
            default=5, ge=1, description="Number of turns to process per backfill chunk"
        )

        # PROMPTS
        batch_fact_extraction_prompt: str = Field(
            default="""# SISTEMA DE EXTRAÇÃO DE INTELIGÊNCIA DELTA GREEN v6

## MISSÃO
Processar múltiplos turnos de conversa e extrair FATOS ATÔMICOS otimizados para recuperação semântica (RAG).

## REGRAS DE OURO (OBRIGATÓRIAS)

### 0. IDENTIDADE E VERDADE (CRÍTICO)
- NUNCA invente nomes de Agentes. Use APENAS nomes que aparecem EXPLICITAMENTE no texto.
- USE APENAS nomes presentes no texto ou no DOSSIER.
- Se o texto usa "Eu" ou "Você", identifique o Agente pelo nome no Dossier ou use apenas "O Agente".
- Para abreviações de nomes, busque o nome completo no Dossier.

### 1. AUTOSSUFICIÊNCIA TOTAL
Cada fato DEVE ser compreensível isoladamente, sem contexto adicional.
- ❌ ERRADO: "Ele pegou a arma do cofre"
- ✅ CERTO: "Agente [NOME] pegou uma Glock 17 (15 balas) do Green Box 12 em Newark em 12 Fev 2026"

### 2. TEMPORALIDADE OBRIGATÓRIA (CRÍTICO)
Extraia a data/hora mais recente do texto (procure em [TIME] nos Dossiers).
- **Todo fato deve ser ancorado no tempo se possível.**
- Se o texto diz "hoje" e o dossier diz "07 Fev 2026", use "07 Fev 2026".

### 3. EXTRATOR DE DOSSIER (OBRIGATÓRIO)
O texto contém blocos ASCII "DELTA GREEN INTERNAL DOSSIER". Você DEVE extrair:
- Estatísticas vitais (HP, WP, SAN, BP)
- BONDS (Vínculos) e seus valores (ex: Sam: 92)
- Equipamentos listados em [EQUIP]
- Estados atuais em [STATE] (Stress, Taint, Insight)

### 4. ZERO PRONOMES
Substitua TODOS os pronomes por nomes completos.
- ❌ ERRADO: "Ela perdeu sanidade ao ver isso"
- ✅ CERTO: "Agente [NOME] perdeu 4 pontos de Sanidade ao testemunhar o cadáver de [NOME]"

### 5. QUANTIFICAÇÃO OBRIGATÓRIA
Inclua números sempre que existirem: HP, SAN, munição, distâncias, tempo, dinheiro.
- ❌ ERRADO: "Agente gastou munição"
- ✅ CERTO: "Agente [NOME] disparou 6 tiros de seu revólver .38, restando 0 balas"

### 6. CAUSALIDADE EXPLÍCITA
Conecte ação → consequência quando relevante.
- ❌ ERRADO: "O NPC morreu"
- ✅ CERTO: "[NOME] morreu após ser atingido por 3 tiros de Agente [NOME] durante confronto no escritório"

## CATEGORIAS (códigos exatos)
| Código | Uso | Prioridade RAG |
|--------|-----|----------------|
| PSYCHE | Sanidade, trauma, motivação, breaks mentais | ALTA |
| INV | Itens: +ganho, -perda, *uso, #modificação. SEMPRE com quantidade | ALTA |
| REL | Relacionamentos, confiança, alianças, traições, disposição NPC | ALTA |
| INTIM | Dinâmicas de poder, vulnerabilidade emocional, desejos, confissões íntimas | ALTA |
| PHYS | Estados físicos, vestimenta, sensações corporais, modificações corporais | MÉDIA |
| WORLD | Locais, pistas, mudanças ambientais, mapas | MÉDIA |
| MECH | HP, dano, condições, resultados de combate/testes | ALTA |
| PLOT | Missões, revelações, objetivos completados/falhados | CRÍTICA |
| TIME | Passagem de tempo explícita, deadlines, horários | MÉDIA |
| DIALOG | Acordos vinculantes, ameaças, promessas, informações-chave | ALTA |
| GOAL | Intenções do jogador, novos objetivos, mudanças de estratégia | ALTA |

## NÍVEIS DE IMPORTÂNCIA
- **critical**: Risco de morte, revelações maiores, mudanças permanentes, itens únicos, confissões transformadoras
- **high**: Recursos significativos, NPCs importantes, dinâmicas de relacionamento, estados emocionais intensos
- **medium**: Itens úteis, NPCs menores, progressão padrão, detalhes físicos relevantes
- **low**: Flavor puramente atmosférico SEM impacto narrativo ou emocional

## EXTRAÇÃO DE ENTIDADES
- Extraia TODAS as entidades nomeadas: pessoas, locais, itens, organizações, conceitos
- Use nomenclatura consistente (mesma entidade = mesmo nome em todos os fatos)
- Adicione descritores para novas entidades: "[NOME] (Handler do FBI)"
- Entidades são CRÍTICAS para busca por palavra-chave no RAG

## O QUE IGNORAR
- Descrições atmosféricas SEM impacto narrativo, emocional ou de caracterização
- Informação já capturada em outro fato (duplicatas)
- Discussões OOC (fora de personagem)
- Ações falhas SEM consequências narrativas

## O QUE SEMPRE CAPTURAR (mesmo se parecer 'flavor')
- Estados físicos dos personagens (roupas, postura, sensações)
- Dinâmicas de poder entre personagens (quem controla, quem obedece)
- Confissões, desejos revelados, vulnerabilidades expostas
- Objetos com significado emocional (presentes, símbolos, itens de ritual)
- Antecipação, tensão emocional, estados de expectativa

## FORMATO DE SAÍDA (JSON estrito, sem markdown)
{
  "turns": {
    "1": {
      "facts": [
        {
          "entities": ["Agente [NOME]", "Glock 17", "Green Box 12", "Newark"],
          "importance": "high"
        }
      ],
      "stats": {"hp": "10", "hp_max": "12", "san": "45", "wp": "8", "bp": "42", "stress": "high", "heat": "2"},
      "location": "Green Box 12, Newark, New Jersey",
      "tension": "mid"
    }
  }
}

## ESCALA DE TENSÃO
- **low**: Seguro, calmo, investigação tranquila
- **mid**: Alerta, tenso, perigo potencial
- **high**: Perigo ativo, combate, perseguição
- **crit**: Risco de vida iminente, horror cósmico, TPK potencial

## ⚠️ LEMBRETES CRÍTICOS (LEIA ANTES DE RESPONDER)

### ANTI-ALUCINAÇÃO (OBRIGATÓRIO)
**NÃO INVENTE NOMES.** Use APENAS nomes que aparecem LITERALMENTE no texto fornecido.
- ❌ PROIBIDO: Inventar sobrenomes (ex: "Sam Webb", "Sam Reeves", "Sam Lawson")
- ❌ PROIBIDO: Usar nomes de outros personagens (ex: "Olivia Chen", "Kira") 
- ✅ CORRETO: Use apenas o nome exato do texto (ex: "Sam", "Valéria")
- Se não souber o nome completo, use apenas o primeiro nome ou "O Agente"

### VARIAÇÃO DE IMPORTÂNCIA (OBRIGATÓRIO)
Distribua os níveis de importância de forma realista. EXEMPLOS CONCRETOS:

**critical** (5-10%):
- "[AGENTE] confessou estar apaixonada pela primeira vez na vida"
- "[AGENTE] descobriu que [NPC] é um agente duplo"
- "[AGENTE] perdeu 15 pontos de SAN ao testemunhar ritual"

**high** (20-30%):
- "[AGENTE] revelou segredo íntimo nunca contado a ninguém"
- "[NPC] declarou lealdade absoluta a [AGENTE]"
- "[AGENTE] tomou decisão que mudará dinâmica do relacionamento"

**medium** (30-40%):
- "[AGENTE] está usando roupa específica durante a cena"
- "[AGENTE] e [NPC] discutiram planos para o fim de semana"
- "Apartamento tem cheiro de lavanda e vela acesa"

**low** (20-30%):
- "[NPC] ronronou no canto do sofá"
- "Era noite quando a cena ocorreu"
- "[AGENTE] ajustou posição no sofá"

⚠️ SE a conversa contém confissões emocionais, revelações íntimas ou mudanças de relacionamento, a maioria dos fatos DEVE ser "high" ou "critical", NÃO "low"."""
        )

        fact_extraction_prompt: str = Field(
            default="""# EXTRATOR DE MEMÓRIA DELTA GREEN v6 - TURNO ÚNICO

## OBJETIVO
Extrair inteligência ATÔMICA e RECUPERÁVEL deste turno para o sistema de memória vetorial (RAG).

## REGRAS FUNDAMENTAIS

### IDENTIDADE (Regra #0)
- NÃO INVENTE NOMES. Use apenas personagens citados no turno ou no Dossier.
- Nomes que não aparecem EXPLICITAMENTE no texto são PROIBIDOS.
- Identifique o Agente pelo nome completo que aparece no Dossier.

### AUTOSSUFICIÊNCIA (Regra #1)
Cada fato deve fazer sentido COMPLETO isoladamente, como se fosse lido anos depois.
- ❌ "Ele encontrou a evidência no local"
- ✅ "Agente [NOME] encontrou uma fita cassete rotulada 'CASO DELTA 1947' no porão da residência de [NOME] em 145 Oak Street, Arkham"

### ZERO PRONOMES (Regra #2)
Substitua TODOS os pronomes (ele, ela, isso, aquilo) por nomes próprios.
- ❌ "Ela atirou nele quando ele avançou"
- ✅ "Agente [NOME] atirou em [NOME] quando [NOME] avançou com a faca"

### QUANTIFICAÇÃO (Regra #3)
SEMPRE inclua números quando existirem: HP, SAN, munição, distâncias, tempo, valores.
- ❌ "Perdeu sanidade"
- ✅ "Agente [NOME] perdeu 1d6 (resultado: 4) pontos de Sanidade, caindo de 52 para 48 SAN (07 Fev 2026)"

### TEMPORALIDADE (Regra #2)
Sempre inclua a data/hora do evento no texto do fato.
- Busque a data nos blocos [TIME] do Dossier.
- Formato: "Fato ocorrido... (DD Mmm AAAA)"

### EXTRATOR DE DOSSIER (OBRIGATÓRIO)
O texto contém blocos ASCII "DELTA GREEN INTERNAL DOSSIER". Você DEVE extrair:
- Estatísticas vitais (HP, WP, SAN, BP)
- BONDS (Vínculos) e seus valores (ex: Sam: 92)
- Equipamentos listados em [EQUIP]
- Estados atuais em [STATE] (Stress, Taint, Insight)

### CAUSALIDADE (Regra #4)
Conecte causa e efeito quando relevante para gameplay futuro.
- ❌ "O NPC ficou hostil"
- ✅ "NPC [NOME] tornou-se hostil aos Agentes após descobrir que eles invadiram sua propriedade sem mandado"

## CATEGORIAS (códigos exatos obrigatórios)
- **PSYCHE**: Perda/ganho de SAN, traumas adquiridos, mudanças de motivação, colapsos mentais
- **INV**: Itens (+ganhos/-perdidos/*usados). SEMPRE com quantidade e localização
- **REL**: Mudanças de confiança, alianças, conflitos, disposição de NPCs
- **INTIM**: Dinâmicas de poder entre personagens, vulnerabilidade emocional, desejos revelados, confissões íntimas
- **PHYS**: Estados físicos, vestimenta, sensações corporais, modificações corporais, posturas
- **WORLD**: Novos locais, estados ambientais, pistas descobertas, atualizações de mapa
- **MECH**: Mudanças de HP, ferimentos, condições, resultados de combate, testes de perícia
- **PLOT**: Atualizações de missão, segredos revelados, objetivos alterados
- **TIME**: Passagem de tempo (horas/dias), deadlines, horários estabelecidos
- **DIALOG**: Promessas feitas, ameaças emitidas, acordos, informações críticas compartilhadas
- **GOAL**: Intenções declaradas pelo jogador, novos objetivos, mudanças de estratégia

## IMPORTÂNCIA
- **critical**: Risco de morte, revelações maiores, consequências permanentes, confissões transformadoras
- **high**: NPCs importantes, dinâmicas de relacionamento, estados emocionais intensos, pistas principais
- **medium**: Itens úteis, progressão padrão, detalhes físicos narrativamente relevantes

## ENTIDADES (crucial para RAG)
- Extraia TODA entidade nomeada: pessoas, locais, itens, organizações, conceitos
- Mantenha nomenclatura consistente através da sessão
- Adicione descritores para novas entidades: "[NOME] (Handler do FBI, Célula M)"

## IGNORAR
- Descrições atmosféricas SEM impacto narrativo, emocional ou de caracterização
- Informação duplicada
- Discussão meta/OOC
- Rolagens falhas sem consequências narrativas

## SEMPRE CAPTURAR (mesmo que pareça 'flavor')
- Estados físicos dos personagens (roupas, objetos no corpo, posturas, sensações)
- Dinâmicas de poder (quem controla, quem obedece, submissão, dominância)
- Confissões, desejos revelados, vulnerabilidades emocionais
- Objetos com significado emocional ou ritual
- Antecipação, tensão entre personagens, estados de expectativa

## SAÍDA (JSON puro, sem wrapper markdown)
{
  "facts": [
    {
      "text": "Agente [NOME] perdeu 3 SAN após testemunhar o cadáver mutilado de [NOME] no escritório",
      "type": "PSYCHE",
      "entities": ["Agente [NOME]", "[NOME]"],
      "importance": "high"
    },
    {
      "text": "Agente [NOME] encontrou uma pasta classificada rotulada 'MAJESTIC EYES ONLY' no cofre de [NOME]",
      "type": "PLOT",
      "entities": ["Agente [NOME]", "Pasta MAJESTIC", "[NOME]"],
      "importance": "critical"
    }
  ],
  "stats": {
    "hp": "10",
    "hp_max": "12",
    "san": "47",
    "wp": "9",
    "bp": "44",
    "stress": "high",
    "heat": "1"
  },
  "location": "FBI Field Office, Boston - Escritório do Diretor",
  "tension": "high"
}

## TENSÃO: low | mid | high | crit

## ⚠️ LEMBRETES CRÍTICOS (LEIA ANTES DE RESPONDER)

### ANTI-ALUCINAÇÃO (OBRIGATÓRIO)
**NÃO INVENTE NOMES.** Use APENAS nomes que aparecem LITERALMENTE no texto fornecido.
- ❌ PROIBIDO: Inventar sobrenomes (ex: "Sam Webb", "Sam Reeves", "Sam Lawson")
- ❌ PROIBIDO: Usar nomes de outros personagens (ex: "Olivia Chen", "Kira") 
- ✅ CORRETO: Use apenas o nome exato do texto (ex: "Sam", "Valéria")

### VARIAÇÃO DE IMPORTÂNCIA (OBRIGATÓRIO)
EXEMPLOS CONCRETOS para classificação:

**critical** (5-10%):
- "[AGENTE] confessou amor pela primeira vez"
- "[AGENTE] perdeu 10+ SAN em evento traumático"

**high** (20-30%):
- "[AGENTE] revelou segredo nunca compartilhado"
- "[NPC] declarou lealdade ou traição"

**medium** (30-40%):
- "[AGENTE] usando roupa/objeto específico"
- "Conversa sobre planos futuros"

**low** (20-30%):
- "Descrição atmosférica sem impacto"
- "Posição ou movimento trivial"

⚠️ Confissões emocionais e mudanças de relacionamento são HIGH ou CRITICAL, nunca LOW."""
        )

        summary_system_prompt: str = Field(
            default="""# ARQUIVISTA TÁTICO - SISTEMA DE SUMARIZAÇÃO v6

## MISSÃO
Comprimir narrativa de RPG em inteligência DENSA e RECUPERÁVEL otimizada para busca semântica (RAG).

## IDENTIDADE E VERDADE (CRÍTICO)
- NUNCA invente nomes de Agentes. Use APENAS nomes que aparecem EXPLICITAMENTE no texto.
- USE APENAS nomes presentes no texto ou no DOSSIER.
- Para abreviações de nomes, busque o nome completo no Dossier.
- JAMAIS use nomes que não aparecem no texto original.

## CABEÇALHO OBRIGATÓRIO (CRÍTICO)
Todo sumário DEVE ter um cabeçalho com DATA e HORA do jogo.
- Molde: `# [TÍTULO] - [DD MMM AAAA]`
- Se houver horário: `# [TÍTULO] - [DD MMM AAAA] ([HH:MM])`
- Se não houver data explícita, estime pelo contexto ou Dossier.

## PRINCÍPIOS FUNDAMENTAIS

### 1. PRESERVAÇÃO DE NOMES (OBRIGATÓRIO)
- TODOS os personagens, locais, itens e organizações DEVEM aparecer com nomes completos
- NUNCA use pronomes no resumo
- ❌ "Ele foi ao local e encontrou o item"
- ✅ "Agente [NOME] foi ao Warehouse 13 em Docks District e encontrou o Tomo de Alhazred"

### 2. PRESERVAÇÃO DE NÚMEROS
- HP, SAN, WP, BP atuais e mudanças
- Quantidades de itens, munição, dinheiro
- Distâncias, durações de tempo, datas
- Resultados de dados quando significativos

### 3. CRONOLOGIA
- Eventos em ordem de ocorrência
- Marcadores temporais quando disponíveis ("às 3h da manhã", "após 2 horas")

### 4. CAUSA-EFEITO
- Vincule ações às suas consequências
- "X fez Y → resultou em Z"

### 5. DENSIDADE MÁXIMA
- Estilo telegráfico: sem artigos (o/a/um/uma) quando possível
- Sem palavras de preenchimento
- Máxima informação por token

## SEÇÕES OBRIGATÓRIAS

### ## EVENTOS (cronológico)
- [QUEM] fez [O QUÊ] em [ONDE] → [RESULTADO]
- Use nomes completos, nunca pronomes
- Inclua resultados mecânicos (dano, SAN perdida, etc.)

### ## COMBATE/CONFLITO (se houver)
- Participantes listados por nome
- Sequência de ações significativas
- Dano dado/recebido com números
- Estados finais de HP/SAN

### ## DESCOBERTAS
- Pistas encontradas com descrição específica
- Segredos revelados
- Informações aprendidas
- Conexões descobertas

### ## INVENTÁRIO
- Itens: +adquirido, -perdido, *usado, #modificado
- SEMPRE com quantidades
- Localização do item se relevante

### ## RELACIONAMENTOS
- NPC [Nome]: [mudança de disposição] (motivo)
- Novos contatos estabelecidos
- Alianças/inimizades formadas

### ## DINÂMICAS ÍNTIMAS (se aplicável)
- Dinâmicas de poder entre personagens (dom/sub, controle, obediência)
- Confissões, desejos revelados, vulnerabilidades expostas
- Estados emocionais intensos (antecipação, medo, desejo, amor)
- Acordos ou promessas de natureza pessoal/íntima

### ## ESTADOS FÍSICOS
- Vestimenta atual dos personagens presentes
- Sensações corporais relevantes (dor, prazer, formigamento, lesões)
- Objetos no corpo (joias, equipamentos, modificações)
- Posturas e posições narrativamente significativas

### ## ESTADO ATUAL
```
📍 Local: [localização exata]
❤️ HP: [atual]/[máx] | 🧠 SAN: [atual] (BP: [valor])
⚡ WP: [atual] | 😰 Stress: [nível]
🚔 Heat: [nível] | ⚠️ Tensão: [low/mid/high/crit]
🎯 Ameaças Ativas: [lista]
```

### ## PENDENTE
- Ameaças não resolvidas
- Objetivos em aberto
- Ações prometidas
- Pistas não investigadas
- Deadlines se houver

## FORMATAÇÃO
- Use **negrito** para nomes importantes (primeira menção)
- Listas com bullet points para facilitar scan
- Mantenha seções mesmo que vazias (escreva "Nenhum" se aplicável)

## OTIMIZAÇÃO RAG
- Inclua sinônimos de termos importantes entre parênteses
- Repita nomes de entidades chave em contextos diferentes
- Use termos de busca que jogadores provavelmente usarão

## ⚠️ LEMBRETE CRÍTICO: ANTI-ALUCINAÇÃO
**NÃO INVENTE NOMES.** Use APENAS nomes que aparecem LITERALMENTE no texto.
- ❌ PROIBIDO: Inventar sobrenomes não presentes no texto
- ❌ PROIBIDO: Usar nomes de personagens de outras histórias
- ✅ CORRETO: Use o nome exato como aparece (ex: "Sam", "Valéria")"""
        )

    def __init__(self):
        self.valves = self.Valves()
        self.icon = ICON_URL
        self._collection_checked = False
        self._stopwords_cache: Optional[FrozenSet[str]] = None
        self._last_stopwords_csv = ""
        self._session: Optional[aiohttp.ClientSession] = None
        self._qdrant_client: Optional["QdrantClient"] = None
        self._qdrant_config_hash: str = ""
        self._zero_vector_cache: Optional[List[float]] = None
        self._background_tasks: Set[asyncio.Task] = set()

    def _create_background_task(self, coro: Awaitable[Any]) -> None:
        """Helper to create safe background tasks."""
        loop = asyncio.get_running_loop()
        task = loop.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        task.add_done_callback(_log_task_exception)

    async def _ensure_lock_manager(self):
        """Ensure the correct lock manager is initialized."""
        global _GLOBAL_LOCK_MANAGER, _GLOBAL_LOCK_INIT_GUARD

        if _GLOBAL_LOCK_MANAGER:
            return

        async with _GLOBAL_LOCK_INIT_GUARD:
            if _GLOBAL_LOCK_MANAGER:
                return

            if self.valves.redis_url and HAS_REDIS:
                try:
                    logger.info(
                        f"Initializing Redis Lock Manager: {self.valves.redis_url}"
                    )
                    _GLOBAL_LOCK_MANAGER = RedisLockManager(self.valves.redis_url)
                except Exception as e:
                    logger.error(
                        f"Failed to init Redis Lock, falling back to Memory: {e}"
                    )
                    _GLOBAL_LOCK_MANAGER = MemoryLockManager()
            else:
                _GLOBAL_LOCK_MANAGER = MemoryLockManager()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with automatic cleanup."""
        await self.close()

    async def close(self) -> None:
        """Cleanup resources - call on shutdown."""
        if self._session and not self._session.closed:
            await self._session.close()

        # Wait for background tasks
        if self._background_tasks:
            logger.info(
                f"Waiting for {len(self._background_tasks)} background tasks..."
            )
            # Cancel pending tasks to ensure quick shutdown if needed
            for task in list(self._background_tasks):
                if not task.done():
                    task.cancel()

            # Wait with timeout
            if self._background_tasks:
                await asyncio.wait(list(self._background_tasks), timeout=5.0)

        # Close global lock manager if it's ours
        global _GLOBAL_LOCK_MANAGER
        if _GLOBAL_LOCK_MANAGER:
            await _GLOBAL_LOCK_MANAGER.close()

    def _get_stopwords(self) -> FrozenSet[str]:
        """Get stopwords set with caching."""
        csv = self.valves.stopwords_csv

        # Fast path: empty CSV
        if not csv or not csv.strip():  # pylint: disable=no-member
            if self._stopwords_cache is None:
                self._stopwords_cache = DEFAULT_STOPWORDS
                self._last_stopwords_csv = ""
            return self._stopwords_cache

        # Normalize CSV for cache stability
        normalized = ",".join(
            sorted({s.strip().lower() for s in csv.split(",") if s.strip()})
        )

        if self._stopwords_cache is not None and normalized == self._last_stopwords_csv:
            return self._stopwords_cache

        self._stopwords_cache = DEFAULT_STOPWORDS | frozenset(normalized.split(","))
        self._last_stopwords_csv = normalized
        return self._stopwords_cache

    def _get_zero_vector(self) -> List[float]:
        """Get cached zero vector for fallback embeddings (returns copy for safety)."""
        if (
            self._zero_vector_cache is None
            or len(self._zero_vector_cache) != self.valves.embedding_dimensions
        ):
            self._zero_vector_cache = [0.0] * self.valves.embedding_dimensions
        return self._zero_vector_cache.copy()

    # Log level method cache for performance
    _LOG_METHODS: Dict[str, Callable[..., None]] = {
        "debug": logger.debug,
        "info": logger.info,
        "warning": logger.warning,
        "error": logger.error,
    }

    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if debug logging is enabled."""
        if not self.valves.debug_logging:
            return
        log_func = self._LOG_METHODS.get(level, logger.info)
        if level == "error":
            log_func(f"📎 RPG Mgr | {msg}", exc_info=True)
        else:
            log_func(f"📎 RPG Mgr | {msg}")

    def _build_summary_prompt(
        self,
        text: str,
        t_start: Optional[int] = None,
        t_end: Optional[int] = None,
        last_summary_text: str = "",
    ) -> str:
        """
        Build context-aware summary user prompt.

        Args:
            text: The conversation text to summarize.
            t_start: Start turn number (optional).
            t_end: End turn number (optional).
            last_summary_text: Previous summary for context continuity.

        Returns:
            Formatted summary prompt string.
        """
        context_prefix = ""
        if last_summary_text:
            truncated_last = _safe_truncate(last_summary_text, SUMMARY_CONTEXT_TRUNCATE)
            context_prefix = f"Contexto Anterior: {truncated_last}\n\n"

        turn_info = ""
        if t_start is not None and t_end is not None:
            turn_info = f" (turnos {t_start}-{t_end},"
        else:
            turn_info = " ("

        return (
            f"{context_prefix}"
            f"Resuma estas mensagens{turn_info} ~{estimate_tokens(text)} tokens) "
            f"em aproximadamente {self.valves.summary_target_tokens} tokens:\n\n{text}"
        )

    async def _emit_status(
        self,
        emitter: Optional[Callable[[dict], Awaitable[None]]],
        description: str,
        done: bool = True,
    ) -> None:
        """
        Emit status message if emitter is provided.

        Args:
            emitter: Event emitter callback (can be None).
            description: Status message to display.
            done: Whether this is a final status.
        """
        if emitter:
            await emitter(
                {"type": "status", "data": {"description": description, "done": done}}
            )

    def _extract_text(self, content: Any) -> str:
        """Extract text content from various message formats."""
        if content is None:
            return ""

        if isinstance(content, str):
            text = content
        elif isinstance(content, list):
            # Use list comprehension with join for better performance
            text = " ".join(
                item["text"]
                for item in content
                if isinstance(item, dict)
                and item.get("type") == "text"
                and "text" in item
            )
        else:
            text = str(content)

        # Remove markers - check both at once to avoid redundant iterations
        suffix = self.valves.narrative_suffix
        marker = self.valves.narrative_suffix_marker

        if suffix and suffix in text:
            text = text.partition(suffix)[0].strip()
        elif marker and marker in text:
            text = text.partition(marker)[0].strip()

        return text

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.valves.api_timeout)
            )
        return self._session

    def _get_qdrant(self) -> "QdrantClient":
        """Get or create Qdrant client with config drift detection."""
        if not HAS_QDRANT:
            raise ImportError("qdrant-client not installed")

        # Config fingerprint
        current_config = (
            f"{self.valves.qdrant_host}|{self.valves.qdrant_port}|"
            f"{self.valves.qdrant_api_key}|{self.valves.qdrant_timeout}"
        )

        # Check for config change
        if self._qdrant_client and current_config != self._qdrant_config_hash:
            logger.info("Qdrant configuration changed. Recreating client.")
            self._qdrant_client = None

        if self._qdrant_client is not None:
            return self._qdrant_client

        self._qdrant_config_hash = current_config

        # Detect if host is a Qdrant Cloud URL (contains 'cloud.qdrant.io')
        host = self.valves.qdrant_host
        is_cloud = "cloud.qdrant.io" in host  # pylint: disable=no-member

        if is_cloud:
            # Qdrant Cloud requires HTTPS URL
            url = (
                f"https://{host}" if not host.startswith("http") else host
            )  # pylint: disable=no-member
            kwargs = {"url": url, "timeout": self.valves.qdrant_timeout}
        else:
            # Local/self-hosted Qdrant uses host:port
            kwargs = {
                "host": host,
                "port": self.valves.qdrant_port,
                "timeout": self.valves.qdrant_timeout,
            }

        if self.valves.qdrant_api_key:
            kwargs["api_key"] = self.valves.qdrant_api_key

        self._qdrant_client = QdrantClient(**kwargs)
        return self._qdrant_client

    async def _ensure_collection_safe(self, client: "QdrantClient") -> None:
        """Ensure collection exists with proper indexes."""
        if self._collection_checked:
            return

        async with _GLOBAL_INIT_LOCK:
            if self._collection_checked:
                return
            try:
                # Checking logic
                exists = False
                try:
                    info = client.get_collection(self.valves.qdrant_collection)
                    # Safety: Check dimensions mismatch
                    vectors_config = info.config.params.vectors
                    if isinstance(vectors_config, VectorParams):
                        current_size = cast(int, vectors_config.size)
                    elif isinstance(vectors_config, dict):
                        # Handle case where it might be a dict
                        current_size = cast(int, vectors_config.get("size", 0))
                    else:
                        current_size = 0  # Fallback or unknown

                    if current_size != self.valves.embedding_dimensions:
                        msg = (
                            f"Dimension mismatch: DB={current_size}, "
                            f"Config={self.valves.embedding_dimensions}. Recreating."
                        )
                        self._log(msg)
                        client.delete_collection(self.valves.qdrant_collection)
                        exists = False
                    else:
                        exists = True
                except Exception as e:
                    # Robust handling of "Not Found" errors across different qdrant-client versions
                    err_str = str(e).lower()
                    status_code = getattr(e, "status_code", None)

                    if status_code == 404 or "404" in err_str or "not found" in err_str:
                        msg = (
                            f"Collection {self.valves.qdrant_collection} "
                            "not found (404). Proceeding to create."
                        )
                        self._log(msg)
                        exists = False
                    else:
                        raise e

                if not exists:
                    client.create_collection(
                        self.valves.qdrant_collection,
                        vectors_config=VectorParams(
                            size=self.valves.embedding_dimensions,
                            distance=Distance.COSINE,
                        ),
                    )
                    # Create indexes (tuple of tuples for immutability)
                    indexes = (
                        ("chat_id", PayloadSchemaType.KEYWORD),
                        ("memory_type", PayloadSchemaType.KEYWORD),
                        ("subtype", PayloadSchemaType.KEYWORD),
                        ("entities", PayloadSchemaType.KEYWORD),
                        ("turn_number", PayloadSchemaType.INTEGER),
                        ("ts", PayloadSchemaType.FLOAT),
                    )
                    for field, schema_type in indexes:
                        client.create_payload_index(
                            self.valves.qdrant_collection, field, schema_type
                        )
                    self._log(f"Collection {self.valves.qdrant_collection} created.")
                self._collection_checked = True
            except Exception as e:
                self._log(f"DB Init Error: {e}", "error")

    def _scroll_last_memory(
        self,
        client: "QdrantClient",
        chat_id: str,
        memory_type: Union[str, List[str]],
        with_payload: Any = True,
    ) -> Optional[Any]:
        """
        Scroll the last memory of a specific type (synchronous helper).

        Used within run_in_executor to avoid blocking the event loop.
        """
        mem_match: Union[MatchAny, MatchValue]
        if isinstance(memory_type, list):
            mem_match = MatchAny(any=memory_type)
        else:
            mem_match = MatchValue(value=memory_type)

        res, _ = client.scroll(
            self.valves.qdrant_collection,
            scroll_filter=QFilter(
                must=[
                    FieldCondition(key="chat_id", match=MatchValue(value=chat_id)),
                    FieldCondition(key="memory_type", match=mem_match),
                ]
            ),
            limit=1,
            order_by=OrderBy(key="turn_number", direction=Direction.DESC),
            with_payload=with_payload,
            with_vectors=False,
        )
        return res[0] if res else None

    async def _retrieve_context_state(
        self, client: "QdrantClient", chat_id: str
    ) -> Optional[Any]:
        """Fetch the last turn state."""
        loop = asyncio.get_running_loop()

        return await loop.run_in_executor(
            None, partial(self._scroll_last_memory, client, chat_id, "turn")
        )

    def _rank_facts(
        self,
        memories: List[Dict[str, Any]],
        current_turn: int,
        current_tension: str,
    ) -> List[Dict[str, Any]]:
        """Rank and sort memories based on adaptive scoring."""
        for m in memories:
            base_score = m.get("score", 0.5)
            keyword_boost = m.get("_boost", 1.0)

            # Subtype boost
            subtype_boost = 1.0
            if m.get("subtype", "") in IMPORTANT_SUBTYPES:
                subtype_boost = IMPORTANT_SUBTYPE_BOOST

            # Importance boost
            importance_boost = (
                IMPORTANCE_HIGH_BOOST if m.get("importance") == "high" else 1.0
            )

            # Recency bonus: newer memories get slight boost
            recency_bonus = 0.0
            mem_turn = m.get("turn_number", 0)
            if current_turn > 0 and mem_turn > 0:
                recency_bonus = min(
                    (mem_turn / current_turn) * RECENCY_BONUS_MAX, RECENCY_BONUS_MAX
                )

            # Adaptive RAG: boost combat-relevant types when tension is high
            tension_boost = 1.0
            if current_tension in HIGH_TENSION_VALUES:
                mem_type = m.get("memory_type", "")
                mem_subtype = m.get("subtype", "")
                if (
                    mem_type in COMBAT_RELEVANT_TYPES
                    or mem_subtype in COMBAT_RELEVANT_SUBTYPES
                ):
                    tension_boost = TENSION_BOOST_VALUE

            # Calculate final score
            m["_f"] = (
                base_score
                * keyword_boost
                * subtype_boost
                * importance_boost
                * tension_boost
            ) + recency_bonus

        memories.sort(key=itemgetter("_f"), reverse=True)
        return memories

    async def _search_by_keywords(
        self,
        client: "QdrantClient",
        chat_id: str,
        keywords: List[str],
        seen_hashes: Set[int],
    ) -> List[Dict[str, Any]]:
        """Perform keyword search and return deduplicated memories."""
        if not keywords:
            return []

        loop = asyncio.get_running_loop()

        # Use partial to avoid closure issues with lambda
        def _scroll_keywords(c, coll, cid, kws, limit):
            return c.scroll(
                coll,
                scroll_filter=QFilter(
                    must=[
                        FieldCondition(key="chat_id", match=MatchValue(value=cid)),
                        FieldCondition(key="entities", match=MatchAny(any=kws)),
                    ]
                ),
                limit=limit,
                with_vectors=False,
            )

        kw_res, _ = await loop.run_in_executor(
            None,
            partial(
                _scroll_keywords,
                client,
                self.valves.qdrant_collection,
                chat_id,
                keywords,
                self.valves.rag_max_facts,
            ),
        )
        self._log(f"DEBUG_RAG: Keywords '{keywords}' found {len(kw_res)} items.")

        results = []
        for r in kw_res:
            m = dict(r.payload or EMPTY_DICT)
            m["score"] = 1.0  # Base score for keyword matches
            m["_boost"] = self.valves.rag_keyword_boost
            raw_content = m.get("content") or m.get("text") or ""
            content_hash = hash(raw_content)
            if content_hash not in seen_hashes:
                seen_hashes.add(content_hash)
                results.append(m)
        return results

    async def _search_by_semantics(
        self,
        client: "QdrantClient",
        chat_id: str,
        query_emb: Optional[List[float]],
        seen_hashes: Set[int],
        existing_memories: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Perform semantic search and return deduplicated memories."""
        if not query_emb:
            return []

        loop = asyncio.get_running_loop()

        # Use partial to avoid closure issues
        def _query_semantic(c, coll, emb, cid, limit, threshold):
            return c.query_points(
                coll,
                query=emb,
                query_filter=QFilter(
                    must=[
                        FieldCondition(key="chat_id", match=MatchValue(value=cid)),
                        FieldCondition(
                            key="memory_type", match=MatchValue(value="fact")
                        ),
                    ]
                ),
                limit=limit,
                score_threshold=threshold,
            ).points

        sem_res = await loop.run_in_executor(
            None,
            partial(
                _query_semantic,
                client,
                self.valves.qdrant_collection,
                query_emb,
                chat_id,
                self.valves.rag_max_facts,
                self.valves.rag_score_threshold,
            ),
        )
        msg = (
            f"DEBUG_RAG: Semantic search (threshold={self.valves.rag_score_threshold}) "
            f"found {len(sem_res)} items."
        )
        self._log(msg)

        results = []
        for r in sem_res:
            rp = r.payload or EMPTY_DICT
            content_text = str(rp.get("content") or rp.get("text", ""))
            content_hash = hash(content_text)

            # Dedupe using hash + ngram similarity as fallback
            is_duplicate = content_hash in seen_hashes
            if not is_duplicate:
                # Check against already found memories (from keywords)
                for existing in existing_memories:
                    existing_text = str(
                        existing.get("content") or existing.get("text", "")
                    )
                    if (
                        semantic_similarity_ngram(content_text, existing_text)
                        > DEDUPE_SIMILARITY_THRESHOLD
                    ):
                        is_duplicate = True
                        break

            if not is_duplicate:
                seen_hashes.add(content_hash)
                m = dict(rp)
                m["score"] = r.score
                results.append(m)
        return results

    async def _retrieve_relevant_facts(
        self,
        client: "QdrantClient",
        chat_id: str,
        query_text: str,
        query_emb: Optional[List[float]],
        current_turn: int = 0,
        current_tension: str = "low",
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Retrieve and format relevant facts (RAG) with Adaptive scoring.

        Returns:
            Tuple of (formatted_facts_text, relevant_memories_list)
        """
        keywords = extract_keywords(query_text, self._get_stopwords())
        seen_hashes: Set[int] = set()

        # 1. Keyword Search
        relevant_memories = await self._search_by_keywords(
            client, chat_id, keywords, seen_hashes
        )

        # 2. Semantic Search
        semantic_memories = await self._search_by_semantics(
            client, chat_id, query_emb, seen_hashes, relevant_memories
        )
        relevant_memories.extend(semantic_memories)

        if not relevant_memories:
            return ("", [])

        # Rank and Format with Adaptive RAG scoring
        relevant_memories = self._rank_facts(
            relevant_memories, current_turn, current_tension
        )

        facts = [
            m.get("content") or m.get("text")
            for m in relevant_memories
            if m.get("memory_type") == "fact"
        ][: self.valves.rag_max_facts]

        if facts:
            formatted = "\n".join(f"• {f}" for f in facts)
            return (
                f"<RECALLED_INTELLIGENCE>\n{formatted}\n</RECALLED_INTELLIGENCE>",
                relevant_memories,
            )
        return ("", relevant_memories)

    async def _retrieve_summaries(
        self, client: "QdrantClient", chat_id: str
    ) -> Tuple[str, int]:
        """
        Fetch recent summaries.

        Returns:
            Tuple of (formatted_summaries_text, highest_summarized_turn)
        """
        loop = asyncio.get_running_loop()

        # Use partial to avoid closure issues with lambda
        def _scroll_summaries(c, coll, cid, limit):
            return c.scroll(
                coll,
                scroll_filter=QFilter(
                    must=[
                        FieldCondition(key="chat_id", match=MatchValue(value=cid)),
                        FieldCondition(
                            key="memory_type", match=MatchValue(value="summary")
                        ),
                    ]
                ),
                limit=limit,
                order_by=OrderBy(key="turn_number", direction=Direction.ASC),
                with_payload=True,
                with_vectors=False,
            )

        all_sums, _ = await loop.run_in_executor(
            None,
            partial(
                _scroll_summaries,
                client,
                self.valves.qdrant_collection,
                chat_id,
                self.valves.max_summaries_in_conv,
            ),
        )
        if all_sums:
            sum_texts = [r.payload.get("text", "") for r in all_sums]
            joined = "\n\n".join(sum_texts)
            # Get highest turn from the last summary (sorted ASC)
            highest_turn = all_sums[-1].payload.get("turn_number", 0)
            return (f"<MISSION_LOGS>\n{joined}\n</MISSION_LOGS>", highest_turn)
        return ("", 0)

    def _format_state_block(self, payload: Dict[str, Any]) -> str:
        """Format the current state block."""
        tension = payload.get("tension", DEFAULT_TENSION)
        t_icon = TENSION_ICONS.get(tension, "⚪")

        # Use single f-string for efficiency
        return (
            f"<CURRENT_STATE>\n"
            f"📍 Local: {payload.get('location', '?')}\n"
            f"🧠 Status: SAN {payload.get('san', '?')} (BP {payload.get('bp', '?')}) | "
            f"WP {payload.get('wp', '?')} | HP {payload.get('hp', '?')}\n"
            f"🔥 Stress: {payload.get('stress', '?')} | 🚔 Heat: {payload.get('heat', '?')}\n"
            f"⚠️ Tensão: {t_icon} {tension.upper()}\n"
            f"</CURRENT_STATE>"
        )

    def _sanitize_conversation(self, messages: List[dict]) -> List[dict]:
        """Remove system role and assistant-errors/ignored phrases."""
        raw_conv = [m for m in messages if m.get("role") != ROLE_SYSTEM]
        ignored = self.valves.ignored_response_phrases
        conversation = []
        for m in raw_conv:
            if m.get("role") == ROLE_ASSISTANT:
                raw_content = m.get("content", "")
                if isinstance(raw_content, list):
                    low_content = " ".join(
                        item.get("text", "")
                        for item in raw_content
                        if isinstance(item, dict) and item.get("type") == "text"
                    ).lower()
                else:
                    low_content = str(raw_content).lower()
                if any(k in low_content for k in ignored):
                    continue
            conversation.append(m)
        return conversation

    async def _retrieve_turn_anchor(
        self,
        client: "QdrantClient",
        chat_id: str,
        content: str,
    ) -> Optional[int]:
        """Fetch the turn number for a specific message content (Anchor)."""

        def _scroll_anchor(c, coll, cid, txt):
            return c.scroll(
                coll,
                scroll_filter=QFilter(
                    must=[
                        FieldCondition(key="chat_id", match=MatchValue(value=cid)),
                        FieldCondition(
                            key="memory_type", match=MatchValue(value="turn")
                        ),
                        FieldCondition(key="text", match=MatchValue(value=txt)),
                    ]
                ),
                limit=1,
                with_payload=True,
            )

        try:
            loop = asyncio.get_running_loop()
            anchor_res, _ = await loop.run_in_executor(
                None,
                partial(
                    _scroll_anchor,
                    client,
                    self.valves.qdrant_collection,
                    chat_id,
                    content,
                ),
            )
            if anchor_res:
                return cast(int, (anchor_res[0].payload or {}).get("turn_number"))
        except Exception as e:
            self._log(f"Anchor check failed: {e}")
        return None

    async def _handle_turn_sync(
        self,
        client: "QdrantClient",
        chat_id: str,
        conversation: List[dict],
        local_turn_count: int,
        loop: asyncio.AbstractEventLoop,
    ) -> Tuple[int, int]:
        """Ensures sync between local state and Qdrant."""
        # Fetch last stored turn
        abs_current_turn = 0  # Initialize to avoid unbound variable
        try:
            last_point = await loop.run_in_executor(
                None,
                partial(
                    self._scroll_last_memory,
                    client,
                    chat_id,
                    ["turn", "fact", "summary"],
                    with_payload=["turn_number"],
                ),
            )
            if last_point:
                abs_current_turn = (last_point.payload or EMPTY_DICT).get(
                    "turn_number", 0
                )
        except Exception as e_scroll:
            self._log(f"Error fetching current turn: {e_scroll}", "error")

        real_turn_found = None
        if abs_current_turn > local_turn_count and conversation:
            user_msgs = [m for m in conversation if m.get("role") == ROLE_USER]
            last_msg_content = (
                user_msgs[-2].get("content", "") if len(user_msgs) > 1 else ""
            )
            if last_msg_content:
                real_turn_found = await self._retrieve_turn_anchor(
                    client, chat_id, last_msg_content
                )
                if real_turn_found:
                    self._log(
                        f"ANCHOR: Desync detected. Re-aligned to Turn {real_turn_found}"
                    )

        if real_turn_found:
            current_turn = real_turn_found + 1
        elif abs_current_turn > 0:
            current_turn = (
                max(abs_current_turn + 1, local_turn_count)
                if local_turn_count > 0
                else abs_current_turn + 1
            )
        else:
            current_turn = local_turn_count

        # Cold start detection: local session has 1 turn but DB has existing memories
        is_cold_start = local_turn_count == 1 and abs_current_turn > 1
        if is_cold_start:
            # Force window to start from 1 to ensure all summaries are considered
            window_start = 1
            self._log(f"COLD START: DB turn={abs_current_turn}, forcing window_start=1")
        else:
            window_start = max(1, current_turn - local_turn_count + 1)

        return current_turn, window_start

    async def _check_backfill_needed(
        self,
        client: "QdrantClient",
        chat_id: str,
        local_turn_count: int,
        window_start_abs_turn: int,
        loop: asyncio.AbstractEventLoop,
    ) -> bool:
        """Determines if backfill should be triggered."""
        try:
            # Cold start optimization: if window_start=1 (forced by cold start detection)
            # and we have 1 local turn, check if summaries exist first
            if local_turn_count == 1 and window_start_abs_turn == 1:
                s_count_res = await loop.run_in_executor(
                    None,
                    partial(
                        client.count,
                        self.valves.qdrant_collection,
                        count_filter=QFilter(
                            must=[
                                FieldCondition(
                                    key="chat_id", match=MatchValue(value=chat_id)
                                ),
                                FieldCondition(
                                    key="memory_type", match=MatchValue(value="summary")
                                ),
                            ]
                        ),
                    ),
                )
                if s_count_res and s_count_res.count > 0:
                    self._log(
                        f"COLD START: {s_count_res.count} summaries exist, skipping backfill"
                    )
                    return False

            # Parallel count for efficiency
            m_task = loop.run_in_executor(
                None,
                partial(
                    client.count,
                    self.valves.qdrant_collection,
                    count_filter=QFilter(
                        must=[
                            FieldCondition(
                                key="chat_id", match=MatchValue(value=chat_id)
                            )
                        ]
                    ),
                ),
            )
            s_task = loop.run_in_executor(
                None,
                partial(
                    client.count,
                    self.valves.qdrant_collection,
                    count_filter=QFilter(
                        must=[
                            FieldCondition(
                                key="chat_id", match=MatchValue(value=chat_id)
                            ),
                            FieldCondition(
                                key="memory_type", match=MatchValue(value="summary")
                            ),
                        ]
                    ),
                ),
            )
            l_task = loop.run_in_executor(
                None,
                partial(
                    self._scroll_last_memory,
                    client,
                    chat_id,
                    "summary",
                    with_payload=["turn_number"],
                ),
            )

            m_res, s_res, last_sum_point = await asyncio.gather(m_task, s_task, l_task)

            mem_count = m_res.count if m_res else 0
            sum_count = s_res.count if s_res else 0
            last_sum_turn = (
                (last_sum_point.payload or EMPTY_DICT).get("turn_number", 0)
                if last_sum_point
                else 0
            )

            is_gap_unreachable = (last_sum_turn + 1) < window_start_abs_turn

            if (
                (mem_count == 0 and local_turn_count > 3)
                or (sum_count == 0 and local_turn_count > 10)
                or is_gap_unreachable
            ):
                self._log(
                    f"Backfill Trigger: mems={mem_count}, "
                    f"sums={sum_count}, gap={is_gap_unreachable}"
                )
                return True
        except Exception as e:
            self._log(f"Backfill check error: {e}", "error")
        return False

    async def _retrieve_inlet_context(
        self,
        client: "QdrantClient",
        chat_id: str,
        session: "aiohttp.ClientSession",
        messages: List[dict],
        query_text: str,
        current_turn: int,
        files: Optional[List[dict]] = None,
    ) -> Tuple[List[str], list, int]:
        """Unified retrieval strategy."""
        search_query = query_text
        if len(messages) > 1 and messages[-2].get("role") == ROLE_ASSISTANT:
            last_assist = self._extract_text(messages[-2].get("content"))
            if last_assist:
                search_query = (
                    f"Contexto RPG:\n{last_assist[:1500]}\nJogador: {query_text}"
                )

        state_res, query_emb = await asyncio.gather(
            self._retrieve_context_state(client, chat_id),
            embed_with_retry(
                self.valves.embedding_api_key,
                self.valves.embedding_api_base_url,
                self.valves.embedding_model,
                search_query,
                self.valves.embedding_dimensions,
                session=session,
            ),
        )

        current_tension = DEFAULT_TENSION
        state_block = ""
        if state_res:
            payload = state_res.payload or EMPTY_DICT
            state_block = self._format_state_block(payload)
            current_tension = payload.get("tension", DEFAULT_TENSION)

        # Parallel retrieval
        rag_task = self._retrieve_relevant_facts(
            client, chat_id, query_text, query_emb, current_turn, current_tension
        )
        sums_task = self._retrieve_summaries(client, chat_id)

        (rag_inj, rel_mems), (sum_inj, highest_sum_turn) = await asyncio.gather(
            rag_task, sums_task
        )

        static_ctx = parse_uploaded_files(files)
        blocks = [b for b in [state_block, sum_inj, static_ctx, rag_inj] if b]
        return blocks, rel_mems, highest_sum_turn

    def _perform_truncation(
        self,
        conversation: List[dict],
        indices: List[int],
        highest_sum: int,
        current: int,
        start: int,
    ) -> List[dict]:
        """Handles context-aware conversation truncation."""
        if (
            not self.valves.enable_conversation_truncation
            or highest_sum <= 0
            or highest_sum >= current
        ):
            return conversation
        local_idx = (highest_sum + 1) - start
        if 0 <= local_idx < len(indices):
            return conversation[indices[local_idx] :]
        keep = min(self.valves.recent_turns_to_protect * 2, len(conversation))
        return conversation[-keep:] if keep > 0 else conversation

    def _format_entity_list(self, memories: List[Dict[str, Any]]) -> str:
        """Extract and format a list of entities from memories for status display."""
        entities = {
            str(e)
            for m in memories
            for e in (m.get("entities") or [])
            if e and isinstance(m.get("entities"), list)
        }
        if not entities:
            return f"🧠 {len(memories)} Fatos"

        ent_list = list(entities)
        ent_str = ", ".join(f'"{e}"' for e in ent_list[:2])
        if (rem := len(entities) - 2) > 0:
            ent_str += f" (+{rem})"
        return f"🧠 {ent_str}"

    async def _emit_inlet_status(
        self, emitter, memories, blocks, is_backfill, summaries_created
    ):
        """Standardized inlet status feedback."""
        if not emitter:
            return
        parts = []
        if memories:
            parts.append(self._format_entity_list(memories))

        if any("<MISSION_LOGS>" in b for b in blocks):
            parts.append("📝 Resumo")

        if is_backfill:
            parts.append(
                f"Backfill: {summaries_created} resumo{'s' if summaries_created != 1 else ''}"
            )

        msg = f"✅ {' | '.join(parts)}" if parts else f"✅ Contexto V{VERSION} Pronto"
        await self._emit_status(emitter, msg, done=True)

    def _extract_last_pair(self, messages: List[dict]) -> Tuple[str, str]:
        """Extracts the last assistant and user message text."""
        asst, usr = "", ""
        for m in reversed(messages):
            role = m.get("role")
            if not asst and role == ROLE_ASSISTANT:
                asst = self._extract_text(m.get("content"))
            elif not usr and role == ROLE_USER:
                usr = self._extract_text(m.get("content"))
            if asst and usr:
                break
        return asst, usr

    async def _should_skip_extraction(
        self, chat_id: str, asst_text: str, emitter
    ) -> bool:
        """Determines if extraction should be skipped (error or too short)."""
        token_count = estimate_tokens(asst_text)

        # 1. Check minimum length first
        if token_count < self.valves.min_tokens_for_extraction:
            self._log(
                f"Skipping extraction: too short ({token_count} < {self.valves.min_tokens_for_extraction})"
            )
            await self._emit_status(
                emitter, "⏭️ Mensagem curta (sem memória)", done=True
            )
            return True

        # 2. Check ignored phrases ONLY if message is relatively short (e.g., error messages)
        # Long RPG responses might contain "sorry" inherently, so we don't want to skip those.
        # Threshold: 250 tokens (approx 700 chars) covers standard refusal messages.
        if token_count < 250:
            asst_lower = asst_text.lower()
            if any(k in asst_lower for k in self.valves.ignored_response_phrases):
                self._log(
                    f"Ignoring error/system response in {chat_id} (tokens={token_count})"
                )
                await self._emit_status(emitter, "⚠️ Erro ignorado", done=True)
                return True

        return False

    def _create_turn_memory(
        self,
        user_text: str,
        asst_text: str,
        stats: Dict[str, Any],
        loc: Optional[str],
        tens: Optional[str],
        entities: List[str],
    ) -> Dict[str, Any]:
        """Create a turn memory payload."""
        p = {
            k: v
            for k, v in {
                "location": loc,
                "tension": tens,
                "entities": entities,
                "hp": stats.get("hp"),
                "hp_max": stats.get("hp_max"),
                "wp": stats.get("wp"),
                "san": stats.get("san"),
                "bp": stats.get("bp"),
                "stress": stats.get("stress"),
                "heat": stats.get("heat"),
            }.items()
            if v is not None
        }
        user_memory = _safe_truncate(user_text, MAX_USER_TEXT_MEMORY)
        asst_memory = _safe_truncate(asst_text, MAX_ASSISTANT_TEXT_MEMORY)
        return {
            "vector_text": f"User: {user_memory}\n\nAssistant: {asst_memory}",
            "type": "turn",
            "max_retries": self.valves.turn_embed_max_retries,
            "p": p,
        }

    def _create_fact_memory(
        self,
        fact: Dict[str, Any],
        stats: Dict[str, Any],
        loc: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        """Create a fact memory payload."""
        txt = fact.get("text")
        if not txt:
            return None

        fe = fact.get("entities")
        clean_e = (
            [str(e) for e in fe if e]
            if isinstance(fe, list)
            else ([str(fe)] if fe else [])
        )
        fp = {
            k: v
            for k, v in {
                "subtype": fact.get("type")
                or fact.get("category")
                or DEFAULT_FACT_CATEGORY,
                "content": txt,
                "entities": clean_e,
                "importance": fact.get("importance", "medium"),
                "location": loc,
                "hp": stats.get("hp"),
                "wp": stats.get("wp"),
                "san": stats.get("san"),
                "bp": stats.get("bp"),
                "stress": stats.get("stress"),
                "heat": stats.get("heat"),
            }.items()
            if v is not None
        }
        return {
            "vector_text": txt,
            "type": "fact",
            "max_retries": self.valves.fact_embed_max_retries,
            "p": fp,
        }

    def _build_memory_payloads(
        self, data: dict, user_text: str, asst_text: str
    ) -> List[dict]:
        """Prepares turn and fact memory payloads."""
        stats = data.get("stats", {})
        loc = data.get("location")
        tens = data.get("tension")
        facts_list = data.get("facts", [])
        mems = []

        if user_text or asst_text:
            ents = {
                str(e)
                for f in facts_list
                for e in (
                    f.get("entities")
                    if isinstance(f.get("entities"), list)
                    else [f.get("entities")] if f.get("entities") else []
                )
                if e
            }
            mems.append(
                self._create_turn_memory(
                    user_text, asst_text, stats, loc, tens, list(ents)
                )
            )

        for f in facts_list:
            if fact_mem := self._create_fact_memory(f, stats, loc):
                mems.append(fact_mem)

        return mems

    def _parse_backfill_facts(self, facts_res: str) -> List[dict]:
        """Parses batch/single extraction LLM response for backfill."""
        parsed = []
        try:
            clean = JSON_FENCE_PATTERN.sub("", facts_res).strip()
            data = extract_json_object(clean)
            turns = data.get("turns", {})
            it = (
                turns.items()
                if isinstance(turns, dict)
                else (
                    enumerate(turns)
                    if isinstance(turns, list)
                    else ([("1", data)] if "facts" in data else [])
                )
            )
            for t_key, t_info in it:
                if not isinstance(t_info, dict):
                    continue
                stats = (
                    {k: str(v) for k, v in t_info.get("stats", {}).items()}
                    if isinstance(t_info.get("stats"), dict)
                    else {}
                )
                for f in t_info.get("facts") or []:
                    if isinstance(f, dict) and (txt := f.get("text")):
                        parsed.append(
                            {
                                "text": txt,
                                "type": f.get("type") or f.get("category", "fact"),
                                "entities": f.get("entities", []),
                                "stats": stats,
                                "location": t_info.get("location", "Unknown"),
                                "tension": t_info.get("tension", "unknown"),
                                "importance": classify_importance_postprocess(
                                    txt, f.get("importance", "low")
                                ),
                                "b_turn": t_key,
                            }
                        )
        except Exception as e:
            self._log(f"Backfill Parse Error: {e}")
        return parsed

    async def _extract_backfill_single(self, session, chunk) -> List[dict]:
        """Processes older_text per-turn if batch is disabled."""
        tasks = []
        sem = asyncio.Semaphore(1)  # Strict serial execution for 12 RPM limit

        async def _bounded_extract(idx: int, text: str):
            async with sem:
                try:
                    res = await call_llm_api(
                        self.valves.llm_api_key,
                        self.valves.llm_api_base_url,
                        self.valves.extractor_model,
                        self.valves.fact_extraction_prompt,
                        text,
                        temperature=self.valves.llm_temperature,
                        session=session,
                    )
                    # Enforce 12 RPM (1 req / 5s). Sleep 6s to be safe globaly.
                    self._log(f"Rate Limiter: Sleeping 6s after Turn (idx {idx})...")
                    await asyncio.sleep(6.0)
                    return res
                except Exception as e:
                    self._log(
                        f"Fact Extraction Failed for Turn (idx {idx}): {e}", "error"
                    )
                    # Even on failure, sleep to respect rate limit if it was a 429
                    await asyncio.sleep(6.0)
                    return None

        for i, m in enumerate(chunk):
            if m.get("role") == ROLE_USER:
                u = self._extract_text(m.get("content"))
                a = (
                    self._extract_text(chunk[i + 1].get("content"))
                    if (i + 1) < len(chunk)
                    and chunk[i + 1].get("role") == ROLE_ASSISTANT
                    else ""
                )
                t = EXTRACTION_USER_TEMPLATE.format(
                    user_message=_safe_truncate(u, MAX_USER_TEXT_EXTRACT),
                    assistant_response=_safe_truncate(a, MAX_ASSISTANT_TEXT_EXTRACT),
                )
                tasks.append((len(tasks) + 1, _bounded_extract(len(tasks) + 1, t)))
        parsed = []
        if tasks:
            # We need to preserve order and turn index
            indices, coros = zip(*tasks)
            results = await asyncio.gather(*coros)
            for idx, txt in zip(indices, results):
                if not txt:
                    continue
                try:
                    d = extract_json_object(JSON_FENCE_PATTERN.sub("", txt).strip())
                    if isinstance(d, dict):
                        for f in d.get("facts", []):
                            parsed.append(
                                {
                                    "text": f.get("text"),
                                    "type": f.get("category") or f.get("type", "fact"),
                                    "entities": f.get("entities", []),
                                    "stats": d.get("stats", {}),
                                    "location": d.get("location", "Unknown"),
                                    "tension": d.get("tension", "unknown"),
                                    "importance": classify_importance_postprocess(
                                        f.get("text", ""), f.get("importance", "low")
                                    ),
                                    "b_turn": idx,
                                }
                            )
                except Exception:
                    pass
        return parsed

    async def _save_backfill_points(
        self,
        client: "QdrantClient",
        chat_id: str,
        t_start: int,
        t_end: int,
        summary_res: str,
        sum_emb: Optional[List[float]],
        parsed_facts: List[Dict[str, Any]],
        embeddings: List[Any],
    ) -> None:
        """Construct and save Qdrant points for backfill."""

        points = []

        if sum_emb:

            points.append(
                PointStruct(
                    id=uuid.uuid4().hex,
                    vector=sum_emb,
                    payload={
                        "chat_id": chat_id,
                        "text": summary_res,
                        "memory_type": "summary",
                        "turn_number": t_end,
                        "turn_range": [t_start, t_end],
                        "ts": time_now(),
                        "is_backfill": True,
                    },
                )
            )

        for i, f in enumerate(parsed_facts):

            if f_emb := (embeddings[i] if i < len(embeddings) else None):

                try:

                    b_off = int(f.get("b_turn", 1)) - 1

                except Exception:

                    b_off = 0

                ent = f.get("entities", [])

                points.append(
                    PointStruct(
                        id=uuid.uuid4().hex,
                        vector=f_emb,
                        payload={
                            "chat_id": chat_id,
                            "text": f["text"],
                            "content": f["text"],
                            "memory_type": "fact",
                            "subtype": f["type"],
                            "entities": (ent if isinstance(ent, list) else [str(ent)]),
                            "turn_number": t_start + b_off,
                            "importance": f.get("importance", "low"),
                            "hp": str(f["stats"].get("hp", "")),
                            "san": str(f["stats"].get("san", "")),
                            "wp": str(f["stats"].get("wp", "")),
                            "bp": str(f["stats"].get("bp", "")),
                            "stress": str(f["stats"].get("stress", "")),
                            "location": f["location"],
                            "tension": f["tension"],
                            "ts": time_now(),
                            "is_backfill": True,
                        },
                    )
                )

        if points:

            loop = asyncio.get_running_loop()

            await loop.run_in_executor(
                None, partial(client.upsert, self.valves.qdrant_collection, points)
            )

            self._log(
                f"Backfill: Saved {len(points)} items for turns {t_start}-{t_end}"
            )

    async def _process_backfill_chunk(
        self,
        session: "aiohttp.ClientSession",
        client: "QdrantClient",
        chat_id: str,
        chunk: List[dict],
        t_start: int,
        t_end: int,
        last_summary_text: str,
        cutoff_turn: int,
        emitter: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> Tuple[bool, str]:
        """Process a single backfill chunk: Summary + Extraction + Embedding."""

        await self._emit_status(
            emitter,
            f"🧠 Backfill: Processando Turnos {t_start}-{t_end}...",
            done=False,
        )

        chunk_text = _safe_truncate(
            "\n".join(
                f"[{(m.get('role') or 'unknown').upper()}]: {self._extract_text(m.get('content'))}"
                for m in chunk
            ),
            self.valves.max_backfill_text_chars,
        )

        # Decide if we should summarize this chunk
        should_summarize = (t_end <= cutoff_turn) and self.valves.enable_summarization
        summary_res = ""
        sum_emb = None

        if should_summarize:
            summary_task = call_llm_api(
                self.valves.llm_api_key,
                self.valves.llm_api_base_url,
                self.valves.summarizer_model,
                self.valves.summary_system_prompt,
                self._build_summary_prompt(
                    chunk_text, t_start, t_end, last_summary_text
                ),
                temperature=self.valves.llm_temperature,
                session=session,
            )
        else:
            # Create a dummy task that returns None immediately
            summary_task = asyncio.create_task(asyncio.sleep(0, result=None))

        if should_summarize:
            summary_res_opt = await summary_task
            summary_res = summary_res_opt or ""
        else:
            await summary_task  # Await the dummy task
            summary_res = ""

        parsed_facts = []
        if self.valves.enable_fact_extraction:
            if self.valves.backfill_batch_extraction_enabled:

                f_res = await call_llm_api(
                    self.valves.llm_api_key,
                    self.valves.llm_api_base_url,
                    self.valves.extractor_model,
                    self.valves.batch_fact_extraction_prompt,
                    f"Turns {t_start}-{t_end}:\n{chunk_text}",
                    temperature=self.valves.llm_temperature,
                    session=session,
                )

                parsed_facts = self._parse_backfill_facts(f_res or "")

            else:

                parsed_facts = await self._extract_backfill_single(session, chunk)

        if should_summarize and not summary_res:
            summary_res = FALLBACK_SUMMARY_TEMPLATE.format(t_start=t_start, t_end=t_end)

        # Embeddings
        emb_tasks = []
        if should_summarize and summary_res:
            emb_tasks.append(
                embed_with_retry(
                    self.valves.embedding_api_key,
                    self.valves.embedding_api_base_url,
                    self.valves.embedding_model,
                    summary_res,
                    self.valves.embedding_dimensions,
                    session=session,
                )
            )

        for f in parsed_facts:

            emb_tasks.append(
                embed_with_retry(
                    self.valves.embedding_api_key,
                    self.valves.embedding_api_base_url,
                    self.valves.embedding_model,
                    f["text"],
                    self.valves.embedding_dimensions,
                    session=session,
                )
            )

        embeddings = await asyncio.gather(*emb_tasks)

        # Extract summary embedding if it was requested
        if should_summarize:
            sum_emb = embeddings[0] or await embed_with_retry(
                self.valves.embedding_api_key,
                self.valves.embedding_api_base_url,
                self.valves.embedding_model,
                f"{FALLBACK_EMBEDDING_TEXT} {uuid.uuid4().hex}",
                self.valves.embedding_dimensions,
                session=session,
            )
            # Remove summary embedding from list to match parsed_facts indexing
            fact_embeddings = embeddings[1:]
        else:
            fact_embeddings = embeddings

        await self._save_backfill_points(
            client,
            chat_id,
            t_start,
            t_end,
            summary_res,
            sum_emb,
            parsed_facts,
            fact_embeddings,
        )

        return bool(sum_emb), summary_res

    async def _process_backfill(
        self,
        session: "aiohttp.ClientSession",
        client: "QdrantClient",
        chat_id: str,
        conversation: List[dict],
        window_start: int,
        local_total: int,
        current_turn: int,
        emitter: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> int:
        """Execute robust backfill: Summarize and Extract Facts in batches.

        IMPORTANT: Facts are extracted from ALL turns (1 to current_turn).
        Summaries are only created for turns up to (current_turn - recent_turns_to_protect).
        """
        summaries_created = 0
        last_summary_text = ""
        batch_size = self.valves.backfill_batch_size

        # Get all user message indices in conversation
        user_indices = [
            i for i, m in enumerate(conversation) if m.get("role") == ROLE_USER
        ]
        conversation_len = len(conversation)
        total_turns = len(user_indices)

        if not user_indices:
            return 0

        summary_cutoff = max(0, current_turn - self.valves.recent_turns_to_protect)

        # Process in batches using 0-based indexing for user_indices
        batch_start_idx = 0  # 0-based index into user_indices
        loops = 0

        while batch_start_idx < total_turns and loops < 30:
            loops += 1
            batch_end_idx = min(batch_start_idx + batch_size - 1, total_turns - 1)

            self._log(
                f"BACKFILL LOOP: batch_start_idx={batch_start_idx}, batch_end_idx={batch_end_idx}, "
                f"total_turns={total_turns}, loops={loops}"
            )

            # Calculate absolute turn numbers (1-based)
            t_start = window_start + batch_start_idx
            t_end = window_start + batch_end_idx

            # Get message slice from user_indices
            u_start = user_indices[batch_start_idx]
            # Include up to the next user message (or end of conversation)
            if batch_end_idx + 1 < len(user_indices):
                u_end = user_indices[batch_end_idx + 1]
            else:
                u_end = conversation_len

            chunk = conversation[u_start:u_end]
            if not chunk:
                break

            success, summary_text = await self._process_backfill_chunk(
                session,
                client,
                chat_id,
                chunk,
                t_start,
                t_end,
                last_summary_text,
                summary_cutoff,
                emitter,
            )

            if success:
                summaries_created += 1
                last_summary_text = summary_text

            # Move to next batch
            batch_start_idx = batch_end_idx + 1

            # Rate limit mitigation: Pause between batches
            await asyncio.sleep(10.0)

        return summaries_created

    async def _handle_summary_gap(
        self,
        client: "QdrantClient",
        chat_id: str,
        t_start: int,
        gap_end: int,
    ) -> bool:
        """Insert a gap marker in the summary history."""
        self._log(f"Gap detected: Inserting gap marker for turns {t_start}-{gap_end}.")
        gap_text = (
            f"Registro de Sistema: Lacuna Temporal Identificada.\n"
            f"Dados perdidos entre Turno {t_start} e Turno {gap_end}.\n"
            f"O sistema retomou a gravação a partir do Turno {gap_end + 1}."
        )

        gap_point = PointStruct(
            id=uuid.uuid4().hex,
            vector=self._get_zero_vector(),
            payload={
                "chat_id": chat_id,
                "text": gap_text,
                "memory_type": "summary",
                "turn_number": gap_end,
                "turn_range": [t_start, gap_end],
                "ts": time_now(),
                "language": self.valves.default_language,
                "is_gap": True,
            },
        )
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, partial(client.upsert, self.valves.qdrant_collection, [gap_point])
            )
            return True
        except Exception as e:
            self._log(f"Failed to insert gap marker: {e}", "error")
            return False

    async def _summarize_range_step(
        self,
        session: "aiohttp.ClientSession",
        client: "QdrantClient",
        chat_id: str,
        to_summarize: List[dict],
        t_start: int,
        t_end: int,
        last_summary_text: str,
        emitter: Optional[Callable[[dict], Awaitable[None]]],
    ) -> Tuple[bool, str]:
        """Perform a single summarization step (LLM + Embedding + Save)."""
        await self._emit_status(
            emitter, f"📝 Consolidando turnos {t_start}-{t_end}...", done=False
        )

        older_text = "\n".join(
            f"[{m.get('role', 'unknown').upper()}]: {self._extract_text(m.get('content'))}"
            for m in to_summarize
        )
        older_text = _safe_truncate(older_text, self.valves.max_backfill_text_chars)

        summary = await call_llm_api(
            self.valves.llm_api_key,
            self.valves.llm_api_base_url,
            self.valves.summarizer_model,
            self.valves.summary_system_prompt,
            self._build_summary_prompt(older_text, t_start, t_end, last_summary_text),
            temperature=self.valves.llm_temperature,
            session=session,
        )

        if not summary:
            summary = FALLBACK_SUMMARY_TEMPLATE.format(t_start=t_start, t_end=t_end)

        summary_emb = await embed_with_retry(
            self.valves.embedding_api_key,
            self.valves.embedding_api_base_url,
            self.valves.embedding_model,
            summary,
            self.valves.embedding_dimensions,
            session=session,
        )

        if not summary_emb:
            summary_emb = await embed_with_retry(
                self.valves.embedding_api_key,
                self.valves.embedding_api_base_url,
                self.valves.embedding_model,
                f"{FALLBACK_EMBEDDING_TEXT} {uuid.uuid4().hex}",
                self.valves.embedding_dimensions,
                session=session,
            )

        if summary_emb:
            summary_point = PointStruct(
                id=uuid.uuid4().hex,
                vector=summary_emb,
                payload={
                    "chat_id": chat_id,
                    "text": summary,
                    "memory_type": "summary",
                    "turn_number": t_end,
                    "turn_range": [t_start, t_end],
                    "ts": time_now(),
                    "language": self.valves.default_language,
                },
            )
            loop = asyncio.get_running_loop()
            await asyncio.shield(
                loop.run_in_executor(
                    None,
                    partial(
                        client.upsert, self.valves.qdrant_collection, [summary_point]
                    ),
                )
            )
            return True, summary
        return False, ""

    async def _process_incremental_summarization(
        self,
        session: "aiohttp.ClientSession",
        client: "QdrantClient",
        chat_id: str,
        conversation: List[dict],
        window_start: int,
        user_msg_indices: List[int],
        current_turn: int,
        emitter: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> int:
        """Process incremental summarization for turns that need it."""
        if current_turn < self.valves.summary_interval_turns:
            return 0

        loop = asyncio.get_running_loop()
        last_summary_turn, last_summary_text = 0, ""

        try:
            last_summary_point = await loop.run_in_executor(
                None, partial(self._scroll_last_memory, client, chat_id, "summary")
            )
            if last_summary_point:
                p = last_summary_point.payload or EMPTY_DICT
                last_summary_turn, last_summary_text = p.get("turn_number", 0), p.get(
                    "text", ""
                )
        except Exception as e:
            self._log(f"Error fetching last summary: {e}", "error")
            return 0

        turns_to_summarize_end = current_turn - self.valves.recent_turns_to_protect
        turns_available = turns_to_summarize_end - last_summary_turn

        if turns_available < self.valves.summary_interval_turns:
            return 0

        summaries_created = 0
        for _ in range(10):  # MAX_LOOPS
            if turns_available < self.valves.summary_interval_turns:
                break

            t_start = last_summary_turn + 1
            t_end = min(
                last_summary_turn + self.valves.summary_interval_turns,
                turns_to_summarize_end,
            )
            if t_end < t_start:
                break

            local_start_idx = t_start - window_start
            if local_start_idx < 0:
                gap_end = window_start - 1
                if await self._handle_summary_gap(client, chat_id, t_start, gap_end):
                    last_summary_turn = gap_end
                    turns_available = turns_to_summarize_end - last_summary_turn
                    continue
                break

            if local_start_idx >= len(user_msg_indices):
                break

            local_end_idx = t_end - window_start
            start_idx = user_msg_indices[local_start_idx]
            end_idx = (
                user_msg_indices[local_end_idx + 1]
                if (local_end_idx + 1) < len(user_msg_indices)
                else len(conversation)
            )

            to_summarize = conversation[start_idx:end_idx]
            if not to_summarize:
                break

            success, summary_text = await self._summarize_range_step(
                session,
                client,
                chat_id,
                to_summarize,
                t_start,
                t_end,
                last_summary_text,
                emitter,
            )

            if success:
                summaries_created += 1
                last_summary_turn, last_summary_text = t_end, summary_text
                turns_available = turns_to_summarize_end - last_summary_turn
            else:
                last_summary_turn = t_end  # Force advance
                turns_available = turns_to_summarize_end - last_summary_turn

        return summaries_created

    async def _process_inlet_logic(
        self,
        chat_id: str,
        messages: List[dict],
        body: dict,
        __metadata__: Optional[dict],
        __event_emitter__: Optional[Callable[[dict], Awaitable[None]]],
    ) -> dict:
        """Core logic for inlet processing inside the lock."""
        loop = asyncio.get_running_loop()
        session = await self._get_session()
        client = self._get_qdrant()
        await self._ensure_collection_safe(client)

        # 1. Sanitize and Tracking
        conversation = self._sanitize_conversation(messages)
        user_msg_indices = [
            i for i, m in enumerate(conversation) if m.get("role") == ROLE_USER
        ]
        local_turn_count = len(user_msg_indices)

        # 2. Sync / Anchor Check
        current_turn, window_start_abs_turn = await self._handle_turn_sync(
            client, chat_id, conversation, local_turn_count, loop
        )

        # 3. Backfill Logic
        is_backfill = False
        summaries_created = 0
        if await self._check_backfill_needed(
            client, chat_id, local_turn_count, window_start_abs_turn, loop
        ):
            is_backfill = True
            await self._emit_status(
                __event_emitter__,
                "🧠 Backfill: Sincronizando memória...",
                done=False,
            )
            summaries_created = await self._process_backfill(
                session,
                client,
                chat_id,
                conversation,
                window_start_abs_turn,
                local_turn_count,
                current_turn,
                __event_emitter__,
            )
        # 4. Incremental Summarization
        elif local_turn_count > self.valves.recent_turns_to_protect:
            await self._process_incremental_summarization(
                session,
                client,
                chat_id,
                conversation,
                window_start_abs_turn,
                user_msg_indices,
                current_turn,
                __event_emitter__,
            )

        # 5. Retrieve Context
        query_text = self._extract_text(messages[-1].get("content"))
        files = (__metadata__ or {}).get("files", []) or body.get("files", [])
        blocks, relevant_memories, highest_sum_turn = (
            await self._retrieve_inlet_context(
                client,
                chat_id,
                session,
                messages,
                query_text,
                current_turn,
                files=files,
            )
        )

        # 6. Truncation and Injection
        truncated = self._perform_truncation(
            conversation,
            user_msg_indices,
            highest_sum_turn,
            current_turn,
            window_start_abs_turn,
        )
        original_sys = [m for m in messages if m.get("role") == ROLE_SYSTEM]
        if blocks:
            final = "<RPG_CONTEXT>\n" + "\n\n".join(blocks) + "\n</RPG_CONTEXT>"
            body["messages"] = (
                [{"role": ROLE_SYSTEM, "content": final}] + original_sys + truncated
            )
        elif truncated != conversation:
            body["messages"] = original_sys + truncated

        # 7. Status Feedback
        await self._emit_inlet_status(
            __event_emitter__,
            relevant_memories,
            blocks,
            is_backfill,
            summaries_created,
        )
        return body

    async def inlet(
        self,
        body: dict,
        __user__: Optional[dict] = None,
        __event_emitter__: Optional[Callable[[dict], Awaitable[None]]] = None,
        __metadata__: Optional[dict] = None,
    ) -> dict:
        """Process incoming messages and inject context with Backfill support."""
        if not self.valves.llm_api_key:
            return body

        chat_id = _extract_chat_id(__metadata__, body)
        messages = body.get("messages", [])
        if not messages:
            return body

        await self._ensure_lock_manager()
        # Lock manager is guaranteed to be initialized by _ensure_lock_manager
        if _GLOBAL_LOCK_MANAGER:
            lock = await _GLOBAL_LOCK_MANAGER.get_lock(chat_id)
            async with lock:
                try:
                    body = await self._process_inlet_logic(
                        chat_id, messages, body, __metadata__, __event_emitter__
                    )
                except Exception as e:
                    self._log(f"Inlet Critical Error: {e}", "error")
                    await self._emit_status(
                        __event_emitter__, f"⚠️ Erro: {str(e)[:100]}...", done=True
                    )
                finally:
                    # Cleanup: Close aiohttp session to prevent "Unclosed client session" error
                    if self._session and not self._session.closed:
                        await self._session.close()
                        self._session = None

        # Suffix handling
        if self.valves.enable_suffix and self.valves.narrative_suffix:
            msgs = body.get("messages", [])
            if msgs and msgs[-1].get("role") == ROLE_USER:
                last = msgs[-1]
                content = last.get("content", "")
                if isinstance(content, list):
                    if not any(
                        self.valves.narrative_suffix_marker in str(item)
                        for item in content
                    ):
                        content.append(
                            {
                                "type": "text",
                                "text": f"\n\n{self.valves.narrative_suffix}",
                            }
                        )
                elif (
                    isinstance(content, str)
                    and self.valves.narrative_suffix_marker not in content
                ):
                    last["content"] = (
                        f"{content.strip()}\n\n{self.valves.narrative_suffix}"
                    )

        return body

    async def _generate_and_save_summary(
        self, client: "QdrantClient", chat_id: str, current_turn: int, turns: List[Any]
    ) -> None:
        """Generate a summary of the provided turns and save it."""
        try:
            # Build conversation text efficiently using list comprehension
            conversation_text = ""
            for t in turns:
                p = t.payload or EMPTY_DICT
                tn = p.get("turn_number", "?")
                txt = p.get("text", "")
                conversation_text += f"Turn {tn}:\n{txt}\n"
            if not conversation_text:
                return

            # Truncate to prevent context window overflow
            conversation_text = _safe_truncate(
                conversation_text, self.valves.max_backfill_text_chars
            )

            # Get last summary for context continuity
            loop = asyncio.get_running_loop()
            last_summary_text = ""
            try:
                # Use helper
                last_sum_point = await loop.run_in_executor(
                    None,
                    partial(
                        self._scroll_last_memory,
                        client,
                        chat_id,
                        "summary",
                        with_payload=["text"],
                    ),
                )
                if last_sum_point:
                    last_summary_text = (last_sum_point.payload or EMPTY_DICT).get(
                        "text", ""
                    )
            except Exception as e:
                logger.error(f"Error retrieving last summary context: {e}")

            # Build context-aware prompt
            summary_user_prompt = self._build_summary_prompt(
                conversation_text, last_summary_text=last_summary_text
            )

            session = await self._get_session()
            summary = await call_llm_api(
                self.valves.llm_api_key,
                self.valves.llm_api_base_url,
                self.valves.summarizer_model,
                self.valves.summary_system_prompt,
                summary_user_prompt,
                session=session,
            )

            # Fallback placeholder if LLM fails
            if not summary:
                self._log(
                    f"WARNING: Summary generation failed for chat {chat_id}. Using placeholder."
                )
                summary = (
                    "Registro de Sistema: Falha no processamento de dados narrativos. "
                    "Dados corrompidos ou ilegíveis."
                )

            if summary:
                embedding = await embed_with_retry(
                    self.valves.embedding_api_key,
                    self.valves.embedding_api_base_url,
                    self.valves.embedding_model,
                    summary,
                    self.valves.embedding_dimensions,
                    session=session,
                )

                # Fallback: Zero Vector with warning
                if not embedding or not isinstance(embedding, list):
                    logger.warning(
                        f"Failed to embed summary for chat {chat_id}, using zero vector"
                    )
                    embedding = self._get_zero_vector()

                # Save Summary
                pt = PointStruct(
                    id=uuid.uuid4().hex,
                    vector=embedding,
                    payload={
                        "chat_id": chat_id,
                        "memory_type": "summary",
                        "turn_number": current_turn,
                        "text": summary,
                        "ts": time_now(),
                    },
                )
                # Reuse loop from earlier in function
                await loop.run_in_executor(
                    None, partial(client.upsert, self.valves.qdrant_collection, [pt])
                )
                self._log(
                    f"Summary generated for chat {chat_id} at turn {current_turn}"
                )
        except Exception as e:
            self._log(f"Summary Generation Error: {e}", "error")

    async def _embed_and_save_memories(
        self,
        session: "aiohttp.ClientSession",
        client: "QdrantClient",
        chat_id: str,
        mems: List[dict],
        t_num: int,
        data: dict,
        emitter: Optional[Callable[[dict], Awaitable[None]]],
    ) -> None:
        """Process embeddings and save memories to Qdrant."""
        await self._emit_status(emitter, "💾 Arquivando memórias...", done=False)
        embs = await asyncio.gather(
            *[
                embed_with_retry(
                    self.valves.embedding_api_key,
                    self.valves.embedding_api_base_url,
                    self.valves.embedding_model,
                    m["vector_text"],
                    self.valves.embedding_dimensions,
                    max_retries=m.get("max_retries", 3),
                    session=session,
                )
                for m in mems
            ],
            return_exceptions=True,
        )
        pts = [
            PointStruct(
                id=uuid.uuid4().hex,
                vector=e,
                payload={
                    "chat_id": chat_id,
                    "text": mems[i]["vector_text"],
                    "memory_type": mems[i]["type"],
                    "turn_number": t_num,
                    "ts": time_now(),
                    "language": self.valves.default_language,
                    **mems[i]["p"],
                },
            )
            for i, e in enumerate(embs)
            if e and isinstance(e, list)
        ]
        if pts:
            loop = asyncio.get_running_loop()
            await asyncio.shield(
                loop.run_in_executor(
                    None, partial(client.upsert, self.valves.qdrant_collection, pts)
                )
            )
            asyncio.ensure_future(
                loop.run_in_executor(
                    None,
                    partial(
                        _smart_prune_sync,
                        client,
                        self.valves.qdrant_collection,
                        chat_id,
                        self.valves.max_memories_per_chat,
                    ),
                )
            ).add_done_callback(_log_task_exception)

        status_msg = f"💾 Memória T{t_num} Salva"
        if self.valves.outlet_status_style == "detailed":
            t_icon = TENSION_ICONS.get(str(data.get("tension") or "low"), "⚪")
            loc = (
                (data.get("location") or "Desconhecido")[:17] + "..."
                if len(data.get("location") or "") > 20
                else (data.get("location") or "Desconhecido")
            )
            status_msg = (
                f"📍 {loc} | {t_icon} Tensão | 💾 +{len(data.get('facts', []))} Fatos"
            )
        await self._emit_status(emitter, status_msg, done=True)

    async def outlet(
        self,
        body: dict,
        __user__: Optional[dict] = None,
        __event_emitter__: Optional[Callable[[dict], Awaitable[None]]] = None,
        __metadata__: Optional[dict] = None,
    ) -> dict:
        """
        Process outgoing messages and extract memories.

        Args:
            body: Response body with messages.
            __user__: User information.
            __event_emitter__: Event emitter callback.
            __metadata__: Request metadata.

        Returns:
            Original body (memories saved as side effect).
        """
        if not self.valves.llm_api_key or not self.valves.enable_fact_extraction:
            return body

        chat_id = _extract_chat_id(__metadata__, body)
        messages = body.get("messages", [])
        if not messages:
            return body

        await self._ensure_lock_manager()
        if _GLOBAL_LOCK_MANAGER:
            lock = await _GLOBAL_LOCK_MANAGER.get_lock(chat_id)

            async with lock:
                try:
                    session, client = await self._get_session(), self._get_qdrant()
                    asst_text, user_text = self._extract_last_pair(messages)
                    if await self._should_skip_extraction(
                        chat_id, asst_text, __event_emitter__
                    ):
                        return body

                    await self._emit_status(
                        __event_emitter__, "🧠 Analisando consequências...", done=False
                    )
                    raw = await call_llm_api(
                        self.valves.llm_api_key,
                        self.valves.llm_api_base_url,
                        self.valves.extractor_model,
                        self.valves.fact_extraction_prompt,
                        EXTRACTION_USER_TEMPLATE.format(
                            user_message=_safe_truncate(
                                user_text, MAX_USER_TEXT_EXTRACT
                            ),
                            assistant_response=_safe_truncate(
                                asst_text, MAX_ASSISTANT_TEXT_EXTRACT
                            ),
                        ),
                        temperature=self.valves.llm_temperature,
                        session=session,
                    )
                    data = extract_json_object(raw or "")

                    t_num = await get_next_turn_number_async(
                        client, self.valves.qdrant_collection, chat_id
                    )
                    mems = self._build_memory_payloads(data, user_text, asst_text)

                    if mems:
                        await self._embed_and_save_memories(
                            session,
                            client,
                            chat_id,
                            mems,
                            t_num,
                            data,
                            __event_emitter__,
                        )
                        if t_num % self.valves.summary_interval_turns == 0:
                            await self._trigger_summarization(client, chat_id, t_num)
                except Exception as e:
                    self._log(f"Outlet Error: {e}")
                    await self._emit_status(
                        __event_emitter__,
                        f"⚠️ Salvar Falhou: {str(e)[:100]}...",
                        done=True,
                    )
        return body

    async def _trigger_summarization(
        self, client: "QdrantClient", chat_id: str, current_turn: int
    ) -> None:
        """
        Trigger summary generation for recent turns.

        Medical Grade:
            - Runs as a background task via asyncio.create_task.
            - Includes exception logging callback.
            - Fetches chronological history for stable context.
        """
        loop = asyncio.get_running_loop()

        # Use partial to avoid closure issues
        def _scroll_turns(c, coll, cid, limit):
            return c.scroll(
                coll,
                scroll_filter=QFilter(
                    must=[
                        FieldCondition(key="chat_id", match=MatchValue(value=cid)),
                        FieldCondition(
                            key="memory_type", match=MatchValue(value="turn")
                        ),
                    ]
                ),
                limit=limit,
                order_by=OrderBy(key="turn_number", direction=Direction.DESC),
                with_payload=True,
                with_vectors=False,
            )

        turns, _ = await loop.run_in_executor(
            None,
            partial(
                _scroll_turns,
                client,
                self.valves.qdrant_collection,
                chat_id,
                self.valves.summary_interval_turns,
            ),
        )
        if turns:
            turns.reverse()  # Chronological order
            # Fire-and-forget task using managed helper
            self._create_background_task(
                self._generate_and_save_summary(client, chat_id, current_turn, turns)
            )
