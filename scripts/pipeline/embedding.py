"""Embedding backends for the ``embed`` stage.

Two implementations behind a single ``Embedder`` protocol:

- ``MandoEmbedder`` — calls the project's OpenAI-compatible endpoint (Mandu)
  ``/v1/embeddings``. Confirmed available with ``BAAI/bge-m3`` (1024 dims).
  Zero local hardware overhead; subject to API availability + quotas.

- ``SentenceTransformerEmbedder`` — local fallback using
  ``sentence-transformers`` with the same model identifier. Lazy-loads
  the model so ``import pipeline.embedding`` is cheap when only the API
  backend is used.

``build_embedder(settings, preference)`` auto-detects: tries Mandu with a
short health probe; falls back to local on failure. ``preference`` can pin
either backend explicitly.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Iterable, Literal, Protocol

import httpx
from openai import OpenAI

from .config import Settings

log = logging.getLogger(__name__)

# Default model — BAAI/bge-m3 is 1024-dim, multilingual, available both via
# Mandu and locally via sentence-transformers. Using the same identifier on
# both sides keeps the vector index stable when you switch backends.
DEFAULT_EMBEDDING_MODEL = "BAAI/bge-m3"
DEFAULT_EMBEDDING_DIMENSION = 1024


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------
class Embedder(Protocol):
    model_name: str
    dimension: int

    def embed_batch(self, texts: list[str]) -> list[list[float]]: ...


# ---------------------------------------------------------------------------
# Mandu (OpenAI-compatible)
# ---------------------------------------------------------------------------
@dataclass
class MandoEmbedderConfig:
    base_url: str
    api_key: str
    model: str = DEFAULT_EMBEDDING_MODEL
    timeout_s: int = 60
    max_chars: int = 8000   # truncate inputs to stay under context window


class MandoEmbedder:
    def __init__(self, config: MandoEmbedderConfig) -> None:
        self.config = config
        self.model_name = config.model
        self.dimension: int = 0  # discovered on first call (or via probe)
        self._client = OpenAI(
            base_url=config.base_url,
            api_key=config.api_key or "EMPTY",
            timeout=httpx.Timeout(float(config.timeout_s), connect=10.0),
        )

    def probe(self) -> int:
        """Run a 1-token embedding to verify endpoint availability + dimension.

        Raises if the API is unreachable or returns no embedding. Returns the
        embedding dimension so callers can validate against the vector index.
        """
        resp = self._client.embeddings.create(model=self.model_name, input="probe")
        emb = resp.data[0].embedding
        self.dimension = len(emb)
        return self.dimension

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        # Truncate aggressively long texts to keep latency manageable.
        truncated = [(t or "")[: self.config.max_chars] for t in texts]
        resp = self._client.embeddings.create(
            model=self.model_name, input=truncated,
        )
        out = [d.embedding for d in resp.data]
        if out and self.dimension == 0:
            self.dimension = len(out[0])
        return out


# ---------------------------------------------------------------------------
# Local sentence-transformers fallback
# ---------------------------------------------------------------------------
class SentenceTransformerEmbedder:
    def __init__(self, model_name: str = DEFAULT_EMBEDDING_MODEL) -> None:
        self.model_name = model_name
        self._model = None  # lazy
        self.dimension: int = 0

    def _load(self) -> None:
        if self._model is not None:
            return
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as e:
            raise RuntimeError(
                "sentence-transformers not installed. Run: "
                "`pip install sentence-transformers`."
            ) from e
        log.info("Loading local embedding model: %s (this may take a minute)", self.model_name)
        self._model = SentenceTransformer(self.model_name)
        self.dimension = self._model.get_sentence_embedding_dimension()

    def probe(self) -> int:
        self._load()
        return self.dimension

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        self._load()
        if not texts:
            return []
        # `convert_to_numpy=True` then `.tolist()` for portability across torch
        # builds. normalize=True is consistent with cosine similarity downstream.
        arr = self._model.encode(
            texts, convert_to_numpy=True, normalize_embeddings=True,
            show_progress_bar=False,
        )
        return arr.tolist()


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------
EmbedderPreference = Literal["auto", "mandu", "local"]


def build_embedder(
    settings: Settings | None = None,
    *,
    preference: EmbedderPreference = "auto",
    model: str | None = None,
) -> Embedder:
    """Pick a working backend for the current environment.

    ``auto`` tries Mandu first (with a quick probe) and falls back to local on
    any error. ``mandu`` and ``local`` force a specific backend (errors out
    immediately if that backend isn't usable).
    """
    chosen_model = model or os.getenv("OPENAI_MODEL_EMBEDDING") or DEFAULT_EMBEDDING_MODEL

    def _build_mandu() -> MandoEmbedder:
        cfg = MandoEmbedderConfig(
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.sobdemanda.mandu.piaui.pro/v1"),
            api_key=os.getenv("OPENAI_API_KEY", ""),
            model=chosen_model,
        )
        emb = MandoEmbedder(cfg)
        emb.probe()  # raises on failure
        log.info("Embedder: Mandu /v1/embeddings (model=%s, dim=%d)", emb.model_name, emb.dimension)
        return emb

    def _build_local() -> SentenceTransformerEmbedder:
        emb = SentenceTransformerEmbedder(chosen_model)
        emb.probe()  # loads the model
        log.info("Embedder: local sentence-transformers (model=%s, dim=%d)", emb.model_name, emb.dimension)
        return emb

    if preference == "mandu":
        return _build_mandu()
    if preference == "local":
        return _build_local()

    # auto: Mandu first, fallback local
    try:
        return _build_mandu()
    except Exception as e:
        log.warning("Mandu embeddings unavailable (%s) — falling back to local model.", e)
        return _build_local()
