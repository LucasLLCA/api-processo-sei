"""Tests for ``pipeline.embedding``: Embedder protocol + auto-detect fallback."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipeline.embedding import (
    DEFAULT_EMBEDDING_DIMENSION,
    DEFAULT_EMBEDDING_MODEL,
    MandoEmbedder,
    MandoEmbedderConfig,
    SentenceTransformerEmbedder,
    build_embedder,
)


def _mando_response(vectors: list[list[float]]) -> MagicMock:
    resp = MagicMock()
    resp.data = [MagicMock(embedding=v) for v in vectors]
    return resp


# ---------------------------------------------------------------------------
# MandoEmbedder
# ---------------------------------------------------------------------------
def test_mando_probe_sets_dimension():
    cfg = MandoEmbedderConfig(base_url="http://x", api_key="k", model="bge-m3")
    e = MandoEmbedder(cfg)
    fake = _mando_response([[0.1] * 1024])
    with patch.object(e._client.embeddings, "create", return_value=fake):
        dim = e.probe()
    assert dim == 1024
    assert e.dimension == 1024


def test_mando_embed_batch_truncates_long_inputs():
    cfg = MandoEmbedderConfig(base_url="http://x", api_key="k", max_chars=50)
    e = MandoEmbedder(cfg)
    fake = _mando_response([[0.0] * 4, [0.0] * 4])

    captured: dict = {}

    def _capture(**kw):
        captured.update(kw)
        return fake

    with patch.object(e._client.embeddings, "create", side_effect=_capture):
        e.embed_batch(["x" * 200, "short"])

    sent_inputs = captured["input"]
    assert len(sent_inputs[0]) == 50
    assert sent_inputs[1] == "short"


def test_mando_embed_batch_empty_returns_empty():
    cfg = MandoEmbedderConfig(base_url="http://x", api_key="k")
    e = MandoEmbedder(cfg)
    assert e.embed_batch([]) == []


# ---------------------------------------------------------------------------
# SentenceTransformerEmbedder (lazy, no real model loaded)
# ---------------------------------------------------------------------------
def test_local_embedder_lazy_loads_only_on_use(monkeypatch):
    e = SentenceTransformerEmbedder("BAAI/bge-m3")
    assert e._model is None  # not loaded at construction
    # Stub the load so tests don't pull a multi-GB model
    fake_model = MagicMock()
    fake_model.get_sentence_embedding_dimension.return_value = 1024
    fake_model.encode.return_value = MagicMock(tolist=lambda: [[0.0] * 1024])
    monkeypatch.setattr(e, "_load", lambda: setattr(e, "_model", fake_model) or setattr(e, "dimension", 1024))
    e.probe()
    assert e.dimension == 1024


# ---------------------------------------------------------------------------
# build_embedder auto-fallback
# ---------------------------------------------------------------------------
def test_build_embedder_auto_uses_mandu_when_probe_works(monkeypatch):
    monkeypatch.setenv("OPENAI_BASE_URL", "http://x")
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    monkeypatch.setenv("OPENAI_MODEL_EMBEDDING", "bge-m3")
    fake = _mando_response([[0.5] * 1024])
    with patch("pipeline.embedding.OpenAI") as ctor:
        client = MagicMock()
        ctor.return_value = client
        client.embeddings.create.return_value = fake
        e = build_embedder(preference="auto")
    assert isinstance(e, MandoEmbedder)
    assert e.dimension == 1024


def test_build_embedder_auto_falls_back_to_local_on_mandu_error(monkeypatch):
    monkeypatch.setenv("OPENAI_BASE_URL", "http://x")
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    with patch("pipeline.embedding.OpenAI") as ctor:
        client = MagicMock()
        ctor.return_value = client
        client.embeddings.create.side_effect = RuntimeError("503")
        # Stub the local loader
        with patch.object(SentenceTransformerEmbedder, "_load", autospec=True) as load:
            def _stub(self):
                self._model = MagicMock(get_sentence_embedding_dimension=lambda: 1024)
                self.dimension = 1024
            load.side_effect = _stub
            e = build_embedder(preference="auto")
    assert isinstance(e, SentenceTransformerEmbedder)


def test_build_embedder_force_local_skips_mandu(monkeypatch):
    monkeypatch.setenv("OPENAI_BASE_URL", "http://x")
    monkeypatch.setenv("OPENAI_API_KEY", "k")
    with patch.object(SentenceTransformerEmbedder, "_load", autospec=True) as load:
        def _stub(self):
            self._model = MagicMock(get_sentence_embedding_dimension=lambda: 1024)
            self.dimension = 1024
        load.side_effect = _stub
        e = build_embedder(preference="local")
    assert isinstance(e, SentenceTransformerEmbedder)


def test_default_constants():
    assert DEFAULT_EMBEDDING_MODEL == "BAAI/bge-m3"
    assert DEFAULT_EMBEDDING_DIMENSION == 1024
