"""Tests for ``pipeline.ner_llm``: schema validation, retry, normalization."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipeline.ner_llm import (
    NER_SHORT_LABELS,
    NerLLM,
    NerLLMConfig,
    NerLLMError,
    NerLLMSchemaError,
    diff_metrics,
    normalize_entity_record,
)


def _config():
    return NerLLMConfig(
        base_url="http://test",
        api_key="x",
        model="test-model",
        max_retries=3,
    )


def _mock_response(content: str) -> MagicMock:
    """Build a mock OpenAI ChatCompletion response."""
    msg = MagicMock()
    msg.content = content
    choice = MagicMock()
    choice.message = msg
    resp = MagicMock()
    resp.choices = [choice]
    return resp


# ---------------------------------------------------------------------------
# normalize_entity_record
# ---------------------------------------------------------------------------
def test_normalize_record_from_string():
    rec = normalize_entity_record("Foo Bar")
    assert rec == {"text": "Foo Bar", "canonical": "Foo Bar", "provenance": "gliner"}


def test_normalize_record_from_dict_with_text_only():
    rec = normalize_entity_record({"text": "Foo"})
    assert rec["text"] == "Foo"
    assert rec["canonical"] == "Foo"
    assert rec["provenance"] == "gliner"


def test_normalize_record_with_full_fields():
    rec = normalize_entity_record({
        "text": "TJPI",
        "canonical": "Tribunal de Justiça do Piauí (TJPI)",
        "provenance": "llm",
    })
    assert rec["canonical"].startswith("Tribunal")
    assert rec["provenance"] == "llm"


def test_normalize_record_invalid_provenance_falls_back():
    rec = normalize_entity_record({"text": "X", "provenance": "made-up"})
    assert rec["provenance"] == "gliner"


# ---------------------------------------------------------------------------
# diff_metrics
# ---------------------------------------------------------------------------
def test_diff_metrics_drop_consolidate_relabel():
    """`_flat()` dedupes by (label, text.lower()) so 'Maria Silva' + 'MARIA SILVA'
    count as one. Result: before has 3 unique entries (CONTRATANTE, maria silva, lgpd),
    after has 2 (maria silva still in pessoa, lgpd moved to lei)."""
    before = {
        "pessoa": [{"text": "CONTRATANTE"}, {"text": "Maria Silva"}, {"text": "MARIA SILVA"}],
        "orgao": [{"text": "LGPD"}],
    }
    after = {
        "pessoa": [{"text": "Maria Silva", "canonical": "Maria Silva", "provenance": "llm"}],
        "lei": [{"text": "LGPD", "canonical": "Lei Geral de Proteção de Dados (Lei 13.709/2018)", "provenance": "llm"}],
    }
    metrics = diff_metrics(before, after)
    assert metrics["gliner_entity_count"] == 3
    assert metrics["llm_dropped_count"] >= 1   # CONTRATANTE removed
    assert metrics["llm_relabeled_count"] >= 1  # LGPD orgao→lei


def test_diff_metrics_empty_in_empty_out():
    assert diff_metrics({}, {}) == {
        "gliner_entity_count": 0,
        "llm_dropped_count": 0,
        "llm_added_count": 0,
        "llm_kept_count": 0,
        "llm_relabeled_count": 0,
    }


# ---------------------------------------------------------------------------
# NerLLM._chat_json — happy path
# ---------------------------------------------------------------------------
def test_chat_json_returns_parsed_dict():
    valid = json.dumps({"entities": {"pessoa": [{"text": "Joana"}]}})
    llm = NerLLM(_config())
    with patch.object(llm._client.chat.completions, "create", return_value=_mock_response(valid)):
        out = llm.extract("Joana é gerente.")
    assert "entities" in out
    assert out["entities"]["pessoa"][0]["text"] == "Joana"


# ---------------------------------------------------------------------------
# NerLLM._chat_json — retries on invalid JSON
# ---------------------------------------------------------------------------
def test_chat_json_retries_on_invalid_json_then_succeeds():
    llm = NerLLM(_config())
    bad = _mock_response("not-json{{")
    good = _mock_response(json.dumps({"entities": {"orgao": [{"text": "SEAD-PI"}]}}))
    with patch.object(llm._client.chat.completions, "create", side_effect=[bad, good]):
        out = llm.extract("texto")
    assert out["entities"]["orgao"][0]["text"] == "SEAD-PI"


def test_chat_json_raises_after_max_retries_when_all_invalid():
    llm = NerLLM(_config())
    bad = _mock_response("nope")
    with patch.object(llm._client.chat.completions, "create", return_value=bad):
        with pytest.raises(NerLLMError):
            llm.extract("texto")


# ---------------------------------------------------------------------------
# NerLLM._chat_json — schema validation (missing 'entities')
# ---------------------------------------------------------------------------
def test_chat_json_rejects_payload_without_entities_key():
    llm = NerLLM(_config())
    no_entities = _mock_response(json.dumps({"foo": "bar"}))
    good = _mock_response(json.dumps({"entities": {}}))
    with patch.object(llm._client.chat.completions, "create", side_effect=[no_entities, good]):
        out = llm.extract("texto")
    assert out == {"entities": {}}


# ---------------------------------------------------------------------------
# Default labels & extract / consolidate behavior
# ---------------------------------------------------------------------------
def test_default_labels_constant_is_a_tuple_of_strings():
    assert isinstance(NER_SHORT_LABELS, tuple) and len(NER_SHORT_LABELS) > 10
    assert all(isinstance(l, str) and l.islower() for l in NER_SHORT_LABELS)


def test_consolidate_passes_gliner_output_in_prompt():
    """Make sure consolidate() includes the gliner JSON in user message."""
    llm = NerLLM(_config())
    captured: dict = {}

    def _capture(**kw):
        captured.update(kw)
        return _mock_response(json.dumps({"entities": {}}))

    with patch.object(llm._client.chat.completions, "create", side_effect=_capture):
        llm.consolidate("doc text", {"entities": {"pessoa": [{"text": "X"}]}})

    msgs = captured["messages"]
    user_content = next(m["content"] for m in msgs if m["role"] == "user")
    assert "pessoa" in user_content
    assert "doc text" in user_content
