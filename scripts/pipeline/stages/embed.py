"""Stage ``embed`` — compute embeddings of parsed documents and persist them
on ``:Documento`` nodes.

Reads ``parsed_documents/*.txt``, computes a deterministic ``sha256(text +
modelo)`` hash, queries Neo4j for the existing hash on each Documento and
SKIPS docs whose hash already matches (idempotency). Embeddings are
computed in batches via ``pipeline.embedding.build_embedder`` (Mandu
primary, local sentence-transformers fallback).

After this stage runs, the ``documento_embedding_idx`` vector index (set
up in ``cypher.SETUP_CONSTRAINTS``) is populated and the ``similarity``
stage can resolve top-K neighbors via ``db.index.vector.queryNodes``.

Modes: ``neo4j``, ``json-emit``. Hard-depends on ``parse`` (for the .txt
files) and on the ``Documento`` nodes existing in the graph (which come
from ``atividades``+``ner-load`` paths — soft dep).
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

from ..cypher import CHECK_EMBEDDING_HASH_CYPHER, LOAD_DOCUMENTO_EMBEDDING_CYPHER
from ..embedding import build_embedder
from ..registry import stage
from ..state import now_iso
from .._stage_base import RunContext, StageMeta

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _hash_for(text: str, model: str) -> str:
    """Stable hash so we don't re-embed unchanged docs after model rerun."""
    h = hashlib.sha256()
    h.update(model.encode("utf-8"))
    h.update(b"\x00")
    h.update(text.encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _iter_parsed_docs(parsed_dir: Path):
    """Yield (numero, text) tuples from ``parsed_documents/*.txt``."""
    for txt_path in sorted(parsed_dir.glob("*.txt")):
        numero = txt_path.stem
        try:
            text = txt_path.read_text(encoding="utf-8", errors="replace").strip()
        except OSError as e:
            log.warning("could not read %s: %s", txt_path, e)
            continue
        if not text:
            continue
        yield numero, text


# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------
@stage(StageMeta(
    name="embed",
    description="Computa embeddings dos textos parseados e persiste em :Documento (Neo4j vector index).",
    type="enrich",
    depends_on=("parse",),
    soft_depends_on=("ner-load",),
    modes=("neo4j", "json-emit"),
    estimated_duration="~5-15min para ~3k documentos via Mandu API",
))
def run(ctx: RunContext) -> None:
    parsed_dir = Path(ctx.flags.get("input") or ctx.flags.get("parsed_dir") or "./parsed_documents")
    if not parsed_dir.is_dir():
        log.error("parsed-documents dir not found: %s", parsed_dir)
        ctx.cache["embed_summary"] = {"total": 0, "embedded": 0, "skipped": 0, "error": "missing_dir"}
        return

    limit = int(ctx.flags.get("limit") or 0)
    batch_size = int(ctx.flags.get("batch_size") or 32)
    preference = (ctx.flags.get("embedder") or "auto").lower()

    embedder = build_embedder(ctx.settings, preference=preference)
    model = embedder.model_name
    dim = embedder.dimension
    log.info("embed: using %s (dim=%d), batch_size=%d", model, dim, batch_size)

    docs = list(_iter_parsed_docs(parsed_dir))
    if limit:
        docs = docs[:limit]
    log.info("embed: %d parsed documents to consider in %s", len(docs), parsed_dir)
    if not docs:
        ctx.cache["embed_summary"] = {"total": 0, "embedded": 0, "skipped": 0}
        return

    # ── Lookup existing hashes in batches ──
    existing_hashes: dict[str, str] = {}
    existing_models: dict[str, str] = {}
    if ctx.driver is not None:
        with ctx.driver.session() as session:
            for i in range(0, len(docs), 500):
                batch_ids = [n for n, _ in docs[i:i + 500]]
                result = session.run(CHECK_EMBEDDING_HASH_CYPHER, numeros=batch_ids)
                for rec in result:
                    n = rec["numero"]
                    existing_hashes[n] = rec.get("hash") or ""
                    existing_models[n] = rec.get("modelo") or ""
    else:
        log.info("embed: no Neo4j driver available — assuming all docs are new (json-emit mode).")

    # ── Identify pending docs (hash mismatch OR model mismatch OR missing) ──
    pending: list[tuple[str, str, str]] = []  # (numero, text, hash)
    skipped = 0
    missing_in_graph = 0
    for numero, text in docs:
        if ctx.driver is not None and numero not in existing_hashes:
            # Documento node does not exist yet (NER not loaded). Skip — embed
            # would fail to MATCH the node anyway. Recorded for telemetry.
            missing_in_graph += 1
            continue
        h = _hash_for(text, model)
        if existing_hashes.get(numero) == h and existing_models.get(numero) == model:
            skipped += 1
            continue
        pending.append((numero, text, h))

    log.info(
        "embed: %d pending, %d skipped (hash match), %d missing :Documento node",
        len(pending), skipped, missing_in_graph,
    )

    # ── Compute + persist in batches ──
    writer = ctx.require_writer()
    writer.open_phase("embed")
    embedded = 0
    failed = 0
    try:
        for i in range(0, len(pending), batch_size):
            batch = pending[i:i + batch_size]
            texts = [t for _, t, _ in batch]
            try:
                vectors = embedder.embed_batch(texts)
            except Exception as e:
                log.exception("embed batch %d-%d failed: %s", i, i + len(batch), e)
                failed += len(batch)
                continue

            # Defensive: ensure all vectors have the same dim as the model
            for v in vectors:
                if len(v) != embedder.dimension:
                    raise RuntimeError(
                        f"Embedder returned dim={len(v)} but expected {embedder.dimension}. "
                        f"Refusing to write inconsistent vectors. Check model + index config."
                    )

            rows = []
            ts = now_iso()
            for (numero, _text, h), v in zip(batch, vectors):
                rows.append({
                    "documento_numero": numero,
                    "embedding": v,
                    "modelo": model,
                    "hash": h,
                    "dim": embedder.dimension,
                    "computed_at": ts,
                })
            writer.execute_template(
                "load_documento_embedding",
                LOAD_DOCUMENTO_EMBEDDING_CYPHER,
                {"rows": rows},
                phase="embed",
            )
            embedded += len(rows)

            if (i + batch_size) % (batch_size * 10) == 0 or (i + len(batch)) >= len(pending):
                log.info("embed: %d/%d embedded", embedded, len(pending))
    finally:
        writer.close_phase("embed")

    summary = {
        "total": len(docs),
        "embedded": embedded,
        "skipped": skipped,
        "missing_in_graph": missing_in_graph,
        "failed": failed,
        "model": model,
        "dim": embedder.dimension,
    }
    ctx.cache["embed_summary"] = summary
    log.info(
        "embed complete: embedded=%d skipped=%d missing=%d failed=%d (model=%s, dim=%d)",
        embedded, skipped, missing_in_graph, failed, model, embedder.dimension,
    )
