"""
Parse downloaded SEI documents into plain text using LLM/vision models.

Pipeline:
  1. This script parses files into text (this step)
  2. extract_ner_gliner2.py extracts entities from the parsed text (next step)

Routing by file extension:
  - .html, .htm       → strip HTML tags (no LLM needed)
  - .txt               → passthrough (no LLM needed)
  - .pdf               → convert pages to images → vision model describes content
  - .jpg, .jpeg, .png, .gif, .bmp, .webp → vision model describes content
  - .xlsx, .xls, .csv  → parse with openpyxl/pandas → LLM summarises structured data
  - .doc, .docx        → extract text with python-docx → passthrough

Uses the same Qwen API endpoint as the main API (OPENAI_BASE_URL from .env).

Output structure:
  <output_dir>/<documento_numero>.txt    (parsed plain text)
  <output_dir>/<documento_numero>.json   (metadata: source, method, stats)

Usage:
    python scripts/pipeline/stages/parse_documents.py --input ./documentos_sead
    python scripts/pipeline/stages/parse_documents.py --input ./documentos_sead --output ./parsed_documents
    python scripts/pipeline/stages/parse_documents.py --input ./documentos_sead --processo "00002.000175_2025-63"
    python scripts/pipeline/stages/parse_documents.py --input ./documentos_sead --limit 10 --workers 3
    python scripts/pipeline/stages/parse_documents.py --input ./documentos_sead --skip-existing
    python scripts/pipeline/stages/parse_documents.py --input ./documentos_sead --max-pdf-pages 10

Dependencies:
    pip install openai httpx pdf2image Pillow openpyxl pandas python-docx python-dotenv
"""

import argparse
import base64
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

_HERE = Path(__file__).resolve()
_SCRIPTS = next(p for p in _HERE.parents if p.name == "scripts")
for _p in (_SCRIPTS, _SCRIPTS.parent):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from pipeline.cli import add_standard_args, resolve_settings
from pipeline.config import Settings
from pipeline.logging_setup import configure_logging

# Loads .env from project root (Settings.from_env handles dotenv discovery).
# OPENAI_* vars below are read via os.getenv since they are not yet modeled
# in `Settings`; this keeps the ad-hoc config without re-running load_dotenv.
Settings.from_env()
log = configure_logging(__name__)

# ── Config from .env ───────────────────────────────────────────────────────

OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.sobdemanda.mandu.piaui.pro/v1")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
MODEL_VISAO = os.getenv("OPENAI_MODEL_VISAO", "Qwen/Qwen3-Omni-30B-A3B-Instruct")
MODEL_TEXTO = os.getenv("OPENAI_MODEL_TEXTO", "soberano-alpha-local")
OPENAI_TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "120"))

# Ensure base_url ends with /v1
if not OPENAI_BASE_URL.rstrip("/").endswith("/v1"):
    OPENAI_BASE_URL = OPENAI_BASE_URL.rstrip("/") + "/v1"

# ── Extension groups ───────────────────────────────────────────────────────

TEXT_EXTENSIONS = {".html", ".htm", ".txt"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}
PDF_EXTENSIONS = {".pdf"}
SPREADSHEET_EXTENSIONS = {".xlsx", ".xls", ".csv"}
DOCX_EXTENSIONS = {".doc", ".docx"}

ALL_SUPPORTED = TEXT_EXTENSIONS | IMAGE_EXTENSIONS | PDF_EXTENSIONS | SPREADSHEET_EXTENSIONS | DOCX_EXTENSIONS

# ── Prompts (pt-BR) ───────────────────────────────────────────────────────

VISION_SYSTEM_PROMPT = (
    "Você é um assistente de análise documental do governo do Piauí. "
    "Transcreva e descreva o conteúdo do documento apresentado de forma fiel e completa. "
    "Inclua todo texto visível, tabelas, valores, datas, nomes e assinaturas. "
    "Se houver imagens, gráficos ou selos, descreva-os brevemente. "
    "Responda apenas com o conteúdo extraído, sem comentários adicionais."
)

VISION_USER_PROMPT_PDF = "Transcreva o conteúdo completo das páginas do documento PDF abaixo:"
VISION_USER_PROMPT_IMAGE = "Transcreva e descreva o conteúdo completo desta imagem de documento:"

SPREADSHEET_SYSTEM_PROMPT = (
    "Você é um assistente de análise documental do governo do Piauí. "
    "Analise os dados da planilha abaixo e produza um resumo textual completo. "
    "Inclua: nome das colunas, quantidade de linhas, valores totais se houver, "
    "principais categorias, datas encontradas, nomes de pessoas/órgãos, e quaisquer valores monetários. "
    "Responda em texto corrido, sem formatação de tabela."
)


# ── LLM client ─────────────────────────────────────────────────────────────

def _get_client():
    """Create a synchronous OpenAI client (scripts run sync)."""
    from openai import OpenAI
    import httpx as _httpx
    return OpenAI(
        base_url=OPENAI_BASE_URL,
        api_key=OPENAI_API_KEY,
        timeout=_httpx.Timeout(float(OPENAI_TIMEOUT), connect=10.0),
    )


# ── Parsers ─────────────────────────────────────────────────────────────────

def parse_html(filepath: Path) -> tuple[str, str]:
    """Strip HTML tags, styles, scripts, SEI footer → clean plain text. No LLM needed."""
    import html as html_mod
    raw = filepath.read_text(encoding="utf-8", errors="replace")
    # Remove <style> and <script> blocks entirely
    raw = re.sub(r"<style[^>]*>.*?</style>", " ", raw, flags=re.DOTALL | re.IGNORECASE)
    raw = re.sub(r"<script[^>]*>.*?</script>", " ", raw, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML comments
    raw = re.sub(r"<!--.*?-->", " ", raw, flags=re.DOTALL)
    # Remove SEI authentication footer
    raw = re.sub(
        r"(Documento assinado eletronicamente|Referência:?\s*Processo|"
        r"A autenticidade|Código Verificador|código CRC|"
        r"Para verificar a assinatura).*",
        " ", raw, flags=re.DOTALL | re.IGNORECASE,
    )
    # Remove all remaining tags
    text = re.sub(r"<[^>]+>", " ", raw)
    # Decode HTML entities
    text = html_mod.unescape(text)
    # Remove standalone hex codes (SEI CRC hashes)
    text = re.sub(r"\b[0-9A-F]{8}\b", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text, "html-strip"


def parse_txt(filepath: Path) -> tuple[str, str]:
    """Passthrough. No LLM needed."""
    text = filepath.read_text(encoding="utf-8", errors="replace").strip()
    return text, "passthrough"


PAGES_PER_BATCH = 3       # pages sent per API call (avoid context overflow)
IMAGE_MAX_DIMENSION = 1280  # resize large pages to fit within this (pixels)


def _resize_image(img, max_dim: int = IMAGE_MAX_DIMENSION):
    """Resize image if either dimension exceeds max_dim, preserving aspect ratio."""
    w, h = img.size
    if w <= max_dim and h <= max_dim:
        return img
    scale = min(max_dim / w, max_dim / h)
    new_size = (int(w * scale), int(h * scale))
    try:
        from PIL import Image as _PILImage
        resample = _PILImage.Resampling.LANCZOS
    except AttributeError:
        resample = 1  # LANCZOS fallback for older Pillow
    return img.resize(new_size, resample=resample)


def _image_to_base64(img, max_dim: int = IMAGE_MAX_DIMENSION) -> str:
    """Resize + compress image → base64 string."""
    img = _resize_image(img, max_dim)
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=80)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def parse_pdf_vision(filepath: Path, client, max_pages: int) -> tuple[str, str]:
    """Convert PDF pages to images → send to vision model in batches.

    Processes PAGES_PER_BATCH pages at a time to avoid exceeding
    the model's context window. Each batch produces a text chunk,
    and all chunks are concatenated.
    """
    from pdf2image import convert_from_path

    images = convert_from_path(str(filepath), first_page=1, last_page=max_pages)
    total_pages = len(images)
    log.info("  PDF: %d page(s), processing in batches of %d", total_pages, PAGES_PER_BATCH)

    all_text_parts = []

    for batch_start in range(0, total_pages, PAGES_PER_BATCH):
        batch = images[batch_start:batch_start + PAGES_PER_BATCH]
        batch_end = batch_start + len(batch)
        page_range = f"{batch_start + 1}-{batch_end}"

        image_contents = []
        for img in batch:
            b64 = _image_to_base64(img)
            image_contents.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
            })

        prompt = f"Transcreva o conteúdo completo das páginas {page_range} (de {total_pages}) do documento PDF abaixo:"
        user_content = [{"type": "text", "text": prompt}] + image_contents

        try:
            resp = client.chat.completions.create(
                model=MODEL_VISAO,
                messages=[
                    {"role": "system", "content": VISION_SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                temperature=0.3,
                max_tokens=4096,
            )
            chunk_text = resp.choices[0].message.content.strip()
            all_text_parts.append(f"--- Página(s) {page_range} ---\n{chunk_text}")
            log.info("  PDF batch %s/%d OK (%d chars)", page_range, total_pages, len(chunk_text))
        except Exception as e:
            log.warning("  PDF batch %s/%d FAILED: %s", page_range, total_pages, e)
            all_text_parts.append(f"--- Página(s) {page_range} ---\n[Erro: {e}]")

    text = "\n\n".join(all_text_parts)
    return text, f"vision-pdf ({total_pages} pages, {len(all_text_parts)} batches)"


def parse_image_vision(filepath: Path, client) -> tuple[str, str]:
    """Resize + send image to vision model."""
    from PIL import Image

    img = Image.open(filepath)
    b64 = _image_to_base64(img)

    user_content = [
        {"type": "text", "text": VISION_USER_PROMPT_IMAGE},
        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
    ]

    resp = client.chat.completions.create(
        model=MODEL_VISAO,
        messages=[
            {"role": "system", "content": VISION_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        temperature=0.3,
        max_tokens=4096,
    )
    text = resp.choices[0].message.content.strip()
    return text, "vision-image"


MAX_SPREADSHEET_CHARS = 6000  # max chars sent to LLM from spreadsheet data
MAX_SPREADSHEET_COLS = 30     # drop columns beyond this


def _clean_dataframe(df) -> "pd.DataFrame":
    """Clean messy spreadsheets: drop empty rows/cols, normalize headers, handle junk."""
    import pandas as pd

    # Drop completely empty rows and columns
    df = df.dropna(how="all", axis=0)
    df = df.dropna(how="all", axis=1)

    if df.empty:
        return df

    # If the first row looks like a header (all strings, no NaN), promote it
    first_row = df.iloc[0]
    if first_row.notna().all() and all(isinstance(v, str) for v in first_row):
        unnamed_cols = sum(1 for c in df.columns if str(c).startswith("Unnamed"))
        if unnamed_cols > len(df.columns) * 0.5:
            df.columns = [str(v).strip() for v in first_row]
            df = df.iloc[1:].reset_index(drop=True)

    # Normalize column names: strip whitespace, truncate long names
    df.columns = [str(c).strip()[:60] for c in df.columns]

    # Drop duplicate column names (keep first)
    df = df.loc[:, ~df.columns.duplicated()]

    # Limit columns to avoid horizontal explosion
    if len(df.columns) > MAX_SPREADSHEET_COLS:
        df = df.iloc[:, :MAX_SPREADSHEET_COLS]

    # Drop rows where >80% of values are NaN (mostly empty junk rows)
    threshold = len(df.columns) * 0.2
    df = df.dropna(thresh=max(int(threshold), 1))

    # Strip string values
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].map(lambda x: str(x).strip() if isinstance(x, str) else x)

    return df.reset_index(drop=True)


def _read_spreadsheet(filepath: Path) -> "tuple[list[pd.DataFrame], list[str]]":
    """Read spreadsheet, handling multiple sheets, bad encoding, and errors.
    Returns (list_of_dataframes, list_of_sheet_names).
    """
    import pandas as pd

    ext = filepath.suffix.lower()
    dfs = []
    names = []

    if ext == ".csv":
        # Try multiple encodings
        for enc in ("utf-8", "latin-1", "cp1252", "iso-8859-1"):
            try:
                df = pd.read_csv(filepath, encoding=enc, on_bad_lines="skip",
                                 nrows=500, dtype=str)
                dfs.append(df)
                names.append("csv")
                break
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        if not dfs:
            # Last resort: read as bytes
            df = pd.read_csv(filepath, encoding="latin-1", on_bad_lines="skip",
                             nrows=500, dtype=str)
            dfs.append(df)
            names.append("csv")
    else:
        # Excel: read all sheets
        try:
            xls = pd.ExcelFile(filepath, engine="openpyxl")
            for sheet_name in xls.sheet_names:
                try:
                    df = pd.read_excel(xls, sheet_name=sheet_name, nrows=500, dtype=str)
                    dfs.append(df)
                    names.append(sheet_name)
                except Exception as e:
                    log.warning("  Sheet '%s' failed: %s", sheet_name, e)
        except Exception as e:
            raise ValueError(f"Não foi possível abrir a planilha: {e}")

    return dfs, names


def parse_spreadsheet(filepath: Path, client) -> tuple[str, str]:
    """Parse spreadsheet with pandas → clean → send to LLM for summarization.

    Handles: multiple sheets, bad encoding, messy headers, empty rows/cols,
    and limits data sent to LLM to avoid context overflow.
    """
    try:
        dfs_raw, sheet_names = _read_spreadsheet(filepath)
    except Exception as e:
        return f"Erro ao ler planilha: {e}", "spreadsheet-error"

    if not dfs_raw:
        return "Planilha vazia ou ilegível.", "spreadsheet-empty"

    all_parts = []
    total_rows = 0

    for df_raw, sheet_name in zip(dfs_raw, sheet_names):
        df = _clean_dataframe(df_raw)
        if df.empty:
            all_parts.append(f"Aba '{sheet_name}': vazia após limpeza.")
            continue

        total_rows += len(df)
        header = (
            f"Aba: {sheet_name}\n"
            f"Colunas ({len(df.columns)}): {', '.join(df.columns)}\n"
            f"Linhas: {len(df)}"
        )

        # Build sample: limit rows so total chars stay within budget
        remaining_budget = MAX_SPREADSHEET_CHARS - sum(len(p) for p in all_parts)
        if remaining_budget < 500:
            all_parts.append(f"Aba '{sheet_name}': {len(df)} linhas (omitida por limite de tamanho)")
            continue

        sample = df.head(80).to_string(index=False, max_colwidth=60)
        if len(sample) > remaining_budget:
            # Reduce rows until it fits
            for n in (40, 20, 10, 5):
                sample = df.head(n).to_string(index=False, max_colwidth=60)
                if len(sample) <= remaining_budget:
                    break
            else:
                sample = sample[:remaining_budget] + "\n... (truncado)"

        all_parts.append(f"{header}\n\nDados:\n{sample}")

    spreadsheet_text = f"Planilha: {filepath.name}\n\n" + "\n\n---\n\n".join(all_parts)

    resp = client.chat.completions.create(
        model=MODEL_TEXTO,
        messages=[
            {"role": "system", "content": SPREADSHEET_SYSTEM_PROMPT},
            {"role": "user", "content": spreadsheet_text},
        ],
        temperature=0.3,
        max_tokens=4096,
    )
    llm_summary = resp.choices[0].message.content.strip()

    # Combine structure info + LLM summary
    structure_info = f"Planilha: {filepath.name} | Abas: {len(dfs_raw)} | Total linhas: {total_rows}"
    full_text = structure_info + "\n\n" + llm_summary
    return full_text, f"spreadsheet-llm ({len(dfs_raw)} sheets, {total_rows} rows)"


def parse_docx(filepath: Path) -> tuple[str, str]:
    """Extract text from .docx using python-docx. No LLM needed."""
    from docx import Document

    doc = Document(str(filepath))
    paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
    text = "\n".join(paragraphs)
    return text, "docx-extract"


# ── Main processing ─────────────────────────────────────────────────────────

def process_file(
    filepath: Path,
    client,
    max_pdf_pages: int,
) -> tuple[str, str]:
    """Route file to the appropriate parser. Returns (text, method)."""
    ext = filepath.suffix.lower()

    if ext in (".html", ".htm"):
        return parse_html(filepath)
    elif ext == ".txt":
        return parse_txt(filepath)
    elif ext in PDF_EXTENSIONS:
        return parse_pdf_vision(filepath, client, max_pdf_pages)
    elif ext in IMAGE_EXTENSIONS:
        return parse_image_vision(filepath, client)
    elif ext in SPREADSHEET_EXTENSIONS:
        return parse_spreadsheet(filepath, client)
    elif ext in DOCX_EXTENSIONS:
        return parse_docx(filepath)
    else:
        return "", f"unsupported ({ext})"


def discover_documents(
    input_dir: Path,
    protocolos: list[str] | None = None,
    extensions: set[str] | None = None,
) -> list[dict]:
    """Walk download directory. Returns list of {protocolo, documento_numero, filepath}.

    Args:
        extensions: if given, only include files with these extensions (e.g. {".html", ".htm"}).
                    Otherwise uses ALL_SUPPORTED.
    """
    allowed_exts = extensions if extensions else ALL_SUPPORTED
    documents = []
    if not input_dir.exists():
        log.error("Input directory does not exist: %s", input_dir)
        sys.exit(1)

    allowed_dirs = None
    if protocolos:
        allowed_dirs = set()
        for p in protocolos:
            allowed_dirs.add(p)
            allowed_dirs.add(p.replace("/", "_"))
            allowed_dirs.add(p.replace("_", "/"))

    for protocolo_dir in sorted(input_dir.iterdir()):
        if not protocolo_dir.is_dir():
            continue
        protocolo = protocolo_dir.name
        if allowed_dirs and protocolo not in allowed_dirs:
            continue
        for doc_dir in sorted(protocolo_dir.iterdir()):
            if not doc_dir.is_dir():
                continue
            documento_numero = doc_dir.name
            for fp in doc_dir.iterdir():
                if fp.is_file() and fp.suffix.lower() in allowed_exts:
                    documents.append({
                        "protocolo": protocolo,
                        "documento_numero": documento_numero,
                        "filepath": fp,
                    })
    return documents


def process_document(
    doc: dict,
    output_dir: Path,
    client,
    max_pdf_pages: int,
    skip_existing: bool,
) -> str:
    """Process a single document. Returns status string."""
    doc_id = doc["documento_numero"]
    output_txt = output_dir / f"{doc_id}.txt"
    output_json = output_dir / f"{doc_id}.json"

    if skip_existing and output_txt.exists():
        return "skipped"

    filepath = doc["filepath"]
    start = time.monotonic()

    try:
        text, method = process_file(filepath, client, max_pdf_pages)
    except Exception as e:
        log.error("  FAILED %s/%s (%s): %s", doc["protocolo"], doc_id, filepath.name, e)
        return "failed"

    elapsed = time.monotonic() - start

    if not text or len(text.strip()) < 5:
        log.warning("  Empty result for %s/%s (%s)", doc["protocolo"], doc_id, method)
        return "empty"

    # Save text
    output_txt.write_text(text, encoding="utf-8")

    # Save metadata
    meta = {
        "documento_numero": doc_id,
        "protocolo": doc["protocolo"],
        "source_file": filepath.name,
        "source_extension": filepath.suffix.lower(),
        "parse_method": method,
        "text_length": len(text),
        "elapsed_seconds": round(elapsed, 2),
        "parsed_at": datetime.now(timezone.utc).isoformat(),
    }
    output_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    log.info(
        "  OK %s/%s → %s (%d chars, %.1fs)",
        doc["protocolo"], doc_id, method, len(text), elapsed,
    )
    return "ok"


def _execute(args, settings) -> dict:
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse --ext filter
    ext_filter = None
    if args.ext:
        ext_filter = {e if e.startswith(".") else f".{e}" for e in args.ext}
        unsupported = ext_filter - ALL_SUPPORTED
        if unsupported:
            log.warning("Unsupported extensions will be ignored: %s", unsupported)
            ext_filter = ext_filter & ALL_SUPPORTED
        if not ext_filter:
            log.error("No supported extensions in --ext filter")
            sys.exit(1)
        log.info("Extension filter: %s", ", ".join(sorted(ext_filter)))

    # Check if LLM is needed
    needs_llm = ext_filter is None or bool(ext_filter & (IMAGE_EXTENSIONS | PDF_EXTENSIONS | SPREADSHEET_EXTENSIONS))
    if needs_llm and not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set in .env — cannot call LLM/vision models")
        sys.exit(1)

    if needs_llm:
        log.info("Models: vision=%s, text=%s", MODEL_VISAO, MODEL_TEXTO)
        log.info("API: %s", OPENAI_BASE_URL)

    # Discover documents
    documents = discover_documents(input_dir, args.processo, ext_filter)
    if args.limit > 0:
        documents = documents[:args.limit]

    log.info("Found %d documents to parse in %s", len(documents), input_dir)

    if not documents:
        log.info("Nothing to parse.")
        return {"total": 0, "ok": 0, "skipped": 0, "failed": 0, "empty": 0}

    # Show extension breakdown
    from collections import Counter
    ext_counts = Counter(d["filepath"].suffix.lower() for d in documents)
    for ext, count in ext_counts.most_common():
        needs_llm = ext in (IMAGE_EXTENSIONS | PDF_EXTENSIONS | SPREADSHEET_EXTENSIONS)
        tag = " (LLM)" if needs_llm else " (local)"
        log.info("  %s: %d files%s", ext, count, tag)

    # Create client (only if LLM extensions are in the set)
    client = _get_client() if needs_llm else None

    # Process
    stats = Counter()

    if args.workers <= 1:
        for i, doc in enumerate(documents):
            log.info("[%d/%d] %s/%s (%s)",
                     i + 1, len(documents), doc["protocolo"],
                     doc["documento_numero"], doc["filepath"].name)
            status = process_document(doc, output_dir, client, args.max_pdf_pages, args.skip_existing)
            stats[status] += 1
    else:
        def _do(doc):
            return process_document(doc, output_dir, client, args.max_pdf_pages, args.skip_existing)

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_do, d): d for d in documents}
            for i, future in enumerate(as_completed(futures)):
                doc = futures[future]
                try:
                    status = future.result()
                except Exception as e:
                    log.error("[%d/%d] %s/%s ERROR: %s",
                              i + 1, len(documents), doc["protocolo"], doc["documento_numero"], e)
                    status = "failed"
                stats[status] += 1
                if (i + 1) % 50 == 0:
                    log.info("Progress: %d/%d (ok=%d, skipped=%d, failed=%d, empty=%d)",
                             i + 1, len(documents), stats["ok"], stats["skipped"],
                             stats["failed"], stats["empty"])

    log.info(
        "DONE. Total: %d | Parsed: %d | Skipped: %d | Failed: %d | Empty: %d",
        len(documents), stats["ok"], stats["skipped"], stats["failed"], stats["empty"],
    )
    return {
        "total": len(documents),
        "ok": stats["ok"],
        "skipped": stats["skipped"],
        "failed": stats["failed"],
        "empty": stats["empty"],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Parse downloaded SEI documents into plain text using LLM/vision models"
    )
    parser.add_argument("--input", default="./documentos_sead",
                        help="Download directory (default: ./documentos_sead)")
    parser.add_argument("--output", default="./parsed_documents",
                        help="Output directory for parsed text (default: ./parsed_documents)")
    parser.add_argument("--processo", nargs="+", default=None,
                        help="Filter by specific protocolo(s)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit number of documents (0=all)")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip documents already parsed")
    parser.add_argument("--max-pdf-pages", type=int, default=10,
                        help="Max PDF pages to process per document (default: 10)")
    parser.add_argument("--ext", nargs="+", default=None,
                        help="Only process these extensions (e.g. --ext .html .pdf)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Parallel workers for LLM calls (default: 1, be careful with rate limits)")
    add_standard_args(parser, skip={
        "--neo4j-uri", "--neo4j-user", "--neo4j-password", "--neo4j-database",
        "--batch-size", "--workers", "--emit-json", "--read-json",
    })
    args = parser.parse_args()
    settings = resolve_settings(args)
    configure_logging(__name__, settings.log_level)
    _execute(args, settings)


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------
from argparse import Namespace as _Namespace  # noqa: E402

from ..registry import stage  # noqa: E402
from .._stage_base import RunContext, StageMeta  # noqa: E402


@stage(StageMeta(
    name="parse",
    description="Converte arquivos baixados em texto plano (HTML strip / PDF vision / OCR LLM).",
    type="enrich",
    depends_on=("download",),
    modes=("fs",),
    estimated_duration="1-2 docs/s (depende do LLM)",
))
def run(ctx: RunContext) -> None:
    args = _Namespace(
        input=ctx.flags.get("input", "./documentos_sead"),
        output=ctx.flags.get("output", "./parsed_documents"),
        processo=ctx.flags.get("processo"),
        limit=int(ctx.flags.get("limit") or 0),
        skip_existing=bool(ctx.flags.get("skip_existing", True)),
        max_pdf_pages=int(ctx.flags.get("max_pdf_pages") or 10),
        ext=ctx.flags.get("ext"),
        workers=int(ctx.flags.get("workers") or 1),
    )
    summary = _execute(args, ctx.settings) or {}
    ctx.cache["parse_summary"] = summary


if __name__ == "__main__":
    main()
