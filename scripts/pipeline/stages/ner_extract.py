"""
Extract Named Entity Recognition (NER) from downloaded SEI documents using GLiNER2.

Reads documents from the download directory structure produced by
download_documentos_sead.py and extracts entities using the GLiNER2 model.

Output structure:
  <output_dir>/<documento_numero>.json

Each JSON file contains:
  {
    "documento_numero": "...",
    "protocolo": "...",
    "source_file": "...",
    "model": "fastino/gliner2-base-v1",
    "labels": [...],
    "entities": { "label": [{"text": "...", "confidence": 0.95, "spans": [[0, 5]]}] },
    "extracted_at": "2026-03-31T12:00:00"
  }

Usage:
    python scripts/extract_ner_gliner2.py --input ./documentos_sead
    python scripts/extract_ner_gliner2.py --input ./documentos_sead --output ./ner_results
    python scripts/extract_ner_gliner2.py --input ./documentos_sead --model fastino/gliner2-large-v1
    python scripts/extract_ner_gliner2.py --input ./documentos_sead --labels pessoa orgao cargo cpf data
    python scripts/extract_ner_gliner2.py --input ./documentos_sead --limit 10 --threshold 0.5
    python scripts/extract_ner_gliner2.py --input ./documentos_sead --processo "00019.000123_2025-01"
    python scripts/extract_ner_gliner2.py --input ./documentos_sead --processo "00019.000123_2025-01" "00019.000456_2025-02"

Dependencies:
    pip install gliner2 pdfplumber
"""

import argparse
import sys as _sys
from pathlib import Path as _Path

_HERE = _Path(__file__).resolve()
_SCRIPTS = next(p for p in _HERE.parents if p.name == "scripts")
for _p in (_SCRIPTS, _SCRIPTS.parent):
    if str(_p) not in _sys.path:
        _sys.path.insert(0, str(_p))
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from pipeline.cli import add_standard_args, resolve_settings
from pipeline.constants import LABEL_TO_KEY
from pipeline.logging_setup import configure_logging
from pipeline.text import normalize as _normalize

log = configure_logging(__name__)

# Default entity labels with descriptions and examples for better accuracy.
# GLiNER2 uses the label text as context — more descriptive = more accurate extraction.
DEFAULT_LABELS = [
    "nome completo de pessoa física, ex: João Silva, Maria de Souza",
    "nome de empresa ou pessoa jurídica, ex: ABC LTDA, Fundação XYZ, Instituto Nacional",
    "órgão ou secretaria do governo, ex: SEAD-PI, SEJUS, Tribunal de Justiça",
    "cargo, função ou título de autoridade, ex: Secretário, Governador do Estado, Superintendente, Presidente, Diretor",
    "endereço de email, ex: fulano@sead.pi.gov.br",
    "número de CPF, ex: 807.713.433-53",
    "número de CNPJ, ex: 46.067.730/0001-00",
    "matrícula de servidor público, ex: 269422X, 124181-8",
    "data completa, ex: 10 de janeiro de 2025, 05/02/2025",
    "valor monetário em reais, ex: R$ 3.441,36, R$ 250.000,00",
    "endereço ou logradouro, ex: Av. Pedro Freitas, Bairro São Pedro, Teresina/PI",
    "número de telefone, ex: (86) 3216-1712",
    "número de processo SEI, ex: 00095.000323/2025-58",
    "número de lei, ex: Lei nº 6.201, Lei Complementar nº 13",
    "número de decreto, ex: Decreto nº 21.787, Decreto Estadual nº 18.142",
    "número de portaria, ex: Portaria nº 123/2025, Portaria GR nº 265",
    "número de contrato ou edital, ex: Edital 001/2025, Contrato nº 15/2024",
    "objeto ou assunto do documento, ex: progressão funcional, cessão de servidor, enquadramento",
    "objeto de licitação ou contrato, ex: prestação de serviços de TI, aquisição de equipamentos",
    "vigência ou prazo, ex: 12 meses, dois anos, 200 horas, prazo indeterminado",
    "endereço de website ou URL, ex: http://www.sead.pi.gov.br",
]

# LABEL_TO_KEY is imported from pipeline.constants — see top of file.


def _dedup_entities(values: list[dict]) -> list[dict]:
    """Deduplicate entities by normalized text, keeping the longest/most complete version.

    Handles:
      - Exact matches with different casing/accents: "Jacylenne Coêlho" == "JACYLENNE COELHO"
      - Prefix matches: "JACYLENNE COELHO" is a prefix of "JACYLENNE COELHO BEZERRA FORTES"
        → keeps the longer version only
    """
    # First pass: group by exact normalized key
    seen: dict[str, dict] = {}
    for val in values:
        text = val["text"] if isinstance(val, dict) else val
        key = _normalize(text)
        if key not in seen or len(text) > len(seen[key]["text"]):
            seen[key] = val

    # Second pass: merge prefix matches (shorter name is prefix of longer)
    keys = sorted(seen.keys(), key=len)
    merged: dict[str, dict] = {}
    for key in keys:
        # Check if this key is a prefix of any already-kept longer key
        is_prefix = False
        for existing_key in merged:
            if existing_key.startswith(key + " ") or key.startswith(existing_key + " "):
                # One is prefix of the other — keep the longer one
                if len(key) > len(existing_key):
                    # Current is longer, replace
                    merged[key] = seen[key]
                    del merged[existing_key]
                # else: existing is longer, skip current
                is_prefix = True
                break
        if not is_prefix:
            merged[key] = seen[key]

    return list(merged.values())


def _clean_entities(entities: dict[str, list]) -> dict[str, list]:
    """Post-process extracted entities to fix common errors."""
    import re as _re

    cleaned = {}
    for label, values in entities.items():
        # Map descriptive label to short key
        key = LABEL_TO_KEY.get(label, label)
        filtered = []
        for val in values:
            text = val["text"] if isinstance(val, dict) else val
            text = text.strip()

            # Skip empty or very short extractions (< 3 chars)
            if len(text) < 3:
                continue

            # ── pessoa ──
            if key == "pessoa":
                # Filter emails
                if "@" in text:
                    continue
                # Filter generic nouns (e.g. "visitantes", "membros", "servidores")
                if text.lower() in ("visitantes", "membros", "servidores", "servidor", "servidora",
                                     "requerente", "interessado", "interessada", "signatário"):
                    continue
                # Filter truncated fragments (single word < 6 chars)
                if len(text.split()) == 1 and len(text) < 6:
                    continue
                # Strip "servidora/servidor" prefix
                if text.lower().startswith(("servidor ", "servidora ")):
                    val = {"text": _re.sub(r"^servidora?\s+", "", text, flags=_re.IGNORECASE)}
                    text = val["text"]

            # ── pessoa_juridica ──
            if key == "pessoa_juridica":
                pessoa_texts = {v["text"].strip().lower() for v in entities.get("nome de pessoa física", entities.get("pessoa", []))} if isinstance(entities, dict) else set()
                if text.lower() in pessoa_texts:
                    continue
                if text.lower() in ("governo", "empresa", "servidora", "servidor"):
                    continue

            # ── orgao ──
            if key == "orgao":
                if text.lower() in ("servidora", "servidor", "s/n", "centro administrativo",
                                     "v.sa.", "v.sa", "vossa senhoria"):
                    continue

            # ── cargo ──
            if key == "cargo":
                # Filter org names misclassified as cargo
                if _re.search(r"secretaria d[aeo]|governo d[oe]", text, _re.IGNORECASE):
                    continue

            # ── cpf ──
            if key == "cpf":
                # CEP pattern
                if "CEP" in text.upper() or _re.fullmatch(r"\d{5}-\d{3}", text):
                    continue
                # Hex hashes
                if _re.fullmatch(r"[0-9A-F]{6,}", text):
                    continue
                # Document numbers (9 digits starting with 0)
                if _re.fullmatch(r"0\d{8}", text):
                    continue
                # Process numbers leaking (00095.000323/2025-58)
                if _re.search(r"\d{5}\.\d{6}/\d{4}-\d{2}", text):
                    continue
                # CEP pattern without prefix (64.018-900)
                if _re.fullmatch(r"\d{2}\.\d{3}-\d{3}", text):
                    continue

            # ── cnpj ──
            if key == "cnpj":
                # Hex hashes
                if _re.fullmatch(r"[0-9A-Fa-f]{6,}", text):
                    continue
                # Must look like a CNPJ (XX.XXX.XXX/XXXX-XX) or at least have digits
                if not _re.search(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", text):
                    continue

            # ── telefone ──
            if key == "telefone":
                # URLs
                if "http" in text or "www" in text:
                    continue
                # Timestamps (11:10, 14:23)
                if _re.fullmatch(r"\d{1,2}:\d{2}", text):
                    continue
                # Document numbers
                if _re.fullmatch(r"0\d{8}", text):
                    continue
                # Bare label word ("Telefone")
                if text.lower() in ("telefone", "tel", "fone", "fax"):
                    continue

            # ── email ──
            if key == "email":
                # Bare label word ("E-mail")
                if not "@" in text:
                    continue
                # Truncated emails (e.g. "s@uespi.br")
                if len(text.split("@")[0]) < 3:
                    continue

            # ── url ──
            if key == "url":
                # Must contain http or www
                if "http" not in text.lower() and "www" not in text.lower():
                    continue

            # ── valor_monetario ──
            if key == "valor_monetario":
                # Must have R$ prefix to be a real monetary value
                if not _re.search(r"R\$", text):
                    continue

            # ── portaria ──
            if key == "portaria":
                # Must have a number
                if not _re.search(r"\d", text):
                    continue
                # Document numbers (9 digits)
                if _re.fullmatch(r"0?\d{8,9}", text):
                    continue
                # "Art. 2º" is not a portaria
                if text.lower().startswith("art"):
                    continue

            # ── contrato_edital ──
            if key == "contrato_edital" and not _re.search(r"\d", text):
                continue

            # ── matricula ──
            if key == "matricula":
                # Process numbers leaking
                if _re.search(r"\d{5}\.\d{6}/\d{4}-\d{2}", text):
                    continue
                # Must have digits
                if not _re.search(r"\d", text):
                    continue
                # Starts with dot/space (garbled)
                if text.startswith((".", " ")):
                    val = {"text": text.lstrip(". ")}
                    text = val["text"]

            # ── assunto ──
            if key == "assunto":
                if text.lower() in ("documento", "assunto", "processo", "documento oficial", "autos"):
                    continue
                if len(text) < 5:
                    continue

            # ── objeto_licitacao ──
            if key == "objeto_licitacao" and len(text) < 5:
                continue

            # ── vigencia ──
            if key == "vigencia":
                if not _re.search(r"\d|mes|ano|dia|semana|prazo|hora", text, _re.IGNORECASE):
                    continue

            # ── lei ──
            if key == "lei":
                # Bare numbers or ambiguous fractions (13/94)
                if _re.fullmatch(r"[\d./]+", text):
                    continue

            # ── decreto ──
            if key == "decreto":
                if _re.fullmatch(r"[\d./]+", text):
                    continue

            filtered.append(val)

        if filtered:
            cleaned[key] = _dedup_entities(filtered)

    return cleaned


def _clean_relations(relations: dict[str, list]) -> dict[str, list]:
    """Remove noisy/truncated relation tuples."""
    GENERIC_TERMS = {"processo", "documento", "servidor", "servidora", "empresa",
                     "requerente", "portaria", "ofício", "oficio", "autos"}

    cleaned = {}
    for rel_type, pairs in relations.items():
        filtered = []
        for pair in pairs:
            # Skip if either side is too short (truncated chunks like "ando", "progres")
            if any(len(str(p).strip()) < 4 for p in pair):
                continue
            head, tail = str(pair[0]).strip(), str(pair[1]).strip()
            # Skip generic self-references
            if head.lower() == tail.lower():
                continue
            # Skip when either side is a generic term alone
            if head.lower() in GENERIC_TERMS or tail.lower() in GENERIC_TERMS:
                continue
            # Skip email → domain fragments
            if "@" in head and tail.startswith("."):
                continue
            if "@" in tail and head.startswith("."):
                continue
            # Skip single generic words as tail (e.g. "efeitos", "curso")
            if len(tail.split()) == 1 and tail.lower() in ("efeitos", "curso", "progres", "progressão"):
                continue
            filtered.append(pair)
        if filtered:
            cleaned[rel_type] = filtered
    return cleaned

# Chunk sizes auto-detected from model encoder's max_position_embeddings.
# Fallback defaults if detection fails.
DEFAULT_CHUNK_CHARS = 1800
DEFAULT_CHUNK_OVERLAP = 200


def _detect_chunk_size(extractor) -> tuple[int, int]:
    """Detect optimal chunk size from model's encoder max_position_embeddings."""
    try:
        max_tokens = extractor.encoder.config.max_position_embeddings
    except AttributeError:
        log.warning("Could not detect max_position_embeddings, using defaults")
        return DEFAULT_CHUNK_CHARS, DEFAULT_CHUNK_OVERLAP

    # ~3.5 chars per token for pt-BR, with safety margin (80%)
    chars = int(max_tokens * 3.5 * 0.8)
    overlap = int(chars * 0.1)

    log.info("Encoder max tokens: %d → chunk size: %d chars, overlap: %d", max_tokens, chars, overlap)
    return chars, overlap


def extract_text_from_html(filepath: Path) -> str:
    """Extract plain text from an HTML file, removing all non-visible content."""
    html = filepath.read_text(encoding="utf-8", errors="replace")
    # Remove <style> and <script> blocks entirely (including their content)
    html = re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML comments
    html = re.sub(r"<!--.*?-->", " ", html, flags=re.DOTALL)
    # Remove SEI authentication footer (contains hex codes, verification URLs, noise)
    html = re.sub(
        r"(Documento assinado eletronicamente|Referência:?\s*Processo|"
        r"A autenticidade|Código Verificador|código CRC|"
        r"Para verificar a assinatura).*",
        " ", html, flags=re.DOTALL | re.IGNORECASE,
    )
    # Remove all remaining tags
    text = re.sub(r"<[^>]+>", " ", html)
    # Decode HTML entities (&amp; &ordm; &Ccedil; etc.)
    import html as html_mod
    text = html_mod.unescape(text)
    # Remove standalone hex codes (SEI CRC hashes like AC470A64, 7E34DC47)
    text = re.sub(r"\b[0-9A-F]{8}\b", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_text_from_pdf(filepath: Path) -> str:
    """Extract text from a PDF file using pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        log.error("pdfplumber is required for PDF text extraction: pip install pdfplumber")
        sys.exit(1)

    pages_text = []
    try:
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
    except Exception as e:
        log.warning("Failed to extract text from %s: %s", filepath, e)
        return ""

    return "\n".join(pages_text)


def extract_text(filepath: Path) -> str:
    """Extract text from a document file based on its extension."""
    suffix = filepath.suffix.lower()
    if suffix in (".html", ".htm"):
        return extract_text_from_html(filepath)
    elif suffix == ".pdf":
        return extract_text_from_pdf(filepath)
    else:
        # Try reading as plain text
        try:
            return filepath.read_text(encoding="utf-8", errors="replace")
        except Exception:
            log.warning("Unsupported file type: %s", filepath)
            return ""


def chunk_text(text: str, max_chars: int = DEFAULT_CHUNK_CHARS, overlap: int = DEFAULT_CHUNK_OVERLAP) -> list[str]:
    """Split text into overlapping chunks for processing."""
    if len(text) <= max_chars:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + max_chars
        chunk = text[start:end]
        chunks.append(chunk)
        start = end - overlap
    return chunks


def merge_entities(all_entities: list[dict], labels: list[str]) -> dict:
    """Merge entity results from multiple chunks, deduplicating by text."""
    merged = {label: [] for label in labels}

    for chunk_result in all_entities:
        entities = chunk_result.get("entities", {})
        for label in labels:
            for entity in entities.get(label, []):
                entity_text = entity["text"] if isinstance(entity, dict) else entity
                # Check for duplicate (same label + same text)
                existing_texts = set()
                for e in merged[label]:
                    t = e["text"] if isinstance(e, dict) else e
                    existing_texts.add(t.strip().lower())
                if entity_text.strip().lower() not in existing_texts:
                    merged[label].append(entity)

    # Remove empty labels
    return {k: v for k, v in merged.items() if v}


DEFAULT_EXTENSIONS = {".pdf", ".html", ".htm", ".txt"}


def discover_documents(
    input_dir: Path,
    protocolos: list[str] | None = None,
    extensions: set[str] | None = None,
) -> list[dict]:
    """Walk the download directory and discover all document files.

    Expected structure: <input_dir>/<protocolo>/<documento_numero>/<filename>
    If protocolos is given, only process those directories.
    Protocolo values can use either '/' or '_' as separator (both are matched).
    """
    allowed_exts = extensions if extensions else DEFAULT_EXTENSIONS
    documents = []
    if not input_dir.exists():
        log.error("Input directory does not exist: %s", input_dir)
        sys.exit(1)

    # Normalize protocolos: accept both "00019.000123/2025-01" and "00019.000123_2025-01"
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
            for filepath in doc_dir.iterdir():
                if filepath.is_file() and filepath.suffix.lower() in allowed_exts:
                    documents.append({
                        "protocolo": protocolo,
                        "documento_numero": documento_numero,
                        "filepath": filepath,
                    })
    return documents


def main():
    parser = argparse.ArgumentParser(
        description="Extract NER entities from downloaded SEI documents using GLiNER2"
    )
    parser.add_argument(
        "--input", default="./documentos_sead",
        help="Input directory with downloaded documents (default: ./documentos_sead)",
    )
    parser.add_argument(
        "--output", default="./ner_results",
        help="Output directory for JSON results (default: ./ner_results)",
    )
    parser.add_argument(
        "--model", default="fastino/gliner2-base-v1",
        help="GLiNER2 model name (default: fastino/gliner2-base-v1)",
    )
    parser.add_argument(
        "--labels", nargs="+", default=None,
        help="Entity labels to extract (default: governo/admin labels)",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.3,
        help="Minimum confidence threshold for entities (default: 0.3)",
    )
    parser.add_argument(
        "--flat-ner", action="store_true", default=True,
        help="Use flat NER mode (default: True)",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Limit number of documents to process (0=all)",
    )
    parser.add_argument(
        "--skip-existing", action="store_true",
        help="Skip documents that already have a JSON output file",
    )
    parser.add_argument(
        "--processo", nargs="+", default=None,
        help="Specific protocolo(s) to process (directory names, e.g. '00019.000123_2025-01')",
    )
    parser.add_argument(
        "--ext", nargs="+", default=None,
        help="Only process these extensions (e.g. --ext .html .txt)",
    )
    parser.add_argument(
        "--mode", default="hybrid", choices=["gliner2", "llm", "hybrid"],
        help="NER mode: gliner2 (fast, current), llm (Mandu only), hybrid (gliner2+llm cleanup, default)",
    )
    # Pipeline standard flags relevant here: --log-level. The remaining
    # standard flags (Neo4j/Postgres/JSON I/O) don't apply to NER extraction.
    add_standard_args(parser, skip={
        "--neo4j-uri", "--neo4j-user", "--neo4j-password", "--neo4j-database",
        "--batch-size", "--workers", "--emit-json", "--read-json",
    })
    args = parser.parse_args()
    settings = resolve_settings(args)
    configure_logging(__name__, settings.log_level)
    _execute(args, settings)


def _execute(args, settings) -> dict:
    labels = args.labels or DEFAULT_LABELS
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    mode = (getattr(args, "mode", None) or "hybrid").lower()
    if mode not in ("gliner2", "llm", "hybrid"):
        log.error("invalid mode=%s — expected gliner2|llm|hybrid", mode)
        sys.exit(2)
    log.info("ner-extract mode: %s", mode)

    # Parse --ext filter
    ext_filter = None
    if args.ext:
        ext_filter = {e if e.startswith(".") else f".{e}" for e in args.ext}
        log.info("Extension filter: %s", ", ".join(sorted(ext_filter)))

    # Discover documents
    documents = discover_documents(input_dir, args.processo, ext_filter)
    if args.limit > 0:
        documents = documents[: args.limit]

    log.info("Found %d documents in %s", len(documents), input_dir)

    if not documents:
        log.info("Nothing to process.")
        return

    # Load GLiNER2 model only if needed (mode `gliner2` or `hybrid`)
    extractor = None
    max_chunk_chars = 1800
    chunk_overlap = 200
    if mode in ("gliner2", "hybrid"):
        log.info("Loading GLiNER2 model: %s", args.model)
        try:
            from gliner2 import GLiNER2
        except ImportError:
            log.error("gliner2 is required: pip install gliner2")
            sys.exit(1)
        extractor = GLiNER2.from_pretrained(args.model)
        log.info("Model loaded successfully")
        max_chunk_chars, chunk_overlap = _detect_chunk_size(extractor)

    # Build LLM client if needed (mode `llm` or `hybrid`)
    ner_llm = None
    if mode in ("llm", "hybrid"):
        from ..ner_llm import NerLLM, config_from_settings
        ner_llm = NerLLM(config_from_settings(settings))
        log.info("LLM ready: model=%s base_url=%s", ner_llm.config.model, ner_llm.config.base_url)

    # Process documents
    processed = 0
    skipped = 0
    failed = 0

    for doc in documents:
        doc_id = doc["documento_numero"]
        output_file = output_dir / f"{doc_id}.json"

        if args.skip_existing and output_file.exists():
            skipped += 1
            continue

        filepath = doc["filepath"]
        log.info("[%d/%d] Processing %s/%s (%s)", processed + skipped + failed + 1, len(documents), doc["protocolo"], doc_id, filepath.name)

        # Extract text
        text = extract_text(filepath)
        if not text or len(text.strip()) < 10:
            log.warning("No text extracted from %s — skipping", filepath)
            failed += 1
            continue

        # Run extraction per mode
        entities: dict[str, list[dict]] = {}
        classification: dict = {}
        relations: dict[str, list] = {}
        consolidation_metrics: dict = {}
        gliner_entities_before_llm: dict[str, list[dict]] = {}
        used_model_label = args.model

        if mode in ("gliner2", "hybrid"):
            chunks = chunk_text(text, max_chars=max_chunk_chars, overlap=chunk_overlap)
            all_chunk_entities = []
            all_chunk_relations = []
            all_chunk_classifications = []

            for i, chunk in enumerate(chunks):
                try:
                    ner_result = extractor.extract_entities(chunk, labels)
                    all_chunk_entities.append(ner_result.get("entities", {}))
                except Exception as e:
                    log.warning("NER failed on chunk %d of %s: %s", i, doc_id, e)

                if i == 0:
                    try:
                        cls_result = extractor.classify_text(chunk, {
                            "tipo_documento": [
                                "licitacao", "contrato", "parecer_juridico", "oficio",
                                "portaria", "despacho", "nota_tecnica", "ata",
                                "termo_referencia", "certidao", "declaracao", "requerimento",
                            ]
                        })
                        all_chunk_classifications.append(cls_result)
                    except Exception as e:
                        log.debug("Classification failed on chunk %d of %s: %s", i, doc_id, e)

                try:
                    rel_result = extractor.extract_relations(chunk, [
                        "autorizou", "assinou", "encaminhou_para", "solicitou",
                        "contratou", "nomeou", "exonerou", "designou",
                    ])
                    rel_data = rel_result.get("relation_extraction", {})
                    if any(v for v in rel_data.values()):
                        all_chunk_relations.append(rel_data)
                except Exception as e:
                    log.debug("Relation extraction failed on chunk %d of %s: %s", i, doc_id, e)

            if not all_chunk_entities:
                log.warning("No entities extracted from %s", doc_id)
                failed += 1
                continue

            # Merge entities across chunks, dedup by text
            for chunk_ents in all_chunk_entities:
                for label, values in chunk_ents.items():
                    if label not in entities:
                        entities[label] = []
                    for val in values:
                        text_val = val if isinstance(val, str) else str(val)
                        existing = {e["text"].strip().lower() for e in entities[label]}
                        if text_val.strip().lower() not in existing:
                            entities[label].append({"text": text_val})

            # Merge relations
            for chunk_rels in all_chunk_relations:
                for rel_type, pairs in chunk_rels.items():
                    if not pairs:
                        continue
                    if rel_type not in relations:
                        relations[rel_type] = []
                    for pair in pairs:
                        if pair not in relations[rel_type]:
                            relations[rel_type].append(pair)

            classification = all_chunk_classifications[0] if all_chunk_classifications else {}

            # Post-process: clean noisy extractions (existing rules)
            entities = _clean_entities(entities)
            relations = _clean_relations(relations)

            # Snapshot for hybrid metrics before LLM cleanup
            if mode == "hybrid":
                gliner_entities_before_llm = {k: list(v) for k, v in entities.items()}

        # LLM-only or hybrid cleanup
        if mode == "llm":
            try:
                llm_out = ner_llm.extract(text)
                entities = llm_out.get("entities", {})
                classification = llm_out.get("classification", {}) or classification
                relations = llm_out.get("relations", {}) or relations
                used_model_label = f"llm-only:{ner_llm.config.model}"
            except Exception as e:
                log.error("LLM extract failed for %s: %s", doc_id, e)
                failed += 1
                continue

        elif mode == "hybrid":
            try:
                llm_clean = ner_llm.consolidate(text, {
                    "entities": entities, "classification": classification,
                })
                entities_after = llm_clean.get("entities", {})
                # Compute consolidation metrics before swapping in
                from ..ner_llm import diff_metrics
                consolidation_metrics = diff_metrics(gliner_entities_before_llm, entities_after)
                entities = entities_after
                # LLM may also refine the document type; keep its value if provided
                if llm_clean.get("classification"):
                    classification = llm_clean["classification"]
                used_model_label = f"{args.model}+llm-cleanup:{ner_llm.config.model}"
            except Exception as e:
                log.warning("LLM consolidate failed for %s — keeping GLiNER output: %s", doc_id, e)
                # Fall back to GLiNER-only output (already in `entities`)
                used_model_label = f"{args.model}+llm-cleanup-failed"

        # Normalize entity records to unified shape (text + canonical + provenance)
        from ..ner_llm import normalize_entity_record
        normalized: dict[str, list[dict]] = {}
        for label, items in entities.items():
            normalized[label] = [normalize_entity_record(it) for it in items]
            # Default provenance per mode when missing
            for rec in normalized[label]:
                if rec["provenance"] not in ("gliner", "llm", "hybrid"):
                    rec["provenance"] = (
                        "gliner" if mode == "gliner2"
                        else "llm" if mode == "llm"
                        else "hybrid"
                    )
        entities = normalized

        # Build output
        output_data = {
            "documento_numero": doc_id,
            "protocolo": doc["protocolo"],
            "source_file": filepath.name,
            "model": used_model_label,
            "mode": mode,
            "labels": [LABEL_TO_KEY.get(l, l) for l in labels],
            "entities": entities,
            "classification": classification,
            "relations": relations,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }
        if consolidation_metrics:
            output_data["consolidation_metrics"] = consolidation_metrics

        output_file.write_text(json.dumps(output_data, ensure_ascii=False, indent=2), encoding="utf-8")
        processed += 1
        entity_count = sum(len(v) for v in entities.values())
        relation_count = sum(len(v) for v in relations.values())
        cls_label = classification.get("tipo_documento", "?")
        log.info("  → %d entities, %d relations, tipo=%s, mode=%s → %s",
                 entity_count, relation_count, cls_label, mode, output_file.name)

    log.info(
        "DONE. Processed: %d | Skipped: %d | Failed: %d | Total: %d",
        processed, skipped, failed, len(documents),
    )
    return {
        "total": len(documents),
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
    }


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------
from argparse import Namespace as _Namespace  # noqa: E402

from ..registry import stage  # noqa: E402
from .._stage_base import RunContext, StageMeta  # noqa: E402


@stage(StageMeta(
    name="ner-extract",
    description="Extrai entidades nomeadas dos documentos via GLiNER2.",
    type="enrich",
    depends_on=("download",),
    soft_depends_on=("parse",),
    modes=("fs",),
    estimated_duration="3-10s/doc dependendo do tamanho + modelo",
))
def run(ctx: RunContext) -> None:
    args = _Namespace(
        input=ctx.flags.get("input", "./documentos_sead"),
        output=ctx.flags.get("output", "./ner_results"),
        model=ctx.flags.get("model", "fastino/gliner2-base-v1"),
        labels=ctx.flags.get("labels"),
        threshold=float(ctx.flags.get("threshold") or 0.3),
        flat_ner=bool(ctx.flags.get("flat_ner", True)),
        limit=int(ctx.flags.get("limit") or 0),
        skip_existing=bool(ctx.flags.get("skip_existing", True)),
        processo=ctx.flags.get("processo"),
        ext=ctx.flags.get("ext"),
        mode=ctx.flags.get("mode", "hybrid"),
    )
    summary = _execute(args, ctx.settings) or {}
    ctx.cache["ner_extract_summary"] = summary


if __name__ == "__main__":
    main()
