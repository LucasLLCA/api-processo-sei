"""
Analyse downloaded documents directory and summarise by file extension.

Usage:
    python scripts/analyse_downloads.py
    python scripts/analyse_downloads.py --input ./documentos_sead
    python scripts/analyse_downloads.py --input ./documentos_sead --parsed ./parsed_documents --ner-output ./ner_results
    python scripts/analyse_downloads.py --input ./documentos_sead --top 20
"""

import argparse
from collections import Counter
from pathlib import Path

# ── Extension categories (matches parse_documents.py routing) ──

LOCAL_PARSE = {".html", ".htm", ".txt", ".doc", ".docx"}      # No LLM needed
VISION_PARSE = {".pdf", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}  # Vision model
LLM_PARSE = {".xlsx", ".xls", ".csv"}                          # Text LLM

ALL_PARSEABLE = LOCAL_PARSE | VISION_PARSE | LLM_PARSE


def analyse(input_dir: Path, parsed_dir: Path | None, ner_output: Path | None, top: int):
    if not input_dir.exists():
        print(f"Directory not found: {input_dir}")
        return

    ext_count: Counter[str] = Counter()
    ext_size: Counter[str] = Counter()
    processos = set()
    documentos = set()
    empty_docs = 0
    total_files = 0

    # Per-category tracking
    local_files = 0
    vision_files = 0
    llm_files = 0
    unsupported_files = 0
    local_size = 0
    vision_size = 0
    llm_size = 0
    unsupported_size = 0

    # Track which documento_numero have at least one parseable file
    parseable_docs: set[str] = set()
    all_docs_with_files: set[str] = set()

    for processo_dir in input_dir.iterdir():
        if not processo_dir.is_dir():
            continue
        processos.add(processo_dir.name)
        for doc_dir in processo_dir.iterdir():
            if not doc_dir.is_dir():
                continue
            doc_numero = doc_dir.name
            documentos.add(doc_numero)
            files = [f for f in doc_dir.iterdir() if f.is_file()]
            if not files:
                empty_docs += 1
                continue
            all_docs_with_files.add(doc_numero)
            for f in files:
                ext = f.suffix.lower() or "(no extension)"
                size = f.stat().st_size
                ext_count[ext] += 1
                ext_size[ext] += size
                total_files += 1

                if ext in LOCAL_PARSE:
                    local_files += 1
                    local_size += size
                    parseable_docs.add(doc_numero)
                elif ext in VISION_PARSE:
                    vision_files += 1
                    vision_size += size
                    parseable_docs.add(doc_numero)
                elif ext in LLM_PARSE:
                    llm_files += 1
                    llm_size += size
                    parseable_docs.add(doc_numero)
                else:
                    unsupported_files += 1
                    unsupported_size += size

    total_size = sum(ext_size.values())
    parseable_files = local_files + vision_files + llm_files
    parseable_size = local_size + vision_size + llm_size

    # Check parsed results
    parsed_done: set[str] = set()
    if parsed_dir and parsed_dir.exists():
        for f in parsed_dir.iterdir():
            if f.is_file() and f.suffix == ".txt":
                parsed_done.add(f.stem)

    # Check NER results
    ner_done: set[str] = set()
    if ner_output and ner_output.exists():
        for f in ner_output.iterdir():
            if f.is_file() and f.suffix == ".json":
                ner_done.add(f.stem)

    # ── Print report ──

    print(f"\n{'=' * 70}")
    print(f"  DOWNLOAD ANALYSIS: {input_dir}")
    print(f"{'=' * 70}")
    print(f"  Processos:              {len(processos):,}")
    print(f"  Documento directories:  {len(documentos):,}")
    print(f"  Empty doc directories:  {empty_docs:,}")
    print(f"  Total files:            {total_files:,}")
    print(f"  Total size:             {_fmt_size(total_size)}")

    # Extension breakdown
    print(f"\n{'─' * 70}")
    print(f"  EXTENSIONS")
    print(f"{'─' * 70}")
    print(f"  {'Extension':<20} {'Files':>10} {'Size':>14} {'% Files':>10} {'Method':>14}")
    print(f"  {'─' * 20} {'─' * 10} {'─' * 14} {'─' * 10} {'─' * 14}")

    for ext, count in ext_count.most_common(top):
        size = ext_size[ext]
        pct_files = (count / total_files * 100) if total_files else 0
        if ext in LOCAL_PARSE:
            method = "local"
        elif ext in VISION_PARSE:
            method = "vision"
        elif ext in LLM_PARSE:
            method = "llm"
        else:
            method = "unsupported"
        print(f"  {ext:<20} {count:>10,} {_fmt_size(size):>14} {pct_files:>9.1f}% {method:>14}")

    # Parseable summary
    pct_p_files = (parseable_files / total_files * 100) if total_files else 0
    pct_p_size = (parseable_size / total_size * 100) if total_size else 0
    pct_p_docs = (len(parseable_docs) / len(all_docs_with_files) * 100) if all_docs_with_files else 0

    print(f"\n{'─' * 70}")
    print(f"  PARSEABLE FILES (parse_documents.py)")
    print(f"{'─' * 70}")
    print(f"  Total parseable:        {parseable_files:,} / {total_files:,}  ({pct_p_files:.1f}%)")
    print(f"  Parseable size:         {_fmt_size(parseable_size)} / {_fmt_size(total_size)}  ({pct_p_size:.1f}%)")
    print(f"  Parseable documents:    {len(parseable_docs):,} / {len(all_docs_with_files):,}  ({pct_p_docs:.1f}%)")
    print(f"  Unsupported files:      {unsupported_files:,}  ({_fmt_size(unsupported_size)})")
    print()
    print(f"  By method:")
    _print_method("local (html/txt/docx)", local_files, local_size, parseable_files)
    _print_method("vision (pdf/images)",   vision_files, vision_size, parseable_files)
    _print_method("llm (xlsx/csv)",        llm_files, llm_size, parseable_files)

    # Parsing progress
    if parsed_dir:
        parsed_pending = parseable_docs - parsed_done
        parsed_completed = parseable_docs & parsed_done
        print(f"\n{'─' * 70}")
        print(f"  PARSING PROGRESS ({parsed_dir})")
        print(f"{'─' * 70}")
        if parsed_dir.exists():
            pct_done = (len(parsed_completed) / len(parseable_docs) * 100) if parseable_docs else 0
            print(f"  Parsed:      {len(parsed_completed):,} / {len(parseable_docs):,}  ({pct_done:.1f}%)")
            print(f"  Pending:     {len(parsed_pending):,}")
        else:
            print(f"  Output directory not found — 0% complete")
            print(f"  Pending:     {len(parseable_docs):,}")

    # NER progress
    if ner_output:
        # NER can run on parsed docs (from parsed_dir) or directly on text files
        ner_eligible = parsed_done if parsed_dir and parsed_dir.exists() else parseable_docs
        ner_completed = ner_eligible & ner_done
        ner_pending = ner_eligible - ner_done
        print(f"\n{'─' * 70}")
        print(f"  NER PROGRESS ({ner_output})")
        print(f"{'─' * 70}")
        if ner_output.exists():
            pct_done = (len(ner_completed) / len(ner_eligible) * 100) if ner_eligible else 0
            print(f"  Completed:   {len(ner_completed):,} / {len(ner_eligible):,}  ({pct_done:.1f}%)")
            print(f"  Pending:     {len(ner_pending):,}")
        else:
            print(f"  Output directory not found — 0% complete")
            print(f"  Pending:     {len(ner_eligible):,}")

    print(f"{'=' * 70}\n")


def _print_method(label: str, files: int, size: int, total: int):
    pct = (files / total * 100) if total else 0
    print(f"    {label:<25} {files:>8,} files  {_fmt_size(size):>12}  ({pct:.1f}%)")


def _fmt_size(b: int | float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def main():
    parser = argparse.ArgumentParser(description="Analyse downloaded documents by file extension")
    parser.add_argument("--input", default="./documentos_sead",
                        help="Download directory (default: ./documentos_sead)")
    parser.add_argument("--parsed", default=None,
                        help="Parsed text directory to check progress (e.g. ./parsed_documents)")
    parser.add_argument("--ner-output", default=None,
                        help="NER results directory to check progress (e.g. ./ner_results)")
    parser.add_argument("--top", type=int, default=50,
                        help="Show top N extensions (default: 50)")
    args = parser.parse_args()

    parsed_path = Path(args.parsed) if args.parsed else None
    ner_path = Path(args.ner_output) if args.ner_output else None
    analyse(Path(args.input), parsed_path, ner_path, args.top)


if __name__ == "__main__":
    main()
