"""Populate the `unidades_sei` table from the canonical CSV.

Bootstrap step run once per environment (the table is read by ETL Phase A
to enrich `Unidade.id_unidade` / `Unidade.descricao`).

Usage:
    python scripts/bootstrap/populate_unidades_sei.py
    python scripts/bootstrap/populate_unidades_sei.py --csv-path /path/to/unidades_sei.csv
    python scripts/bootstrap/populate_unidades_sei.py --batch-size 1000

If --csv-path is omitted, defaults to `<project>/notebooks/cost/unidades_sei.csv`.
"""

import argparse
import asyncio
import csv
import sys
from pathlib import Path

_HERE = Path(__file__).resolve()
_SCRIPTS = next(p for p in _HERE.parents if p.name == "scripts")
_PROJECT = _SCRIPTS.parent
for _p in (_SCRIPTS, _PROJECT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from api.database import engine, Base, AsyncSessionLocal  # noqa: E402
from api.models.unidade_sei import UnidadeSei  # noqa: E402
from pipeline.logging_setup import configure_logging  # noqa: E402
from sqlalchemy import text  # noqa: E402

log = configure_logging(__name__)

_DEFAULT_CSV = _PROJECT.parent / "notebooks" / "cost" / "unidades_sei.csv"


async def populate(csv_path: Path, batch_size: int) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all, tables=[UnidadeSei.__table__])

    with csv_path.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    log.info("Read %d unidades from CSV", len(rows))

    inserted = 0
    async with AsyncSessionLocal() as session:
        await session.execute(text("DELETE FROM unidades_sei"))
        await session.flush()

        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            for row in batch:
                session.add(UnidadeSei(
                    id_unidade=int(row["IdUnidade"]),
                    sigla=row["Sigla"].strip(),
                    descricao=row["Descricao"].strip(),
                ))
            await session.flush()
            inserted += len(batch)
            log.info("  Inserted %d/%d...", inserted, len(rows))

        await session.commit()

    log.info("Done. %d unidades populated.", inserted)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--csv-path", type=Path, default=_DEFAULT_CSV,
                        help=f"CSV with IdUnidade,Sigla,Descricao (default: {_DEFAULT_CSV})")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Insert batch size (default: 500)")
    args = parser.parse_args()

    if not args.csv_path.is_file():
        log.error("CSV not found: %s", args.csv_path)
        sys.exit(1)

    asyncio.run(populate(args.csv_path, args.batch_size))


if __name__ == "__main__":
    main()
