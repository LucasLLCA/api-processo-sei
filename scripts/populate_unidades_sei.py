"""
Script para popular a tabela unidades_sei a partir do CSV.

Uso:
    python -m scripts.populate_unidades_sei [caminho_csv]

Se o caminho não for fornecido, usa notebooks/cost/unidades_sei.csv
"""
import asyncio
import csv
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.database import engine, Base, AsyncSessionLocal
from api.models.unidade_sei import UnidadeSei
from sqlalchemy import text


async def populate(csv_path: str):
    # Create table if not exists
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all, tables=[UnidadeSei.__table__])

    # Read CSV
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Read {len(rows)} unidades from CSV")

    # Upsert in batches
    batch_size = 500
    inserted = 0

    async with AsyncSessionLocal() as session:
        # Truncate existing data for clean repopulation
        await session.execute(text("DELETE FROM unidades_sei"))
        await session.flush()

        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            for row in batch:
                unidade = UnidadeSei(
                    id_unidade=int(row["IdUnidade"]),
                    sigla=row["Sigla"].strip(),
                    descricao=row["Descricao"].strip(),
                )
                session.add(unidade)

            await session.flush()
            inserted += len(batch)
            print(f"  Inserted {inserted}/{len(rows)}...")

        await session.commit()

    print(f"Done. {inserted} unidades populated.")


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "..", "notebooks", "cost", "unidades_sei.csv"
    )

    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        sys.exit(1)

    asyncio.run(populate(csv_path))


if __name__ == "__main__":
    main()
