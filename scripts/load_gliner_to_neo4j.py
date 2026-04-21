"""
Load GLiNER2 extraction results into Neo4j, linking to existing Documento nodes.

Extended graph model (new nodes and relationships):

  New Nodes:
    - PessoaFisica       {nome*}
    - PessoaJuridica     {nome*}
    - CargoFuncao        {nome*}
    - Legislacao          {referencia*, tipo}       # tipo: lei, decreto, portaria
    - ContratoEdital      {referencia*}
    - ClasseNivel         {referencia*}

  New Relationships:
    - (Documento)-[:MENCIONA_PESSOA {fonte}]->(PessoaFisica)
    - (Documento)-[:MENCIONA_EMPRESA {fonte}]->(PessoaJuridica)
    - (Documento)-[:MENCIONA_ORGAO {fonte}]->(Orgao)           # links to EXISTING Orgao nodes
    - (Documento)-[:MENCIONA_CARGO {fonte}]->(CargoFuncao)
    - (Documento)-[:MENCIONA_LEGISLACAO {fonte}]->(Legislacao)
    - (Documento)-[:MENCIONA_CONTRATO {fonte}]->(ContratoEdital)
    - (Documento)-[:MENCIONA_CLASSE {fonte}]->(ClasseNivel)
    - (Documento)-[:CLASSIFICADO_COMO {tipo_documento, modelo}]->()  # set as property on Documento

  New Properties on Documento:
    - tipo_documento_gliner   (e.g. "certidao", "despacho", "oficio")
    - assunto_gliner          (extracted subject)
    - emails_mencionados      (list of emails found)
    - cpfs_mencionados        (list of CPFs found)
    - cnpjs_mencionados       (list of CNPJs found)
    - matriculas_mencionadas  (list of matrículas found)
    - telefones_mencionados   (list of phone numbers found)
    - urls_mencionadas        (list of URLs found)
    - valores_monetarios      (list of R$ values found)
    - datas_mencionadas       (list of dates found)
    - enderecos_mencionados   (list of addresses found)
    - objeto_licitacao        (extracted tender/contract object)
    - vigencia                (extracted duration/deadline)
    - gliner_model            (model used for extraction)
    - gliner_extracted_at     (timestamp)

  Relation Extraction → Relationships:
    - (PessoaFisica)-[:ASSINOU]->(Documento)
    - (PessoaFisica)-[:AUTORIZOU {objeto}]->(Documento)
    - (PessoaFisica)-[:SOLICITOU {objeto}]->(Documento)
    - (entity)-[:ENCAMINHOU_PARA {destino}]->(Documento)
    - (entity)-[:NOMEOU {pessoa}]->(Documento)
    - (entity)-[:DESIGNOU {pessoa}]->(Documento)
    - (entity)-[:EXONEROU {pessoa}]->(Documento)
    - (entity)-[:CONTRATOU {objeto}]->(Documento)

Usage:
    python scripts/load_gliner_to_neo4j.py
    python scripts/load_gliner_to_neo4j.py --input ./ner_results
    python scripts/load_gliner_to_neo4j.py --input ./ner_results --dry-run
    python scripts/load_gliner_to_neo4j.py --input ./ner_results --limit 10
    python scripts/load_gliner_to_neo4j.py --input ./ner_results --clear-first
"""

import argparse
import json
import sys
from pathlib import Path

from pipeline.cli import add_standard_args, resolve_settings
from pipeline.config import ConfigError
from pipeline.logging_setup import configure_logging
from pipeline.neo4j_driver import build_driver
from pipeline.text import normalize as _normalize
from pipeline.writers import DirectNeo4jWriter, GraphWriter, JsonFileWriter

log = configure_logging(__name__)

# ── Constraints ────────────────────────────────────────────────────────────

SETUP_CONSTRAINTS = [
    "CREATE CONSTRAINT pessoa_fisica_norm IF NOT EXISTS FOR (p:PessoaFisica) REQUIRE p.nome_normalizado IS UNIQUE",
    "CREATE CONSTRAINT pessoa_juridica_norm IF NOT EXISTS FOR (p:PessoaJuridica) REQUIRE p.nome_normalizado IS UNIQUE",
    "CREATE CONSTRAINT cargo_funcao_norm IF NOT EXISTS FOR (c:CargoFuncao) REQUIRE c.nome_normalizado IS UNIQUE",
    "CREATE CONSTRAINT legislacao_referencia IF NOT EXISTS FOR (l:Legislacao) REQUIRE l.referencia IS UNIQUE",
    "CREATE CONSTRAINT contrato_edital_ref IF NOT EXISTS FOR (c:ContratoEdital) REQUIRE c.referencia IS UNIQUE",
]

# ── Cypher templates ───────────────────────────────────────────────────────

# Set properties on existing Documento node
SET_DOC_PROPERTIES = """
MATCH (d:Documento {numero: $doc_numero})
SET d.tipo_documento_gliner = $tipo_documento,
    d.assunto_gliner = $assunto,
    d.emails_mencionados = $emails,
    d.cpfs_mencionados = $cpfs,
    d.cnpjs_mencionados = $cnpjs,
    d.matriculas_mencionadas = $matriculas,
    d.telefones_mencionados = $telefones,
    d.urls_mencionadas = $urls,
    d.valores_monetarios = $valores,
    d.datas_mencionadas = $datas,
    d.enderecos_mencionados = $enderecos,
    d.objeto_licitacao_gliner = $objeto_licitacao,
    d.vigencia_gliner = $vigencia,
    d.gliner_model = $model,
    d.gliner_extracted_at = $extracted_at
RETURN d.numero AS numero
"""

# Link Documento → PessoaFisica (MERGE by normalized key, keep display name)
LINK_PESSOA = """
MATCH (d:Documento {numero: $doc_numero})
UNWIND $items AS item
MERGE (p:PessoaFisica {nome_normalizado: item.norm})
ON CREATE SET p.nome = item.nome
ON MATCH SET p.nome = CASE WHEN size(item.nome) > size(p.nome) THEN item.nome ELSE p.nome END
MERGE (d)-[:MENCIONA_PESSOA]->(p)
"""

# Link Documento → PessoaJuridica
LINK_EMPRESA = """
MATCH (d:Documento {numero: $doc_numero})
UNWIND $items AS item
MERGE (e:PessoaJuridica {nome_normalizado: item.norm})
ON CREATE SET e.nome = item.nome
ON MATCH SET e.nome = CASE WHEN size(item.nome) > size(e.nome) THEN item.nome ELSE e.nome END
MERGE (d)-[:MENCIONA_EMPRESA]->(e)
"""

# Link Documento → existing Orgao (try to match by sigla)
LINK_ORGAO = """
MATCH (d:Documento {numero: $doc_numero})
UNWIND $nomes AS nome
MERGE (o:Orgao {sigla: nome})
MERGE (d)-[:MENCIONA_ORGAO]->(o)
"""

# Link Documento → CargoFuncao
LINK_CARGO = """
MATCH (d:Documento {numero: $doc_numero})
UNWIND $items AS item
MERGE (c:CargoFuncao {nome_normalizado: item.norm})
ON CREATE SET c.nome = item.nome
ON MATCH SET c.nome = CASE WHEN size(item.nome) > size(c.nome) THEN item.nome ELSE c.nome END
MERGE (d)-[:MENCIONA_CARGO]->(c)
"""

# Link Documento → Legislacao (lei, decreto, portaria)
LINK_LEGISLACAO = """
MATCH (d:Documento {numero: $doc_numero})
UNWIND $items AS item
MERGE (l:Legislacao {referencia: item.ref})
ON CREATE SET l.tipo = item.tipo
MERGE (d)-[:MENCIONA_LEGISLACAO]->(l)
"""

# Link Documento → ContratoEdital
LINK_CONTRATO = """
MATCH (d:Documento {numero: $doc_numero})
UNWIND $refs AS ref
MERGE (c:ContratoEdital {referencia: ref})
MERGE (d)-[:MENCIONA_CONTRATO]->(c)
"""

# Link Documento → ClasseNivel
LINK_CLASSE = """
MATCH (d:Documento {numero: $doc_numero})
UNWIND $refs AS ref
MERGE (c:ClasseNivel {referencia: ref})
MERGE (d)-[:MENCIONA_CLASSE]->(c)
"""

# ── Relation loaders ──────────────────────────────────────────────────────

# PessoaFisica -[rel]-> Documento
LINK_PESSOA_REL = """
MATCH (d:Documento {numero: $doc_numero})
UNWIND $pairs AS pair
MERGE (p:PessoaFisica {nome: pair.head})
MERGE (p)-[r:{rel_type}]->(d)
SET r.objeto = pair.tail
"""

# For relations where head is an org/entity, not a person
LINK_ENTITY_REL = """
MATCH (d:Documento {numero: $doc_numero})
UNWIND $pairs AS pair
MERGE (d)-[r:{rel_type}]->(:PessoaFisica {{nome: pair.tail}})
SET r.agente = pair.head
"""

# ── Clear previous GLiNER data ─────────────────────────────────────────────

CLEAR_GLINER = [
    # Remove relationships created by this script
    "MATCH ()-[r:MENCIONA_PESSOA]->() DELETE r",
    "MATCH ()-[r:MENCIONA_EMPRESA]->() DELETE r",
    "MATCH ()-[r:MENCIONA_ORGAO]->() WHERE NOT EXISTS { MATCH ()-[:PERTENCE_AO_ORGAO]->() } DELETE r",
    "MATCH ()-[r:MENCIONA_CARGO]->() DELETE r",
    "MATCH ()-[r:MENCIONA_LEGISLACAO]->() DELETE r",
    "MATCH ()-[r:MENCIONA_CONTRATO]->() DELETE r",
    "MATCH ()-[r:ASSINOU]->() DELETE r",
    "MATCH ()-[r:AUTORIZOU]->() DELETE r",
    "MATCH ()-[r:SOLICITOU]->() DELETE r",
    "MATCH ()-[r:CONTRATOU]->() DELETE r",
    "MATCH ()-[r:NOMEOU]->() DELETE r",
    "MATCH ()-[r:DESIGNOU]->() DELETE r",
    "MATCH ()-[r:EXONEROU]->() DELETE r",
    "MATCH ()-[r:ENCAMINHOU_PARA_DOC]->() DELETE r",
    # Remove orphan nodes created by this script
    "MATCH (p:PessoaFisica) WHERE NOT (p)--() DELETE p",
    "MATCH (p:PessoaJuridica) WHERE NOT (p)--() DELETE p",
    "MATCH (c:CargoFuncao) WHERE NOT (c)--() DELETE c",
    "MATCH (l:Legislacao) WHERE NOT (l)--() DELETE l",
    "MATCH (c:ContratoEdital) WHERE NOT (c)--() DELETE c",
    # Clear properties
    """MATCH (d:Documento) WHERE d.gliner_model IS NOT NULL
       REMOVE d.tipo_documento_gliner, d.assunto_gliner,
              d.emails_mencionados, d.cpfs_mencionados, d.cnpjs_mencionados,
              d.matriculas_mencionadas, d.telefones_mencionados, d.urls_mencionadas,
              d.valores_monetarios, d.datas_mencionadas, d.enderecos_mencionados,
              d.objeto_licitacao_gliner, d.vigencia_gliner,
              d.gliner_model, d.gliner_extracted_at""",
]


# ── Helpers ────────────────────────────────────────────────────────────────

def _texts(entities: dict, key: str) -> list[str]:
    """Extract text values from entities dict."""
    return [e["text"] for e in entities.get(key, []) if isinstance(e, dict) and e.get("text")]


def _items_with_norm(entities: dict, key: str) -> list[dict]:
    """Extract text values with normalized key for MERGE."""
    return [{"nome": e["text"], "norm": _normalize(e["text"])}
            for e in entities.get(key, []) if isinstance(e, dict) and e.get("text")]


def _first_text(entities: dict, key: str) -> str | None:
    """Get first text value or None."""
    texts = _texts(entities, key)
    return texts[0] if texts else None


def _load_relation(writer: GraphWriter, doc_numero: str, rel_type: str,
                   pairs: list, is_person_head: bool = True) -> None:
    """Emit a relation-pair template through the writer.

    Cypher is composed here because Neo4j does not allow parameterizing
    relationship types. `rel_type` is controlled by this module (never user
    input), so string interpolation is safe.
    """
    if not pairs:
        return

    clean_pairs = []
    for pair in pairs:
        if len(pair) >= 2:
            head = str(pair[0]).strip()
            tail = str(pair[1]).strip()
            clean_pairs.append({
                "head": head, "head_norm": _normalize(head),
                "tail": tail, "tail_norm": _normalize(tail),
            })

    if not clean_pairs:
        return

    if is_person_head:
        cypher = f"""
        MATCH (d:Documento {{numero: $doc_numero}})
        UNWIND $pairs AS pair
        MERGE (p:PessoaFisica {{nome_normalizado: pair.head_norm}})
        ON CREATE SET p.nome = pair.head
        MERGE (p)-[r:{rel_type}]->(d)
        SET r.objeto = pair.tail
        """
    else:
        cypher = f"""
        MATCH (d:Documento {{numero: $doc_numero}})
        UNWIND $pairs AS pair
        MERGE (p:PessoaFisica {{nome_normalizado: pair.tail_norm}})
        ON CREATE SET p.nome = pair.tail
        MERGE (d)-[r:{rel_type}]->(p)
        SET r.agente = pair.head
        """

    writer.execute_template(
        f"link_rel_{rel_type.lower()}",
        cypher,
        {"doc_numero": doc_numero, "pairs": clean_pairs},
        phase="gliner",
    )


# ── Main ───────────────────────────────────────────────────────────────────

def load_document(
    writer: GraphWriter | None,
    data: dict,
    *,
    dry_run: bool = False,
    read_driver: object | None = None,
) -> bool:
    """Route one GLiNER result into the writer. Returns True if the document
    was found (or skipped the existence check in JSON-emit mode).

    `read_driver` is an optional live Neo4j driver used solely for the
    "Documento already exists" pre-check. It is None when the writer is a
    `JsonFileWriter` (nothing to check against), in which case we emit the
    writes unconditionally and rely on the replay step to deal with missing
    documents.
    """
    doc_numero = data["documento_numero"]
    entities = data.get("entities", {})
    classification = data.get("classification", {})
    relations = data.get("relations", {})

    if dry_run:
        ent_count = sum(len(v) for v in entities.values())
        rel_count = sum(len(v) for v in relations.values())
        log.info("  [DRY-RUN] doc %s: %d entities, %d relations, tipo=%s",
                 doc_numero, ent_count, rel_count, classification.get("tipo_documento", "?"))
        return True

    assert writer is not None, "load_document called without writer outside dry-run"

    # 1. Check document exists (only when we have a live Neo4j for reads)
    if read_driver is not None:
        with read_driver.session() as session:
            result = session.run(
                "MATCH (d:Documento {numero: $n}) RETURN d.numero",
                n=doc_numero,
            ).single()
        if not result:
            log.warning("  Document %s not found in Neo4j — skipping", doc_numero)
            return False

    # 2. Set properties on Documento
    writer.execute_template(
        "set_doc_properties",
        SET_DOC_PROPERTIES,
        {
            "doc_numero": doc_numero,
            "tipo_documento": classification.get("tipo_documento"),
            "assunto": _first_text(entities, "assunto"),
            "emails": _texts(entities, "email"),
            "cpfs": _texts(entities, "cpf"),
            "cnpjs": _texts(entities, "cnpj"),
            "matriculas": _texts(entities, "matricula"),
            "telefones": _texts(entities, "telefone"),
            "urls": _texts(entities, "url"),
            "valores": _texts(entities, "valor_monetario"),
            "datas": _texts(entities, "data"),
            "enderecos": _texts(entities, "endereco"),
            "objeto_licitacao": _first_text(entities, "objeto_licitacao"),
            "vigencia": _first_text(entities, "vigencia"),
            "model": data.get("model", ""),
            "extracted_at": data.get("extracted_at", ""),
        },
        phase="gliner",
    )

    # 3. Link to entity nodes (every template is composite MERGE/MATCH/ON MATCH
    # SET — stays as execute_template)
    pessoas = _items_with_norm(entities, "pessoa")
    if pessoas:
        writer.execute_template("link_pessoa", LINK_PESSOA,
                                {"doc_numero": doc_numero, "items": pessoas},
                                phase="gliner")

    empresas = _items_with_norm(entities, "pessoa_juridica")
    if empresas:
        writer.execute_template("link_empresa", LINK_EMPRESA,
                                {"doc_numero": doc_numero, "items": empresas},
                                phase="gliner")

    orgaos = _texts(entities, "orgao")
    if orgaos:
        writer.execute_template("link_orgao", LINK_ORGAO,
                                {"doc_numero": doc_numero, "nomes": orgaos},
                                phase="gliner")

    cargos = _items_with_norm(entities, "cargo")
    if cargos:
        writer.execute_template("link_cargo", LINK_CARGO,
                                {"doc_numero": doc_numero, "items": cargos},
                                phase="gliner")

    # Legislação: merge lei + decreto + portaria into typed Legislacao nodes
    leg_items = []
    for ref in _texts(entities, "lei"):
        leg_items.append({"ref": ref, "tipo": "lei"})
    for ref in _texts(entities, "decreto"):
        leg_items.append({"ref": ref, "tipo": "decreto"})
    for ref in _texts(entities, "portaria"):
        leg_items.append({"ref": ref, "tipo": "portaria"})
    if leg_items:
        writer.execute_template("link_legislacao", LINK_LEGISLACAO,
                                {"doc_numero": doc_numero, "items": leg_items},
                                phase="gliner")

    contratos = _texts(entities, "contrato_edital")
    if contratos:
        writer.execute_template("link_contrato", LINK_CONTRATO,
                                {"doc_numero": doc_numero, "refs": contratos},
                                phase="gliner")

    # 4. Load extracted relations
    PERSON_HEAD_RELS = {"assinou": "ASSINOU", "autorizou": "AUTORIZOU",
                        "solicitou": "SOLICITOU", "contratou": "CONTRATOU"}
    ENTITY_HEAD_RELS = {"nomeou": "NOMEOU", "designou": "DESIGNOU",
                        "exonerou": "EXONEROU", "encaminhou_para": "ENCAMINHOU_PARA_DOC"}

    for rel_key, rel_type in PERSON_HEAD_RELS.items():
        _load_relation(writer, doc_numero, rel_type, relations.get(rel_key, []), is_person_head=True)

    for rel_key, rel_type in ENTITY_HEAD_RELS.items():
        _load_relation(writer, doc_numero, rel_type, relations.get(rel_key, []), is_person_head=False)

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Load GLiNER2 extraction results into Neo4j"
    )
    parser.add_argument("--input", default="./ner_results",
                        help="Directory with GLiNER JSON outputs (default: ./ner_results)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Only show what would be loaded")
    parser.add_argument("--clear-first", action="store_true",
                        help="Remove all previous GLiNER data before loading")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit number of files to process (0=all)")
    # --read-json is meaningless here (this script reads its own --input
    # NER JSON files), so it's omitted from the standard flag group.
    add_standard_args(parser, skip={"--read-json"})
    args = parser.parse_args()

    settings = resolve_settings(args)
    configure_logging(__name__, settings.log_level)

    input_dir = Path(args.input)
    if not input_dir.exists():
        log.error("Input directory not found: %s", input_dir)
        sys.exit(1)

    files = sorted(f for f in input_dir.iterdir() if f.suffix == ".json")
    if args.limit > 0:
        files = files[:args.limit]

    log.info("Found %d GLiNER result files in %s", len(files), input_dir)

    if not files:
        log.info("Nothing to load.")
        return

    # -- Resolve writer / driver --------------------------------------------
    #
    # Three modes:
    #   1. --dry-run             : no writer, no driver; load_document logs only.
    #   2. --emit-json DIR       : writer = JsonFileWriter(DIR). No Neo4j
    #                              connection, no Documento existence check.
    #   3. default               : writer = DirectNeo4jWriter, driver connected,
    #                              per-doc existence check enabled.
    driver = None
    writer: GraphWriter | None = None

    if args.dry_run:
        log.info("Dry run: no writes, no emit")
    elif settings.emit_json_dir is not None:
        log.info("Emitting GLiNER writes to %s", settings.emit_json_dir)
        writer = JsonFileWriter(settings.emit_json_dir)
    else:
        try:
            driver = build_driver(settings)
            log.info("Connected to Neo4j: %s", settings.neo4j_uri)
        except ConfigError as e:
            log.error("%s", e)
            sys.exit(2)
        except Exception as e:
            log.error("Failed to connect to Neo4j at %s: %s", settings.neo4j_uri, e)
            sys.exit(1)
        writer = DirectNeo4jWriter(driver, batch_size=settings.batch_size or 1000)

    # -- Run -----------------------------------------------------------------
    try:
        if writer is not None:
            writer.open_phase("gliner")

            log.info("Creating constraints...")
            for cypher in SETUP_CONSTRAINTS:
                try:
                    writer.execute_template("gliner_constraint", cypher, {}, phase="gliner")
                except Exception as e:
                    log.debug("Constraint: %s", e)

            if args.clear_first:
                log.info("Clearing previous GLiNER data...")
                for cypher in CLEAR_GLINER:
                    writer.execute_template("gliner_clear", cypher, {}, phase="gliner")
                log.info("Cleared.")

        loaded = 0
        skipped = 0
        not_found = 0

        for i, filepath in enumerate(files):
            log.info("[%d/%d] Loading %s", i + 1, len(files), filepath.name)
            try:
                data = json.loads(filepath.read_text(encoding="utf-8"))
                found = load_document(writer, data, dry_run=args.dry_run, read_driver=driver)
                if found:
                    loaded += 1
                else:
                    not_found += 1
            except Exception as e:
                log.error("  ERROR loading %s: %s", filepath.name, e)
                skipped += 1

        if writer is not None:
            writer.close_phase("gliner")

        log.info(
            "DONE. Loaded: %d | Not found in Neo4j: %d | Errors: %d | Total: %d",
            loaded, not_found, skipped, len(files),
        )
    finally:
        if writer is not None:
            writer.close()
        if driver is not None:
            driver.close()


if __name__ == "__main__":
    main()
