"""Cypher templates used by the ETL phases.

Pure-string constants only — no imports, no logic. Phase modules import
these and feed them to `writer.execute_template`. Schema constraints are
also defined here so they can be replayed identically by JSON emitters.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Schema: constraints + indexes
# ---------------------------------------------------------------------------
SETUP_CONSTRAINTS: list[str] = [
    "CREATE CONSTRAINT processo_protocolo IF NOT EXISTS FOR (p:Processo) REQUIRE p.protocolo_formatado IS UNIQUE",
    "CREATE CONSTRAINT atividade_source_id IF NOT EXISTS FOR (a:Atividade) REQUIRE a.source_id IS UNIQUE",
    "CREATE CONSTRAINT unidade_sigla IF NOT EXISTS FOR (u:Unidade) REQUIRE u.sigla IS UNIQUE",
    "CREATE CONSTRAINT tipo_procedimento_nome IF NOT EXISTS FOR (tp:TipoProcedimento) REQUIRE tp.nome IS UNIQUE",
    "CREATE CONSTRAINT tipo_acao_chave IF NOT EXISTS FOR (ta:TipoAcao) REQUIRE ta.chave IS UNIQUE",
    "CREATE CONSTRAINT grupo_atividade_chave IF NOT EXISTS FOR (ga:GrupoAtividade) REQUIRE ga.chave IS UNIQUE",
    "CREATE CONSTRAINT orgao_sigla IF NOT EXISTS FOR (o:Orgao) REQUIRE o.sigla IS UNIQUE",
    "CREATE CONSTRAINT usuario_nome IF NOT EXISTS FOR (u:Usuario) REQUIRE u.nome IS UNIQUE",
    "CREATE CONSTRAINT documento_numero IF NOT EXISTS FOR (d:Documento) REQUIRE d.numero IS UNIQUE",
    "CREATE INDEX atividade_data IF NOT EXISTS FOR (a:Atividade) ON (a.data_hora)",
    "CREATE INDEX processo_data_criacao IF NOT EXISTS FOR (p:Processo) ON (p.data_criacao)",
    "CREATE INDEX atividade_seq IF NOT EXISTS FOR (a:Atividade) ON (a.seq)",
    "CREATE CONSTRAINT ciclo_id IF NOT EXISTS FOR (c:Ciclo) REQUIRE c.id IS UNIQUE",
    "CREATE INDEX ciclo_status IF NOT EXISTS FOR (c:Ciclo) ON (c.status)",
    "CREATE INDEX ciclo_entrada IF NOT EXISTS FOR (c:Ciclo) ON (c.entrada)",
    "CREATE INDEX processo_situacao IF NOT EXISTS FOR (p:Processo) ON (p.situacao)",
    # Vector index for document embeddings (Neo4j 5+; cosine similarity).
    # Dimension is fixed at index creation — switching to a different model
    # with a different vector size requires DROPing this index first.
    """CREATE VECTOR INDEX documento_embedding_idx IF NOT EXISTS
       FOR (d:Documento) ON d.embedding
       OPTIONS { indexConfig: {
           `vector.dimensions`: 1024,
           `vector.similarity_function`: 'cosine'
       }}""",
    # Indexes on SIMILAR_DOC properties for tier-/scope-filtered queries.
    "CREATE INDEX similar_doc_tier IF NOT EXISTS FOR ()-[r:SIMILAR_DOC]-() ON (r.tier)",
    "CREATE INDEX similar_doc_intra IF NOT EXISTS FOR ()-[r:SIMILAR_DOC]-() ON (r.intra_processo)",
    # Categoria emergente (stage processo-cluster)
    "CREATE CONSTRAINT categoria_processo_id IF NOT EXISTS FOR (c:CategoriaProcesso) REQUIRE c.id IS UNIQUE",
    "CREATE INDEX categoria_processo_nivel IF NOT EXISTS FOR (c:CategoriaProcesso) ON (c.nivel)",
]

# ---------------------------------------------------------------------------
# Phase A: composite templates (MERGE + chained MATCH/MERGE).
# Pure node and pure edge writes go through writer.write_nodes /
# writer.write_edges and don't need explicit Cypher here.
# ---------------------------------------------------------------------------
SEED_TIPOS_CYPHER = """
UNWIND $types AS t
MERGE (ta:TipoAcao {chave: t.chave})
WITH ta, t
MATCH (ga:GrupoAtividade {chave: t.grupo})
MERGE (ta)-[:PERTENCE_AO_GRUPO]->(ga)
"""

PRECREATE_PROCESSOS_CYPHER = """
UNWIND $rows AS r
MERGE (p:Processo {protocolo_formatado: r.protocolo_formatado})
ON CREATE SET p.data_criacao = datetime(r.data_criacao)
WITH p, r
WHERE r.tipo_procedimento IS NOT NULL
MERGE (tp:TipoProcedimento {nome: r.tipo_procedimento})
MERGE (p)-[:TEM_TIPO]->(tp)
"""

PRECREATE_USUARIOS_CYPHER = """
UNWIND $users AS u
MERGE (usr:Usuario {nome: u.nome})
WITH usr, u
MATCH (o:Orgao {sigla: u.orgao})
MERGE (usr)-[:PERTENCE_AO_ORGAO]->(o)
"""

# ---------------------------------------------------------------------------
# Phase B: composite load statements
# ---------------------------------------------------------------------------
LOAD_ATIVIDADES_CYPHER = """
UNWIND $rows AS row

MATCH (proc:Processo {protocolo_formatado: row.protocolo_formatado})
MATCH (uni:Unidade {sigla: row.unidade})
MATCH (ta:TipoAcao {chave: row.tipo_acao})
MATCH (usr:Usuario {nome: row.usuario})

MERGE (atv:Atividade {source_id: row.source_id})
ON CREATE SET
    atv.data_hora = datetime(row.data_hora),
    atv.descricao = row.descricao,
    atv.tipo_acao = row.tipo_acao,
    atv.grupo = row.grupo,
    atv.ref_id = row.ref_id,
    atv.seq = row.seq

MERGE (atv)-[:DO_PROCESSO]->(proc)
MERGE (atv)-[:EXECUTADO_PELA_UNIDADE]->(uni)
MERGE (atv)-[:TIPO_ACAO]->(ta)
MERGE (atv)-[:EXECUTADO_PELO_USUARIO]->(usr)
"""

LOAD_DOCUMENTO_CYPHER = """
UNWIND $rows AS row
MATCH (atv:Atividade {source_id: row.source_id})
MERGE (doc:Documento {numero: row.numero})
ON CREATE SET doc.tipo = row.tipo,
              doc.serie_id = row.serie_id
MERGE (atv)-[:REFERENCIA_DOCUMENTO]->(doc)
WITH atv, doc
MATCH (atv)-[:DO_PROCESSO]->(p:Processo)
MERGE (p)-[:CONTEM_DOCUMENTO]->(doc)
"""

# ---------------------------------------------------------------------------
# Phase C: timeline DAG
# ---------------------------------------------------------------------------
LOAD_TIMELINE_CYPHER = """
UNWIND $edges AS e
MATCH (a1:Atividade {source_id: e.from_id})
MATCH (a2:Atividade {source_id: e.to_id})
MERGE (a1)-[r:SEGUIDA_POR]->(a2)
SET r.mesma_unidade = e.mesma_unidade,
    r.intervalo_horas = e.intervalo_horas,
    r.intervalo_dias = e.intervalo_dias
MERGE (a2)-[r2:PRECEDIDA_POR]->(a1)
SET r2.mesma_unidade = e.mesma_unidade,
    r2.intervalo_horas = e.intervalo_horas,
    r2.intervalo_dias = e.intervalo_dias
"""

# ---------------------------------------------------------------------------
# Phase D: permanência
# ---------------------------------------------------------------------------
LOAD_PERMANENCIA_UNIDADE_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (u:Unidade {sigla: r.unidade})
MERGE (p)-[rel:PASSOU_PELA_UNIDADE]->(u)
SET rel.duracao_total_horas = r.duracao_total_horas,
    rel.duracao_acumulada_horas = r.duracao_acumulada_horas,
    rel.duracao_lifetime_horas = r.duracao_lifetime_horas,
    rel.visitas = r.visitas,
    rel.primeira_entrada = datetime(r.primeira_entrada),
    rel.ultima_saida = datetime(r.ultima_saida)
"""

LOAD_PERMANENCIA_ORGAO_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (o:Orgao {sigla: r.orgao})
MERGE (p)-[rel:PASSOU_PELO_ORGAO]->(o)
SET rel.duracao_total_horas = r.duracao_total_horas,
    rel.duracao_acumulada_horas = r.duracao_acumulada_horas,
    rel.duracao_lifetime_horas = r.duracao_lifetime_horas,
    rel.visitas = r.visitas,
    rel.primeira_entrada = datetime(r.primeira_entrada),
    rel.ultima_saida = datetime(r.ultima_saida)
"""


# ---------------------------------------------------------------------------
# Stage `situacao`: cycles + open/closed state edges
# ---------------------------------------------------------------------------

# Idempotency: before re-emitting cycles + sparse edges for a batch of
# processos, drop existing :Ciclo nodes and ephemeral edges scoped to those
# processos. Full-state edges (SITUACAO_*, PASSOU_*) use MERGE+SET.
CLEAR_SITUACAO_FOR_PROTOCOLOS_CYPHER = """
UNWIND $protocolos AS pf
MATCH (p:Processo {protocolo_formatado: pf})
OPTIONAL MATCH (p)<-[:DO_PROCESSO]-(c:Ciclo)
OPTIONAL MATCH (p)-[r1:EM_ABERTO_NA_UNIDADE]->()
OPTIONAL MATCH (p)-[r2:EM_ABERTO_NO_ORGAO]->()
OPTIONAL MATCH (p)-[r3:ULTIMA_ATIVIDADE_EM_ABERTO]->()
OPTIONAL MATCH (p)-[r4:ATIVIDADE_MAIS_RECENTE]->()
DETACH DELETE c
DELETE r1, r2, r3, r4
"""

# Full per-(P,U) state record. Always present for every unit the processo
# touched. Frontend reads ``situacao`` here and joins to (Unidade) for sigla.
LOAD_SITUACAO_UNIDADE_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (u:Unidade {sigla: r.unidade})
MERGE (p)-[rel:SITUACAO_PROCESSO_UNIDADE]->(u)
SET rel.situacao = r.situacao,
    rel.duracao_acumulada_horas = r.duracao_acumulada_horas,
    rel.duracao_lifetime_horas = r.duracao_lifetime_horas,
    rel.visitas = r.visitas,
    rel.primeira_entrada = datetime(r.primeira_entrada),
    rel.ultima_saida = CASE WHEN r.ultima_saida IS NULL THEN null ELSE datetime(r.ultima_saida) END,
    rel.ultima_atividade_id = r.ultima_atividade_id,
    rel.ultima_atividade_data_hora = CASE WHEN r.ultima_atividade_data_hora IS NULL THEN null ELSE datetime(r.ultima_atividade_data_hora) END,
    rel.ultima_atividade_tipo_acao = r.ultima_atividade_tipo_acao,
    rel.dias_sem_atividade = r.dias_sem_atividade
"""

# Sparse edge — only emitted when the unit is currently open. Lets the UI
# do `MATCH (u)<-[:EM_ABERTO_NA_UNIDADE]-(p)` without scanning property values.
LOAD_EM_ABERTO_UNIDADE_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (u:Unidade {sigla: r.unidade})
MERGE (p)-[rel:EM_ABERTO_NA_UNIDADE]->(u)
SET rel.desde = datetime(r.desde),
    rel.dias_sem_atividade = r.dias_sem_atividade
"""

LOAD_SITUACAO_ORGAO_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (o:Orgao {sigla: r.orgao})
MERGE (p)-[rel:SITUACAO_PROCESSO_ORGAO]->(o)
SET rel.situacao = r.situacao,
    rel.unidades_abertas_count = r.unidades_abertas_count,
    rel.duracao_acumulada_horas = r.duracao_acumulada_horas,
    rel.duracao_lifetime_horas = r.duracao_lifetime_horas,
    rel.dias_sem_atividade = r.dias_sem_atividade
"""

LOAD_EM_ABERTO_ORGAO_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (o:Orgao {sigla: r.orgao})
MERGE (p)-[rel:EM_ABERTO_NO_ORGAO]->(o)
SET rel.desde = datetime(r.desde),
    rel.unidades_abertas_count = r.unidades_abertas_count
"""

# Promote cycles to first-class :Ciclo nodes with stable composite IDs so
# detailed queries are possible: "all cycles longer than 30 days that
# concluded between Jan and Mar". A single template handles open + closed
# cycles via OPTIONAL FECHADO_POR.
LOAD_CICLOS_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (u:Unidade {sigla: r.unidade})
MERGE (c:Ciclo {id: r.id})
SET c.ordem = r.ordem,
    c.entrada = datetime(r.entrada),
    c.saida = CASE WHEN r.saida IS NULL THEN null ELSE datetime(r.saida) END,
    c.duracao_horas = r.duracao_horas,
    c.status = r.status,
    c.implicit_close = r.implicit_close
MERGE (c)-[:DO_PROCESSO]->(p)
MERGE (c)-[:NA_UNIDADE]->(u)
WITH c, r
WHERE r.abertura_atividade_id IS NOT NULL
MATCH (a_open:Atividade {source_id: r.abertura_atividade_id})
MERGE (c)-[:ABERTO_POR]->(a_open)
"""

LOAD_CICLOS_FECHADO_POR_CYPHER = """
UNWIND $rows AS r
MATCH (c:Ciclo {id: r.id})
MATCH (a_close:Atividade {source_id: r.conclusao_atividade_id})
MERGE (c)-[:FECHADO_POR]->(a_close)
"""

# One per (processo, unidade aberta) — points to the most recent
# significant activity on that open unit. Useful for "where is this
# processo parado now?" answer.
LOAD_ULTIMA_ATIVIDADE_EM_ABERTO_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (a:Atividade {source_id: r.atividade_id})
MERGE (p)-[:ULTIMA_ATIVIDADE_EM_ABERTO]->(a)
"""

# One per processo — most recent activity overall, regardless of state.
LOAD_ATIVIDADE_MAIS_RECENTE_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (a:Atividade {source_id: r.atividade_id})
MERGE (p)-[:ATIVIDADE_MAIS_RECENTE]->(a)
"""

# Set processo-level rollup properties.
LOAD_PROCESSO_SITUACAO_PROPS_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
SET p.situacao = r.situacao,
    p.unidades_em_aberto_count = r.unidades_em_aberto_count,
    p.unidades_em_aberto_siglas = r.unidades_em_aberto_siglas,
    p.data_inicio = CASE WHEN r.data_inicio IS NULL THEN null ELSE datetime(r.data_inicio) END,
    p.data_ultima_atividade = CASE WHEN r.data_ultima_atividade IS NULL THEN null ELSE datetime(r.data_ultima_atividade) END,
    p.data_conclusao_global = CASE WHEN r.data_conclusao_global IS NULL THEN null ELSE datetime(r.data_conclusao_global) END,
    p.duracao_lifetime_horas = r.duracao_lifetime_horas,
    p.duracao_lifetime_dias = r.duracao_lifetime_dias,
    p.situacao_computed_at = datetime(r.situacao_computed_at)
"""


# ---------------------------------------------------------------------------
# Stages `embed` + `similarity`: vector embeddings + semantic similarity
# ---------------------------------------------------------------------------

# Read existing hashes so the embed stage can skip docs whose text + model
# combination has already been embedded.
CHECK_EMBEDDING_HASH_CYPHER = """
UNWIND $numeros AS n
MATCH (d:Documento {numero: n})
RETURN d.numero AS numero, d.embedding_hash AS hash, d.embedding_modelo AS modelo
"""

# Persist embeddings + provenance metadata onto the Documento node. The
# vector index ``documento_embedding_idx`` (declared in SETUP_CONSTRAINTS)
# picks up the property automatically.
LOAD_DOCUMENTO_EMBEDDING_CYPHER = """
UNWIND $rows AS r
MATCH (d:Documento {numero: r.documento_numero})
SET d.embedding = r.embedding,
    d.embedding_modelo = r.modelo,
    d.embedding_hash = r.hash,
    d.embedding_dim = r.dim,
    d.embedding_computed_at = datetime(r.computed_at)
"""

# Idempotency for the similarity stage: drop existing SIMILAR_DOC edges
# originating from the docs we are about to recompute. Inbound edges from
# other docs are dropped on the other side's run.
CLEAR_SIMILAR_DOC_FOR_BATCH_CYPHER = """
UNWIND $numeros AS n
MATCH (a:Documento {numero: n})-[r:SIMILAR_DOC]->()
DELETE r
"""

# Cross-corpus top-K via Neo4j vector index. ``$exclude`` is the source doc
# itself (so it doesn't show up among its own neighbors).
QUERY_VECTOR_NEIGHBORS_CYPHER = """
CALL db.index.vector.queryNodes('documento_embedding_idx', $k_plus_one, $query_embedding)
YIELD node, score
WHERE node.numero <> $exclude
RETURN node.numero AS numero, score
ORDER BY score DESC
LIMIT $k
"""

# For the intra-processo pass we need every (Documento, Processo, embedding)
# tuple at once; processing happens in Python because Neo4j cosine across
# arbitrary pairs in Cypher is more verbose than Python vector ops.
LIST_DOCS_PER_PROCESSO_CYPHER = """
MATCH (p:Processo)-[:CONTEM_DOCUMENTO]->(d:Documento)
WHERE d.embedding IS NOT NULL
WITH p, collect({numero: d.numero, embedding: d.embedding}) AS docs
WHERE size(docs) > 1
RETURN p.protocolo_formatado AS protocolo, docs
"""

# Single edge type with classification props. Always emit only when
# from_doc < to_doc lexicographically so each undirected pair appears once.
LOAD_SIMILAR_DOC_CYPHER = """
UNWIND $rows AS r
MATCH (a:Documento {numero: r.from_doc})
MATCH (b:Documento {numero: r.to_doc})
MERGE (a)-[rel:SIMILAR_DOC]->(b)
SET rel.score = r.score,
    rel.tier = r.tier,
    rel.intra_processo = r.intra_processo,
    rel.modelo = r.modelo,
    rel.computed_at = datetime(r.computed_at)
"""


# ---------------------------------------------------------------------------
# Stages `processo-embed` + `processo-cluster`: hierarchical clustering
# ---------------------------------------------------------------------------

# Read raw signals for one batch of processos: documentos with embedding,
# unidades visited (set + chronological), atividades tipo_acao histogram.
# Returns enough info for `processo_features.build_batch_features` to do
# its work without further graph hits.
LIST_PROCESSOS_FOR_CLUSTERING_CYPHER = """
MATCH (p:Processo)
OPTIONAL MATCH (p)<-[:DO_PROCESSO]-(a:Atividade)
WITH p,
     [a IN collect(DISTINCT a) WHERE a IS NOT NULL] AS atividades

// Documents in chronological order (by min(atividade.data_hora) referencing)
OPTIONAL MATCH (p)<-[:DO_PROCESSO]-(a2:Atividade)-[:REFERENCIA_DOCUMENTO]->(d:Documento)
WHERE d.embedding IS NOT NULL
WITH p, atividades, d, min(a2.data_hora) AS first_seen
WITH p, atividades,
     [item IN collect({d: d, ts: first_seen}) WHERE item.d IS NOT NULL]
       AS doc_items_unsorted

// Sort docs by first_seen ASC
WITH p, atividades, doc_items_unsorted,
     apoc.coll.sortMaps(doc_items_unsorted, '^ts') AS doc_items

// Unidades visitadas (set) — MATCH the property directly on Atividade nodes
OPTIONAL MATCH (a3:Atividade)-[:DO_PROCESSO]->(p)
              -[:CONTEM_DOCUMENTO]-()  // dummy to ensure pattern; ignore
RETURN p.protocolo_formatado AS protocolo,
       p.cluster_features_hash AS cluster_features_hash,
       [item IN doc_items |
           {numero: item.d.numero,
            embedding: item.d.embedding,
            embedding_hash: coalesce(item.d.embedding_hash, '')}
       ] AS documentos,
       [a IN atividades WHERE a.tipo_acao IS NOT NULL | a.tipo_acao] AS atividades_tipo_acao,
       [a IN atividades | a.unidade] AS unidades_visitadas_raw,
       [a IN atividades WHERE a.data_hora IS NOT NULL | {u: a.unidade, ts: a.data_hora}] AS atividades_cronologicas
"""

# Tighter version that doesn't depend on apoc.coll.sortMaps — uses pure Cypher.
# We do the sorting Python-side for portability (the stage runner handles it).
LIST_PROCESSOS_FOR_CLUSTERING_CYPHER = """
MATCH (p:Processo)
OPTIONAL MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
WITH p, collect(a) AS atividades
OPTIONAL MATCH (a2:Atividade)-[:DO_PROCESSO]->(p)
WHERE a2.unidade IS NOT NULL
WITH p, atividades,
     [a IN collect(DISTINCT {u: a2.unidade, ts: a2.data_hora})
        WHERE a.u IS NOT NULL] AS unidade_events
OPTIONAL MATCH (p)<-[:DO_PROCESSO]-(a3:Atividade)-[:REFERENCIA_DOCUMENTO]->(d:Documento)
WHERE d.embedding IS NOT NULL
WITH p, atividades, unidade_events,
     collect(DISTINCT {numero: d.numero,
                       embedding: d.embedding,
                       embedding_hash: coalesce(d.embedding_hash, ''),
                       first_seen: a3.data_hora}) AS docs_unsorted
RETURN p.protocolo_formatado AS protocolo,
       p.cluster_features_hash AS cluster_features_hash,
       docs_unsorted AS documentos_raw,
       [a IN atividades WHERE a.tipo_acao IS NOT NULL | a.tipo_acao] AS atividades_tipo_acao,
       [ev IN unidade_events | ev.u] AS unidades_visitadas,
       unidade_events AS unidade_events_raw
"""

# Persist the UMAP-reduced cluster_features on the :Processo node.
LOAD_PROCESSO_CLUSTER_FEATURES_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
SET p.cluster_features = r.features,
    p.cluster_features_dim = r.dim,
    p.cluster_features_hash = r.hash,
    p.cluster_features_computed_at = datetime(r.computed_at)
"""

# Read all processo cluster_features (used as the input matrix to HDBSCAN).
READ_ALL_CLUSTER_FEATURES_CYPHER = """
MATCH (p:Processo)
WHERE p.cluster_features IS NOT NULL
RETURN p.protocolo_formatado AS protocolo,
       p.cluster_features AS features
"""

# Wipe :CategoriaProcesso + edges before re-emitting (run-scoped substitution).
CLEAR_CATEGORIA_PROCESSO_CYPHER = """
MATCH (c:CategoriaProcesso)
DETACH DELETE c
"""

# Create :CategoriaProcesso nodes (macro + micro + reclassificada).
LOAD_CATEGORIA_PROCESSO_CYPHER = """
UNWIND $rows AS r
MERGE (c:CategoriaProcesso {id: r.id})
SET c.nome = r.nome,
    c.descricao = r.descricao,
    c.marcadores = r.marcadores,
    c.nivel = r.nivel,
    c.n_processos = r.n_processos,
    c.centroid = r.centroid,
    c.computed_at = datetime(r.computed_at)
"""

# Hierarchy: micro → SUBCATEGORIA_DE → macro.
LOAD_SUBCATEGORIA_DE_CYPHER = """
UNWIND $rows AS r
MATCH (child:CategoriaProcesso {id: r.child_id})
MATCH (parent:CategoriaProcesso {id: r.parent_id})
MERGE (child)-[:SUBCATEGORIA_DE]->(parent)
"""

# Edge processo → categoria. One edge per (processo, level=macro|micro).
LOAD_CATEGORIZADO_COMO_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (c:CategoriaProcesso {id: r.categoria_id})
MERGE (p)-[rel:CATEGORIZADO_COMO {nivel: r.nivel}]->(c)
SET rel.reclassificada = r.reclassificada,
    rel.confidence = r.confidence,
    rel.computed_at = datetime(r.computed_at)
"""

# Sample N processos for LLM naming context. Returns each one's resumo (if
# any), top-3 unidades by permanência, top-3 tipos_acao.
SAMPLE_PROCESSOS_FOR_NAMING_CYPHER = """
UNWIND $protocolos AS pf
MATCH (p:Processo {protocolo_formatado: pf})
OPTIONAL MATCH (p)-[r_uni:PASSOU_PELA_UNIDADE]->(u:Unidade)
WITH p, u, r_uni
ORDER BY r_uni.duracao_acumulada_horas DESC
WITH p, collect(u.sigla)[..3] AS top_unidades

OPTIONAL MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
WITH p, top_unidades, a.tipo_acao AS tipo, count(*) AS cnt
ORDER BY cnt DESC
WITH p, top_unidades, collect(tipo)[..3] AS top_tipos_acao

OPTIONAL MATCH (p)<-[:DO_PROCESSO]-(a_first:Atividade)-[:REFERENCIA_DOCUMENTO]->(d:Documento)
WITH p, top_unidades, top_tipos_acao, d, a_first.data_hora AS data_hora
ORDER BY data_hora ASC
WITH p, top_unidades, top_tipos_acao, head(collect(d)) AS first_doc

RETURN p.protocolo_formatado AS protocolo,
       coalesce(first_doc.assunto_gliner, '') AS resumo,
       coalesce(first_doc.tipo, '') AS primeiro_doc_texto,
       top_unidades,
       top_tipos_acao
"""
