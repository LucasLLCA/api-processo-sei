"""SQL queries for the SEI postgres source.

Kept separate from `cypher.py` so SQL changes don't bleed into the graph
schema. Templates use psycopg2 `%s` placeholders; FIND_PROCESSOS_SQL has a
`{date_filter}` slot that callers fill via `str.format`.
"""

from __future__ import annotations


FIND_PROCESSOS_SQL = """
    SELECT protocolo_formatado, data_hora, unidade, usuario, tipo_procedimento
    FROM sei_processo.sei_atividades
    WHERE descricao_replace LIKE 'Processo %%gerado%%'
    {date_filter}
    ORDER BY data_hora
"""

FETCH_ANDAMENTOS_SQL = """
    SELECT id, protocolo_formatado, data_hora,
           unidade, usuario, tipo_procedimento, descricao_replace
    FROM sei_processo.sei_atividades
    WHERE protocolo_formatado = ANY(%s)
    ORDER BY protocolo_formatado, data_hora
"""

FIND_USUARIOS_SQL = """
    SELECT usuario, unidade, COUNT(*) AS cnt
    FROM sei_processo.sei_atividades
    WHERE protocolo_formatado = ANY(%s)
      AND usuario IS NOT NULL
    GROUP BY usuario, unidade
    ORDER BY usuario, cnt DESC
"""

FIND_DISTINCT_UNIDADES_SQL = """
    SELECT DISTINCT unidade
    FROM sei_processo.sei_atividades
    WHERE protocolo_formatado = ANY(%s) AND unidade IS NOT NULL
"""

FIND_REMETIDO_DESCRICOES_SQL = """
    SELECT DISTINCT descricao_replace
    FROM sei_processo.sei_atividades
    WHERE protocolo_formatado = ANY(%s)
      AND descricao_replace LIKE 'Processo remetido%%'
"""

COUNT_ATIVIDADES_FOR_IDS_SQL = (
    "SELECT COUNT(*) FROM sei_processo.sei_atividades "
    "WHERE protocolo_formatado = ANY(%s)"
)
