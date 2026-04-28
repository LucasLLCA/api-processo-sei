"""Pipeline stages — runnable units that produce/consume graph data.

Importing this package registers every stage with ``pipeline.registry`` so
that ``registry.all_stages()`` returns the full catalog. New stages just
need to be added to the import list below and decorate their ``run``
function with ``@stage(StageMeta(...))``.

Top-level access:
    pipeline run <stage> [<stage>...]  # via Typer CLI
    python -m pipeline                  # interactive wizard
"""

# Order doesn't matter for registration, but grouping keeps the file readable.
from . import (  # noqa: F401  -- imports trigger @stage registration
    precreate,
    atividades,
    timeline,
    permanencia,
    situacao,
    download,
    parse,
    ner_extract,
    ner_load,
    embed,
    similarity,
    processo_embed,
    processo_cluster,
    clean,
    resumo,
    _bootstrap,
)
