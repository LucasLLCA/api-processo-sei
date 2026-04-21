"""Pluggable graph readers.

Mirror of the writer package. `GraphReader` is the seam Phase C / D go
through to fetch processo timelines; two implementations will land:

- `Neo4jReader` — runs Cypher against a live driver (step 9).
- `JsonFileReader` — replays a `JsonFileWriter` emit directory (step 10).
"""

from .base import GraphReader, ProcessoTimeline, ReaderError
from .json_reader import JsonFileReader
from .neo4j_reader import Neo4jReader

__all__ = [
    "GraphReader",
    "ProcessoTimeline",
    "ReaderError",
    "Neo4jReader",
    "JsonFileReader",
]
