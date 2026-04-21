"""Pluggable graph writers.

`GraphWriter` is the abstract seam that every ETL script routes its writes
through. Two implementations:

- `DirectNeo4jWriter` — runs generated Cypher against a live Neo4j driver.
- `JsonFileWriter` — stub for NDJSON emission; implemented in a later step.
"""

from .base import GraphWriter, WriterError
from .json_writer import JsonFileWriter
from .neo4j_writer import DirectNeo4jWriter

__all__ = ["GraphWriter", "WriterError", "DirectNeo4jWriter", "JsonFileWriter"]
