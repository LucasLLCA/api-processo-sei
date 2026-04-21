"""Text normalization helpers shared across extract_ner_gliner2 and load_gliner_to_neo4j.

Behavior is intentionally bit-identical to the private `_normalize()` functions
that previously existed in both scripts, so replacing them is a no-op.
"""

from __future__ import annotations

import unicodedata


def strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def collapse_whitespace(text: str) -> str:
    return " ".join(text.split())


def normalize(text: str) -> str:
    """Strip accents, lowercase, and collapse whitespace.

    Preserves the exact semantics of the previous `_normalize()` helpers.
    """
    return collapse_whitespace(strip_accents(text).lower())
