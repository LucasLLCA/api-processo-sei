"""Make `pipeline` importable when pytest is run from the repo root.

The pipeline package lives at scripts/pipeline/, so we add scripts/ to sys.path.
"""

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[2]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
