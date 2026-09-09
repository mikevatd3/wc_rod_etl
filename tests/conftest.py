"""Make the repo root importable so the tests can reach transforms.py.

transforms.py is deliberately importable on its own -- main.py is not, because
it imports entity_analyze and builds a database engine at import time.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
