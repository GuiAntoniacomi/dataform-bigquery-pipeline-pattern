"""Shared pytest fixtures and path setup for the test suite."""

from __future__ import annotations

import sys
from pathlib import Path

# Make `scripts/` importable as a package without installing.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
