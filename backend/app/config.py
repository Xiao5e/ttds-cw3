from pathlib import Path

# Project root: .../backend
BACKEND_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = BACKEND_ROOT.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
INDEX_DIR = DATA_DIR / "index"
SOURCES_DIR = PROJECT_ROOT / "sources"

# Minimal demo setting
DEFAULT_TOP_K = 10
