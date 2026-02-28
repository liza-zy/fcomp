from pathlib import Path
import os


# -------------------------------------------------
# Base project root (fincompass/)
# -------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]


# -------------------------------------------------
# Data lake folder
# -------------------------------------------------

DATA_LAKE_DIR = PROJECT_ROOT / "data_lake"
DATA_LAKE_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------
# DuckDB file path
# -------------------------------------------------

DEFAULT_DUCKDB_PATH = DATA_LAKE_DIR / "moex.duckdb"

DUCKDB_PATH = Path(
    os.getenv("DUCKDB_PATH", str(DEFAULT_DUCKDB_PATH))
)


# -------------------------------------------------
# MOEX base URL
# -------------------------------------------------

MOEX_BASE_URL = os.getenv(
    "MOEX_BASE_URL",
    "https://iss.moex.com/iss"
)


def print_settings():
    print("=== Data Pipeline Settings ===")
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"DATA_LAKE_DIR: {DATA_LAKE_DIR}")
    print(f"DUCKDB_PATH: {DUCKDB_PATH}")
    print(f"MOEX_BASE_URL: {MOEX_BASE_URL}")