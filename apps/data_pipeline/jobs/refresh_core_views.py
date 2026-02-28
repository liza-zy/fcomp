# apps/data_pipeline/jobs/refresh_core_views.py
from __future__ import annotations

import argparse
import duckdb

DUCKDB_PATH_DEFAULT = "data_lake/moex.duckdb"

def main(duckdb_path: str = DUCKDB_PATH_DEFAULT):
    con = duckdb.connect(duckdb_path)

    # bars_1d_clean: только активы из universe_core
    con.execute("drop view if exists bars_1d_clean")
    con.execute("""
      create view bars_1d_clean as
      select b.*
      from bars_1d b
      join universe_core u using(instrument_uid)
    """)

    # returns_1d_core: только core и только по тем же инструментам
    con.execute("drop view if exists returns_1d_core")
    con.execute("""
      create view returns_1d_core as
      select r.*
      from returns_1d r
      join universe_core u using(instrument_uid)
    """)

    # features_1d_core: только core
    con.execute("drop view if exists features_1d_core")
    con.execute("""
      create view features_1d_core as
      select f.*
      from features_1d f
      join universe_core u using(instrument_uid)
    """)

    # sanity output
    bars_rows = con.execute("select count(*) from bars_1d_clean").fetchone()[0]
    ret_rows  = con.execute("select count(*) from returns_1d_core").fetchone()[0]
    feat_rows = con.execute("select count(*) from features_1d_core").fetchone()[0]

    print("✅ core views refreshed")
    print(f"bars_1d_clean rows: {bars_rows}")
    print(f"returns_1d_core rows: {ret_rows}")
    print(f"features_1d_core rows: {feat_rows}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--duckdb", default=DUCKDB_PATH_DEFAULT)
    args = p.parse_args()
    main(args.duckdb)