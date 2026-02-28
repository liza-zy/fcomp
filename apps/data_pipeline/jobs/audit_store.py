# apps/data_pipeline/jobs/audit_store.py
from __future__ import annotations

import argparse
from datetime import date
import duckdb
import pandas as pd

DUCKDB_PATH_DEFAULT = "data_lake/moex.duckdb"

CORE_CLASSES = ("equity", "fx", "metal")

def main(duckdb_path: str = DUCKDB_PATH_DEFAULT, lookback: int = 252):
    con = duckdb.connect(duckdb_path)

    print("=== Universe sizes ===")
    for t in ["universe_new", "universe_core", "universe_wrappers"]:
        exists = con.execute("""
          select count(*) from information_schema.tables
          where table_name = ?
        """, [t]).fetchone()[0]
        if exists:
            print(f"\n-- {t} --")
            print(con.execute(f"""
              select asset_class, count(*) n
              from {t}
              group by 1 order by 1
            """).fetchdf())
        else:
            print(f"\n-- {t} -- (missing)")

    print("\n=== Duplicates (should be 0) ===")
    # ключи дубликатов: instrument_uid+dt в фактах, cov_cache: as_of+method+lookback+i+j
    dup = []
    dup.append(("bars_1d", con.execute("""
      select count(*) from (
        select instrument_uid, dt, count(*) c
        from bars_1d
        group by 1,2
        having count(*) > 1
      ) t
    """).fetchone()[0]))

    dup.append(("returns_1d", con.execute("""
      select count(*) from (
        select instrument_uid, dt, count(*) c
        from returns_1d
        group by 1,2
        having count(*) > 1
      ) t
    """).fetchone()[0]))

    dup.append(("features_1d", con.execute("""
      select count(*) from (
        select instrument_uid, dt, count(*) c
        from features_1d
        group by 1,2
        having count(*) > 1
      ) t
    """).fetchone()[0]))

    dup.append(("cov_cache_1d", con.execute("""
      select count(*) from (
        select as_of_date, method, lookback, i_uid, j_uid, count(*) c
        from cov_cache_1d
        group by 1,2,3,4,5
        having count(*) > 1
      ) t
    """).fetchone()[0]))

    print(pd.DataFrame(dup, columns=["table", "dupe_key_groups"]))

    print("\n=== Max dates by asset_class (bars_1d) ===")
    print(con.execute("""
      select asset_class, max(dt) as max_dt, min(dt) as min_dt, count(*) as rows
      from bars_1d
      group by 1
      order by 1
    """).fetchdf())

    print("\n=== Max dates by asset_class (returns_1d) ===")
    print(con.execute("""
      select asset_class, max(dt) as max_dt, min(dt) as min_dt, count(*) as rows
      from returns_1d
      group by 1
      order by 1
    """).fetchdf())

    print("\n=== Max dates by asset_class (features_1d) ===")
    print(con.execute("""
      select asset_class, max(dt) as max_dt, min(dt) as min_dt, count(*) as rows
      from features_1d
      group by 1
      order by 1
    """).fetchdf())

    # coverage core vs facts
    print("\n=== Coverage vs universe_core ===")
    # missing bars
    print("\n-- Missing bars_1d for core --")
    print(con.execute("""
      select u.asset_class, count(*) as missing
      from universe_core u
      left join (
        select distinct instrument_uid from bars_1d
      ) b using(instrument_uid)
      where b.instrument_uid is null
      group by 1
      order by 1
    """).fetchdf())

    # missing returns (non-null)
    print("\n-- Missing returns_1d (any rows) for core --")
    print(con.execute("""
      select u.asset_class, count(*) as missing
      from universe_core u
      left join (
        select distinct instrument_uid from returns_1d
      ) r using(instrument_uid)
      where r.instrument_uid is null
      group by 1
      order by 1
    """).fetchdf())

    # missing features (vol_60 not null)
    print("\n-- Missing features_1d (vol_60 not null somewhere) for core --")
    print(con.execute("""
      with ok as (
        select distinct instrument_uid
        from features_1d
        where vol_60 is not null
      )
      select u.asset_class, count(*) as missing
      from universe_core u
      left join ok using(instrument_uid)
      where ok.instrument_uid is null
      group by 1
      order by 1
    """).fetchdf())

    # cov coverage
    print("\n=== Cov coverage ===")
    as_of = con.execute("select max(as_of_date) from cov_cache_1d").fetchone()[0]
    print("cov as_of:", as_of)

    print("\n-- assets from cov (by asset_class) --")
    print(con.execute("""
      with cov_uids as (
        select distinct i_uid as instrument_uid
        from cov_cache_1d
        where as_of_date = (select max(as_of_date) from cov_cache_1d)
      )
      select u.asset_class, count(*) as n_in_cov
      from universe_core u
      join cov_uids c using(instrument_uid)
      group by 1
      order by 1
    """).fetchdf())

    print("\n-- core assets missing in cov (sample) --")
    missing = con.execute("""
      with cov_uids as (
        select distinct i_uid as instrument_uid
        from cov_cache_1d
        where as_of_date = (select max(as_of_date) from cov_cache_1d)
      )
      select u.asset_class, u.secid, u.boardid, u.instrument_uid
      from universe_core u
      left join cov_uids c using(instrument_uid)
      where c.instrument_uid is null
      order by u.asset_class, u.secid
      limit 30
    """).fetchdf()
    print(missing)

    # diagnose missing in cov: non-null fraction in last lookback window
    if as_of is not None:
        print("\n-- diagnose: non-null fraction for missing assets in last lookback window --")
        diag = con.execute("""
          with dts as (
            select distinct dt
            from returns_1d_core
            where dt <= ?
            order by dt desc
            limit ?
          ),
          win as (
            select instrument_uid, dt, logret_1d
            from returns_1d_core
            where dt in (select dt from dts)
          ),
          stats as (
            select instrument_uid,
                   count(*) as n_rows,
                   sum(case when logret_1d is null then 1 else 0 end) as n_nulls
            from win
            group by 1
          ),
          cov_uids as (
            select distinct i_uid as instrument_uid
            from cov_cache_1d
            where as_of_date = (select max(as_of_date) from cov_cache_1d)
          )
          select u.asset_class, u.secid, u.boardid,
                 s.n_rows, s.n_nulls,
                 (1.0 - (s.n_nulls::double / nullif(s.n_rows,0))) as non_null_frac
          from universe_core u
          join stats s using(instrument_uid)
          left join cov_uids c using(instrument_uid)
          where c.instrument_uid is null
          order by non_null_frac asc, u.asset_class, u.secid
          limit 50
        """, [as_of, lookback]).fetchdf()
        print(diag)

    print("\n✅ audit done")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--duckdb", default=DUCKDB_PATH_DEFAULT)
    p.add_argument("--lookback", type=int, default=252)
    args = p.parse_args()
    main(args.duckdb, lookback=args.lookback)