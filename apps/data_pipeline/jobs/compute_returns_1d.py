from __future__ import annotations
import duckdb

DB_PATH = "data_lake/moex.duckdb"

def main():
    con = duckdb.connect(DB_PATH)

    con.execute("drop table if exists returns_1d;")

    con.execute("""
    create table returns_1d as
    with base as (
      select
        b.instrument_uid,
        u.asset_class,
        b.secid,
        b.boardid,
        b.dt,
        b.close,
        lag(b.close) over(partition by b.instrument_uid order by b.dt) as prev_close
      from bars_1d b
      join ref_instruments_universe_v2 u
        on u.instrument_uid = b.instrument_uid
    )
    select
      instrument_uid,
      asset_class,
      secid,
      boardid,
      dt,
      case
        when prev_close is null or prev_close = 0 or close is null or close = 0 then null
        else ln(close / prev_close)
      end as logret_1d
    from base;
    """)

    con.execute("create index if not exists idx_ret_uid_dt on returns_1d(instrument_uid, dt);")
    con.execute("create index if not exists idx_ret_dt on returns_1d(dt);")

    print("✅ returns_1d created")
    print(con.sql("select asset_class, count(*) n from returns_1d where logret_1d is not null group by 1 order by 1").df())
    print("max(dt) returns_1d:", con.sql("select max(dt) from returns_1d").fetchone()[0])

if __name__ == "__main__":
    main()