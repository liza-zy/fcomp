# apps/data_pipeline/jobs/refresh_universes.py
from __future__ import annotations

import argparse
from datetime import date
import duckdb

DUCKDB_PATH_DEFAULT = "data_lake/moex.duckdb"

CORE_CLASSES = ("equity", "fx", "metal")
WRAPPER_CLASSES = ("fund",)  # bonds later


def get_common_as_of(con: duckdb.DuckDBPyConnection) -> date:
    """
    Общая дата, на которую есть данные у всех core-классов.
    Берём min(max_dt) по equity/fx/metal.
    """
    return con.execute(
        """
        select min(max_dt) from (
          select asset_class, max(dt) as max_dt
          from bars_1d
          where asset_class in ('equity','fx','metal')
          group by 1
        )
        """
    ).fetchone()[0]


def main(
    duckdb_path: str = DUCKDB_PATH_DEFAULT,
    # Порог для core (чтобы совпало с daily_run: min_core_bars)
    min_core_bars: int = 252,
    # Отдельный порог для wrapper (фонды) и для new
    min_fund_obs: int = 60,
    min_new_bars: int = 60,
    # “живость” инструмента
    new_recency_days: int = 90,
    core_recency_days: int = 30,
    as_of_date: str | None = None,
):
    con = duckdb.connect(duckdb_path)

    # 1) as_of (общий)
    as_of = date.fromisoformat(as_of_date) if as_of_date else get_common_as_of(con)

    # 2) universe_new: шире, но только “живое” и с минимальным числом наблюдений
    con.execute("drop table if exists universe_new")
    con.execute(
        """
        create table universe_new as
        with obs as (
          select
            instrument_uid,
            count(*)::int as n_obs,
            min(dt) as first_dt,
            max(dt) as last_dt
          from bars_1d
          where dt <= ?
          group by 1
        )
        select
          r.instrument_uid,
          r.asset_class,
          r.secid,
          r.boardid,
          o.n_obs,
          o.first_dt,
          o.last_dt
        from ref_instruments r
        join obs o using(instrument_uid)
        where r.asset_class in ('equity','fx','metal','fund')
          and o.n_obs >= case
            when r.asset_class in ('equity','fx','metal') then ?
            when r.asset_class='fund' then ?
            else 999999
          end
          and o.last_dt >= (?::date - (? * interval '1 day'))
        """,
        [as_of, min_new_bars, min_fund_obs, as_of, new_recency_days],
    )

    # 3) universe_core: только core-классы, строго >= min_core_bars и “свежее”
    con.execute("drop table if exists universe_core")
    con.execute(
        """
        create table universe_core as
        select *
        from universe_new
        where asset_class in ('equity','fx','metal')
          and n_obs >= ?
          and last_dt >= (?::date - (? * interval '1 day'))
        """,
        [min_core_bars, as_of, core_recency_days],
    )

    # 4) universe_wrappers: фонды отдельно
    con.execute("drop table if exists universe_wrappers")
    con.execute(
        """
        create table universe_wrappers as
        select *
        from universe_new
        where asset_class in ('fund')
        """
    )

    # Индексы (не обязательно, но полезно)
    con.execute("create unique index if not exists ux_universe_new_uid on universe_new(instrument_uid)")
    con.execute("create unique index if not exists ux_universe_core_uid on universe_core(instrument_uid)")
    con.execute("create unique index if not exists ux_universe_wrap_uid on universe_wrappers(instrument_uid)")

    print("✅ refreshed universes")
    print(f"as_of: {as_of}")
    print(con.execute("""
      select 'new' as u, asset_class, count(*) n
      from universe_new
      group by 1,2 order by 1,2
    """).fetchdf())
    print(con.execute("""
      select 'core' as u, asset_class, count(*) n
      from universe_core
      group by 1,2 order by 1,2
    """).fetchdf())
    print(con.execute("""
      select 'wrap' as u, asset_class, count(*) n
      from universe_wrappers
      group by 1,2 order by 1,2
    """).fetchdf())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--duckdb", default=DUCKDB_PATH_DEFAULT)
    p.add_argument("--as-of", default=None)

    p.add_argument("--min-core-bars", type=int, default=252)
    p.add_argument("--min-new-bars", type=int, default=60)
    p.add_argument("--min-fund-obs", type=int, default=60)

    p.add_argument("--new-recency-days", type=int, default=90)
    p.add_argument("--core-recency-days", type=int, default=30)

    args = p.parse_args()
    as_of = date.fromisoformat(args.as_of) if args.as_of else get_common_as_of(con)

    main(
        duckdb_path=args.duckdb,
        as_of_date=args.as_of,
        min_core_bars=args.min_core_bars,
        min_new_bars=args.min_new_bars,
        min_fund_obs=args.min_fund_obs,
        new_recency_days=args.new_recency_days,
        core_recency_days=args.core_recency_days,
    )