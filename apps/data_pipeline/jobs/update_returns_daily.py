from __future__ import annotations

from datetime import date, timedelta
import duckdb

DB_PATH = "data_lake/moex.duckdb"

def main():
    con = duckdb.connect(DB_PATH)

    # 1) до какой даты есть свечи
    bars_max = con.sql("select max(dt) from bars_1d").fetchone()[0]
    if bars_max is None:
        print("❌ bars_1d is empty")
        return

    # 2) для каждого инструмента берём последнюю дату returns
    # и пересчитываем только хвост: last_dt-1 .. bars_max
    # (last_dt-1 нужно, чтобы корректно посчитать первый новый logret)
    con.execute("""
    create temp table ret_tail as
    select
      u.instrument_uid,
      coalesce(r.last_dt, date '1900-01-01') as last_dt
    from ref_instruments_universe_v2 u
    left join (
      select instrument_uid, max(dt) as last_dt
      from returns_1d
      group by 1
    ) r using (instrument_uid);
    """)

    # 3) удаляем хвост в returns_1d, который будем пересчитывать
    # если last_dt=1900-01-01 (инструмента нет в returns) — удалять нечего
    con.execute("""
    delete from returns_1d
    using ret_tail t
    where returns_1d.instrument_uid = t.instrument_uid
      and returns_1d.dt >= (t.last_dt - interval 1 day);
    """)

    # 4) пересчитываем logret на хвосте из bars_1d
    con.execute("""
    insert into returns_1d
    with base as (
      select
        b.instrument_uid,
        u.asset_class,
        b.secid,
        b.boardid,
        b.dt,
        b.close,
        lag(b.close) over(partition by b.instrument_uid order by b.dt) as prev_close,
        t.last_dt
      from bars_1d b
      join ref_instruments_universe_v2 u on u.instrument_uid = b.instrument_uid
      join ret_tail t on t.instrument_uid = b.instrument_uid
      where b.dt >= (t.last_dt - interval 1 day)
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

    # 5) короткий отчёт
    print("✅ update_returns_daily done")
    print("bars max(dt):", bars_max)
    print("returns max(dt):", con.sql("select max(dt) from returns_1d").fetchone()[0])
    print(con.sql("""
      select asset_class, count(*) n
      from returns_1d
      where dt >= (current_date - interval 20 day)
      group by 1 order by 1
    """).df())

if __name__ == "__main__":
    main()