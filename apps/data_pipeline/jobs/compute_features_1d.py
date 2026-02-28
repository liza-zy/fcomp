"""
features_1d — дневные признаки (feature store) для портфельного движка.

Источник:
- bars_1d (OHLCV)
- returns_1d (logret_1d)

Ключ:
- (instrument_uid, dt)

Набор фичей (MVP, window=60):
1) logret_1d
   - Лог-доходность: ln(close_t / close_{t-1})

2) vol_20, vol_60
   - Rolling volatility: std(logret_1d) за 20 и 60 торговых дней.
   - Используется для оценки риска и фильтрации.

3) mom_20, mom_60
   - Momentum: сумма logret_1d за 20 и 60 дней (приближённо лог-рост).
   - Используется как простой сигнал “тренда”.

4) sma_20, sma_60
   - Скользящая средняя цены close за 20 и 60 дней.
   - Используется для трендовых фильтров/доп. сигналов.

5) adv_value_20, adv_value_60
   - Average Daily Traded Value: среднее (close * volume) за 20 и 60 дней.
   - Прокси ликвидности, помогает исключать неликвидные инструменты.

6) maxdd_60
   - Max drawdown за 60 дней по close:
     max_{t in window} (peak_to_trough decline)
   - В MVP можно использовать для “защитных” профилей и фильтрации.

Инкрементальный пересчёт:
- ежедневно пересчитываем только хвост по каждому инструменту:
  последние (WINDOW + BUFFER) дней, чтобы корректно обновить rolling-окна.
"""

from __future__ import annotations

import duckdb

DB_PATH = "data_lake/moex.duckdb"

WINDOW = 60
BUFFER = 5  # небольшой запас на пропуски/стыки


def main():
    con = duckdb.connect(DB_PATH)

    # Создаём таблицу если её нет
    con.execute("""
    create table if not exists features_1d (
      instrument_uid varchar,
      asset_class varchar,
      secid varchar,
      boardid varchar,
      dt date,

      close double,
      volume double,
      value double,

      logret_1d double,

      vol_20 double,
      vol_60 double,

      mom_20 double,
      mom_60 double,

      sma_20 double,
      sma_60 double,

      adv_value_20 double,
      adv_value_60 double,

      maxdd_60 double
    );
    """)

    # вычислим для каждого инструмента с какой даты пересчитывать хвост
    con.execute("""
    create or replace temp table feat_tail as
    select
      u.instrument_uid,
      coalesce(max(f.dt), date '1900-01-01') as last_feat_dt
    from ref_instruments_universe_v2 u
    left join features_1d f using (instrument_uid)
    group by 1;
    """)

    # удаляем хвост, который будем пересчитывать
    # пересчитываем последние WINDOW+BUFFER дней от последней даты фич
    con.execute(f"""
    delete from features_1d
    using feat_tail t
    where features_1d.instrument_uid = t.instrument_uid
      and features_1d.dt >= (t.last_feat_dt - interval '{WINDOW + BUFFER}' day);
    """)

    # пересчитываем хвост
    # value = close*volume (если его нет в bars_1d как отдельного поля)
    con.execute(f"""
    insert into features_1d
    with base as (
      select
        b.instrument_uid,
        u.asset_class,
        b.secid,
        b.boardid,
        b.dt,
        cast(b.close as double) as close,
        cast(b.volume as double) as volume,
        cast(b.close as double) * cast(b.volume as double) as value,
        r.logret_1d
      from bars_1d b
      join ref_instruments_universe_v2 u on u.instrument_uid = b.instrument_uid
      left join returns_1d r
        on r.instrument_uid = b.instrument_uid and r.dt = b.dt
      join feat_tail t on t.instrument_uid = b.instrument_uid
      where b.dt >= (t.last_feat_dt - interval '{WINDOW + BUFFER}' day)
    ),
    roll as (
      select
        *,

        stddev_samp(logret_1d) over w20 as vol_20,
        stddev_samp(logret_1d) over w60 as vol_60,

        sum(logret_1d) over w20 as mom_20,
        sum(logret_1d) over w60 as mom_60,

        avg(close) over w20 as sma_20,
        avg(close) over w60 as sma_60,

        avg(value) over w20 as adv_value_20,
        avg(value) over w60 as adv_value_60,

        max(close) over w60 as rolling_peak_60

      from base
      window
        w20 as (partition by instrument_uid order by dt rows between 19 preceding and current row),
        w60 as (partition by instrument_uid order by dt rows between 59 preceding and current row)
    ),
    dd as (
      select
        *,
        case
          when rolling_peak_60 is null or rolling_peak_60 = 0 then null
          else (rolling_peak_60 - close) / rolling_peak_60
        end as dd_from_peak_60
      from roll
    ),
    dd2 as (
      select
        instrument_uid, asset_class, secid, boardid, dt,
        close, volume, value, logret_1d,
        vol_20, vol_60,
        mom_20, mom_60,
        sma_20, sma_60,
        adv_value_20, adv_value_60,
        max(dd_from_peak_60) over (partition by instrument_uid order by dt rows between 59 preceding and current row) as maxdd_60
      from dd
    )
    select * from dd2;
    """)

    # отчёт
    print("✅ compute_features_1d done")
    print("features_1d rows:", con.sql("select count(*) from features_1d").fetchone()[0])
    print("max(dt):", con.sql("select max(dt) from features_1d").fetchone()[0])

    # быстрый sanity-check
    print(con.sql("""
      select secid, dt, logret_1d, vol_20, vol_60, mom_20, mom_60, adv_value_20, maxdd_60
      from features_1d
      where secid='AFLT'
      order by dt desc
      limit 5
    """).df())


if __name__ == "__main__":
    main()