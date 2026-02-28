from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import duckdb
import pandas as pd

from apps.data_pipeline.runtime.settings import DUCKDB_PATH


def connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DUCKDB_PATH))


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create table if not exists ref_asset_classes(
          asset_class varchar primary key,
          name varchar
        );

        create table if not exists ref_sectors(
          sector_id varchar primary key,
          name varchar,
          provider varchar,
          meta_json varchar,
          updated_at timestamptz
        );

        create table if not exists ref_instruments(
          instrument_uid varchar primary key,
          asset_class varchar,
          engine varchar,
          market varchar,
          boardid varchar,
          secid varchar,
          shortname varchar,
          name varchar,
          isin varchar,
          currencyid varchar,
          lot bigint,
          type varchar,
          group_name varchar,
          is_traded boolean,
          meta_json varchar,
          updated_at timestamptz
        );

        create index if not exists idx_ref_instruments_secid on ref_instruments(secid);
        create index if not exists idx_ref_instruments_asset on ref_instruments(asset_class);
        """
    )


def seed_asset_classes(con: duckdb.DuckDBPyConnection) -> None:
    ensure_schema(con)
    rows = [
        ("equity", "Equities (shares)"),
        ("fund", "Funds/ETFs"),
        ("fx", "FX"),
        ("metal", "Metals"),
    ]
    con.execute("delete from ref_asset_classes;")
    con.executemany("insert into ref_asset_classes(asset_class, name) values (?, ?)", rows)


def _now_ts():
    return datetime.now(timezone.utc)


def upsert_instruments(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """
    Expect df columns:
      instrument_uid, asset_class, engine, market, boardid, secid, shortname, name,
      isin, currencyid, lot, type, group_name, is_traded, meta_json
    """
    ensure_schema(con)

    df = df.copy()
    # защита от дублей в выгрузке MOEX
    df = df.drop_duplicates(subset=["instrument_uid"], keep="last")

    df["updated_at"] = _now_ts()

    con.register("tmp_instruments", df)

    # MERGE поддерживается в DuckDB (актуальные версии).
    con.execute(
        """
        merge into ref_instruments as t
        using tmp_instruments as s
        on t.instrument_uid = s.instrument_uid
        when matched then update set
          asset_class = s.asset_class,
          engine = s.engine,
          market = s.market,
          boardid = s.boardid,
          secid = s.secid,
          shortname = s.shortname,
          name = s.name,
          isin = s.isin,
          currencyid = s.currencyid,
          lot = s.lot,
          type = s.type,
          group_name = s.group_name,
          is_traded = s.is_traded,
          meta_json = s.meta_json,
          updated_at = s.updated_at
        when not matched then insert (
          instrument_uid, asset_class, engine, market, boardid, secid, shortname, name,
          isin, currencyid, lot, type, group_name, is_traded, meta_json, updated_at
        ) values (
          s.instrument_uid, s.asset_class, s.engine, s.market, s.boardid, s.secid, s.shortname, s.name,
          s.isin, s.currencyid, s.lot, s.type, s.group_name, s.is_traded, s.meta_json, s.updated_at
        );
        """
    )
    con.unregister("tmp_instruments")






def ensure_bars_1d_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create table if not exists bars_1d (
            instrument_uid varchar,
            asset_class varchar,
            secid varchar,
            boardid varchar,
            dt date,

            open double,
            high double,
            low double,
            close double,
            volume double,
            value double,

            begin_ts timestamp,
            end_ts timestamp,

            primary key (instrument_uid, dt)
        );
        """
    )
    # индексы под типовые запросы
    con.execute("create index if not exists idx_bars1d_dt on bars_1d(dt);")
    con.execute("create index if not exists idx_bars1d_asset on bars_1d(asset_class);")


def upsert_bars_1d(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    ensure_bars_1d_schema(con)

    # Нормализация типов
    df = df.copy()
    df["dt"] = pd.to_datetime(df["dt"]).dt.date

    # Быстрый "upsert": удаляем пересечение по ключам, потом вставляем
    con.register("tmp_bars_1d", df)

    con.execute(
        """
        delete from bars_1d
        where (instrument_uid, dt) in (select instrument_uid, dt from tmp_bars_1d);
        """
    )
    con.execute(
        """
        insert into bars_1d
        select
            instrument_uid, asset_class, secid, boardid, dt,
            open, high, low, close, volume, value,
            begin_ts, end_ts
        from tmp_bars_1d;
        """
    )

    con.unregister("tmp_bars_1d")
    return len(df)