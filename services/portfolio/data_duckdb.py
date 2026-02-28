from __future__ import annotations
import duckdb
import pandas as pd
from datetime import date

DUCKDB_PATH_DEFAULT = "data_lake/moex.duckdb"

CORE_CLASSES = ("equity", "fx", "metal")

class DuckDBMarketData:
    def __init__(self, path: str = DUCKDB_PATH_DEFAULT):
        self.path = path

    def connect(self):
        return duckdb.connect(self.path)

    def get_as_of_common(self) -> date:
        with self.connect() as con:
            return con.execute("select max(dt) from bars_1d_clean").fetchone()[0]

    def load_universe_core(self, as_of: date) -> pd.DataFrame:
        with self.connect() as con:
            # universe_core у тебя уже пересчитывается daily_run
            df = con.execute("""
                select u.instrument_uid, u.asset_class, u.secid, u.boardid,
                       r.currencyid,
                       coalesce(s.name, '') as sector_name
                from universe_core u
                join ref_instruments r using(instrument_uid)
                left join ref_sectors s
                  on s.secid = u.secid
                where u.asset_class in ('equity','fx','metal')
            """).fetchdf()
        return df

    def load_returns_wide(self, instrument_uids: list[str], as_of: date, lookback: int) -> pd.DataFrame:
        with self.connect() as con:
            # берём последние lookback дат на каждый инструмент из returns_1d_core
            df = con.execute(f"""
                select dt, instrument_uid, logret_1d
                from returns_1d_core
                where instrument_uid in ({",".join(["?"]*len(instrument_uids))})
                  and dt <= ?
                qualify row_number() over (partition by instrument_uid order by dt desc) <= ?
            """, [*instrument_uids, as_of, lookback]).fetchdf()

        # pivot -> dt x instrument_uid
        wide = df.pivot(index="dt", columns="instrument_uid", values="logret_1d").sort_index()
        # на всякий: оставляем только те uids, у которых достаточно данных
        wide = wide.dropna(axis=1, thresh=int(0.9*lookback))
        return wide

    def load_cov_matrix(self, instrument_uids: list[str], as_of: date, method: str, lookback: int) -> pd.DataFrame:
        with self.connect() as con:
            df = con.execute(f"""
                select i_uid, j_uid, cov
                from cov_cache_1d
                where as_of_date = ?
                  and method = ?
                  and lookback = ?
                  and i_uid in ({",".join(["?"]*len(instrument_uids))})
                  and j_uid in ({",".join(["?"]*len(instrument_uids))})
            """, [as_of, method, lookback, *instrument_uids, *instrument_uids]).fetchdf()

        if df.empty:
            return pd.DataFrame()

        cov = df.pivot(index="i_uid", columns="j_uid", values="cov").sort_index().sort_index(axis=1)
        return cov