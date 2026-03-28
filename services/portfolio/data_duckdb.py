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
            df = con.execute("""
                select
                  instrument_uid,
                  asset_class,
                  secid,
                  boardid,
                  n_obs,
                  first_dt,
                  last_dt
                from universe_core
                where last_dt <= ?
                """, [as_of]).df()
            return df

    def load_universe_for_risk_profile(self, as_of: date, max_risk_score: int) -> pd.DataFrame:
        with self.connect() as con:
            df = con.execute("""
                select
                    u.instrument_uid,
                    u.asset_class,
                    u.secid,
                    u.boardid,
                    u.n_obs,
                    u.first_dt,
                    u.last_dt,
                    ri.currencyid,
                    ri.group_name,
                    arp.risk_profile,
                    arp.risk_score,
                    arp.ann_vol_pct
                from universe_core u
                join ref_instruments ri
                  on ri.instrument_uid = u.instrument_uid
                join asset_risk_profile arp
                  on arp.instrument_uid = u.instrument_uid
                where u.last_dt <= ?
                  and arp.risk_score <= ?
                order by u.asset_class, u.secid
            """, [as_of, max_risk_score]).df()
            return df

    def load_returns_wide(self, instrument_uids: list[str], as_of: date, lookback: int) -> pd.DataFrame:
        with self.connect() as con:
            # берём последние lookback дат на каждый инструмент из returns_1d_core
            df = con.execute(f"""
                select dt, instrument_uid, logret_1d
                from returns_1d
                where instrument_uid in ({",".join(["?"]*len(instrument_uids))})
                  and dt <= ?
                qualify row_number() over (partition by instrument_uid order by dt desc) <= ?
            """, [*instrument_uids, as_of, lookback]).fetchdf()

        # pivot -> dt x instrument_uid
        wide = df.pivot(index="dt", columns="instrument_uid", values="logret_1d").sort_index()
        wide = wide.sort_index()
        wide = wide.tail(lookback)
        # на всякий: оставляем только те uids, у которых достаточно данных
        #wide = wide.dropna(axis=1, thresh=int(0.9*lookback))
        min_frac = 0.95  # можно начать с 0.90
        min_non_nan = int(len(wide) * min_frac)

        wide = wide.dropna(axis=1, thresh=min_non_nan)
        wide = wide.dropna(axis=0, thresh=max(2, int(wide.shape[1] * 0.5)))
        return wide

    def load_cov_matrix(self, instrument_uids: list[str], as_of: date, method: str, lookback: int) -> pd.DataFrame:
        with self.connect() as con:
            df = con.execute(f"""
                select i_uid, j_uid, cov_ij as cov
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
