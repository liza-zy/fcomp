# apps/data_pipeline/jobs/rebuild_cov_cache.py
from __future__ import annotations

import argparse
from datetime import date
import duckdb
import pandas as pd
import numpy as np

DUCKDB_PATH_DEFAULT = "data_lake/moex.duckdb"

def get_common_as_of_date(con: duckdb.DuckDBPyConnection) -> date:
    # общий as_of по core-классам: equity+fx+metal
    return con.execute("""
      select min(max_dt) from (
        select asset_class, max(dt) as max_dt
        from returns_1d_core
        where asset_class in ('equity','fx','metal')
        group by 1
      )
    """).fetchone()[0]

def load_returns_from_close_ffill(con, as_of, lookback: int, min_non_null_frac: float) -> pd.DataFrame:
    # 1) берём последние lookback дат по core-календарю (по features_1d_core)
    dts = con.execute("""
        select distinct dt
        from features_1d_core
        where dt <= ?
        order by dt desc
        limit ?
    """, [as_of, lookback]).fetchdf()["dt"].tolist()

    # 2) тянем close по этим датам (ВАЖНО: из features_1d_core)
    df = con.execute("""
        select instrument_uid, dt, close
        from features_1d_core
        where dt in (select unnest(?))
    """, [dts]).fetchdf()

    if df.empty:
        return pd.DataFrame()

    # 3) pivot цен
    px = df.pivot(index="dt", columns="instrument_uid", values="close").sort_index()

    # 4) ffill цены
    px = px.ffill()

    # 5) log-returns
    rets = np.log(px).diff().iloc[1:]

    # 6) НЕ dropna(how="any") (это убивает всё из-за новых активов),
    #    вместо этого фильтруем по доле непустых
    rets = drop_bad_assets(rets, min_non_null_frac=min_non_null_frac)

    # (опционально) убрать строки, где все NaN
    rets = rets.dropna(axis=0, how="all")

    return rets

def pivot_returns(df: pd.DataFrame) -> pd.DataFrame:
    # df: instrument_uid, dt, logret_1d
    wide = df.pivot(index="dt", columns="instrument_uid", values="logret_1d").sort_index()
    return wide

def drop_bad_assets(wide: pd.DataFrame, min_non_null_frac: float) -> pd.DataFrame:
    # удаляем активы, где слишком много пропусков
    frac = wide.notna().mean(axis=0)
    keep_cols = frac[frac >= min_non_null_frac].index
    return wide[keep_cols]

def cov_ledoit(wide: pd.DataFrame) -> pd.DataFrame:
    # Ledoit-Wolf shrinkage (простая версия через numpy + sklearn если есть; но без зависимостей сделаем fallback)
    # В MVP: используем обычную ковариацию + небольшой shrink к диагонали
    X = wide.to_numpy(dtype=float)
    # заполнение NaN нулями (после drop_bad_assets NaN мало)
    X = np.nan_to_num(X, nan=0.0)
    S = np.cov(X, rowvar=False, bias=False)
    # shrink to diagonal
    diag = np.diag(np.diag(S))
    alpha = 0.1
    C = (1 - alpha) * S + alpha * diag
    return pd.DataFrame(C, index=wide.columns, columns=wide.columns)

def cov_ewma(wide: pd.DataFrame, lam: float = 0.94) -> pd.DataFrame:
    # EWMA covariance
    X = wide.to_numpy(dtype=float)
    X = np.nan_to_num(X, nan=0.0)
    T, N = X.shape
    # mean 0 assumption for log returns (ok)
    C = np.zeros((N, N), dtype=float)
    w = 1.0
    norm = 0.0
    for t in range(T):
        xt = X[t:t+1].T  # N x 1
        C += w * (xt @ xt.T)
        norm += w
        w *= lam
    C /= max(norm, 1e-12)
    return pd.DataFrame(C, index=wide.columns, columns=wide.columns)

def save_cov(con, method: str, lookback: int, as_of, cov: pd.DataFrame):
    # хранение в “длинном” виде: i_uid, j_uid, value
    cov = cov.copy()
    cov.index.name = "i_uid"
    cov.columns.name = "j_uid"

    df = cov.stack(future_stack=True).reset_index()
    df.columns = ["i_uid", "j_uid", "value"]
    df["method"] = method
    df["lookback"] = lookback
    df["as_of_date"] = as_of


    con.execute("""
      create table if not exists cov_cache_1d (
        as_of_date DATE,
        method VARCHAR,
        lookback INTEGER,
        i_uid VARCHAR,
        j_uid VARCHAR,
        value DOUBLE
      )
    """)

    con.execute("""
      delete from cov_cache_1d
      where as_of_date = ? and method = ? and lookback = ?
    """, [as_of, method, lookback])

    con.register("tmp_cov_df", df)
    con.execute("""
      insert into cov_cache_1d
      select as_of_date, method, lookback, i_uid, j_uid, value
      from tmp_cov_df
    """)
    con.unregister("tmp_cov_df")

def main(
    duckdb_path: str = DUCKDB_PATH_DEFAULT,
    lookback: int = 252,
    min_non_null_frac: float = 0.98,
    as_of_date: str | None = None,
):
    con = duckdb.connect(duckdb_path)

    if as_of_date is None:
        as_of = get_common_as_of_date(con)
    elif isinstance(as_of_date, date):
        as_of = as_of_date
    else:
        as_of = date.fromisoformat(as_of_date)

    print(f"[cov_cache] as_of_date={as_of} lookback={lookback}")

    # # берём последние lookback дат из returns_1d_core, но только до as_of
    # df = con.execute("""
    #   with dts as (
    #     select distinct dt
    #     from returns_1d_core
    #     where dt <= ?
    #     order by dt desc
    #     limit ?
    #   )
    #   select instrument_uid, dt, logret_1d
    #   from returns_1d_core
    #   where dt in (select dt from dts)
    # """, [as_of, lookback]).fetchdf()

    # if df.empty:
    #     raise SystemExit("No returns for cov build")

    wide = load_returns_from_close_ffill(
        con,
        as_of=as_of,
        lookback=lookback,
        min_non_null_frac=min_non_null_frac,
    )
    print(f"assets used: {wide.shape[1]} rows: {wide.shape[0]}")
    # wide = pivot_returns(df)
    # wide = drop_bad_assets(wide, min_non_null_frac=min_non_null_frac)

    print(f"assets used: {wide.shape[1]} rows: {wide.shape[0]}")
    if wide.shape[1] < 2:
        print("⚠️ Not enough assets for covariance")
        return

    cov1 = cov_ledoit(wide)
    save_cov(con, "ledoit", lookback, as_of, cov1)
    print("✅ saved ledoit")

    cov2 = cov_ewma(wide, lam=0.94)
    save_cov(con, "ewma", lookback, as_of, cov2)
    print("✅ saved ewma")

    print(con.execute("""
      select method, lookback, count(*) as n
      from cov_cache_1d
      where as_of_date = ?
      group by 1,2
      order by 1
    """, [as_of]).fetchdf())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--duckdb", default=DUCKDB_PATH_DEFAULT)
    p.add_argument("--lookback", type=int, default=252)
    p.add_argument("--min-non-null-frac", type=float, default=0.98)
    p.add_argument("--as-of", default=None)
    args = p.parse_args()

    main(
        duckdb_path=args.duckdb,
        lookback=args.lookback,
        min_non_null_frac=args.min_non_null_frac,
        as_of_date=args.as_of,
    )