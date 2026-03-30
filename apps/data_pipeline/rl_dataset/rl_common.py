from __future__ import annotations

from dataclasses import dataclass
from math import sqrt

import numpy as np
import pandas as pd

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection


TRADING_DAYS_PER_YEAR = 252
DEPOSIT_UID = "synthetic:deposit:rub"
WEIGHT_EPS = 1e-8


@dataclass(frozen=True)
class RewardParams:
    transaction_cost_bps: float = 20.0
    band_penalty_scale: float = 0.10


def normalize_weights(w: pd.Series) -> pd.Series:
    if w is None or len(w) == 0:
        return pd.Series(dtype=float)
    w = w.fillna(0.0).clip(lower=0.0)
    s = float(w.sum())
    if s <= 0:
        return w
    return w / s


def drop_small_weights(w: pd.Series, eps: float = WEIGHT_EPS) -> pd.Series:
    if w is None or len(w) == 0:
        return pd.Series(dtype=float)
    w = w.copy()
    w[w.abs() < eps] = 0.0
    return normalize_weights(w)


def l1_turnover(w_from: pd.Series, w_to: pd.Series) -> float:
    idx = sorted(set(w_from.index) | set(w_to.index))
    a = w_from.reindex(idx).fillna(0.0)
    b = w_to.reindex(idx).fillna(0.0)
    return float((a - b).abs().sum())


def load_risk_profiles(con) -> dict[int, dict]:
    rows = con.execute(
        """
        SELECT
            risk_profile_id,
            profile_name,
            target_vol_min,
            target_vol_max,
            max_drawdown_target,
            risk_penalty_lambda,
            turnover_penalty_lambda
        FROM risk_profiles
        ORDER BY risk_profile_id
        """
    ).fetchall()

    return {
        int(r[0]): {
            "risk_profile_id": int(r[0]),
            "profile_name": str(r[1]),
            "target_vol_min": float(r[2]) if r[2] is not None else None,
            "target_vol_max": float(r[3]) if r[3] is not None else None,
            "max_drawdown_target": float(r[4]) if r[4] is not None else None,
            "risk_penalty_lambda": float(r[5]) if r[5] is not None else 1.0,
            "turnover_penalty_lambda": float(r[6]) if r[6] is not None else 0.0,
        }
        for r in rows
    }


def load_decision_calendar(con) -> pd.DataFrame:
    df = con.execute(
        """
        SELECT decision_date, prev_decision_date, next_decision_date, split, window_id, rebalance_index
        FROM decision_calendar
        ORDER BY decision_date
        """
    ).df()
    for c in ["decision_date", "prev_decision_date", "next_decision_date"]:
        df[c] = pd.to_datetime(df[c])
    return df


def load_instruments(con) -> pd.DataFrame:
    return con.execute(
        """
        SELECT instrument_id, instrument_uid, asset_class
        FROM instruments
        """
    ).df()


def get_deposit_instrument_id(con) -> int:
    row = con.execute(
        """
        SELECT instrument_id
        FROM instruments
        WHERE instrument_uid = ?
        """,
        [DEPOSIT_UID],
    ).fetchone()
    if not row:
        raise ValueError("Synthetic deposit instrument is missing in instruments")
    return int(row[0])


def load_baseline_weights(
    con,
    method_name: str = "markowitz_max_sharpe",
) -> dict[tuple[pd.Timestamp, int], pd.Series]:
    df = con.execute(
        """
        SELECT
            bp.decision_date,
            bp.risk_profile_id,
            bp.portfolio_id,
            bpw.instrument_id,
            bpw.target_weight
        FROM baseline_portfolios bp
        JOIN baseline_portfolio_weights bpw
          ON bp.portfolio_id = bpw.portfolio_id
        WHERE bp.method_name = ?
        """,
        [method_name],
    ).df()

    if df.empty:
        return {}

    df["decision_date"] = pd.to_datetime(df["decision_date"])
    out = {}
    for (d, rp), g in df.groupby(["decision_date", "risk_profile_id"], sort=True):
        w = pd.Series(g["target_weight"].values, index=g["instrument_id"].values, dtype=float)
        out[(d, int(rp))] = drop_small_weights(w)
    return out


def load_baseline_portfolio_ids(
    con,
    method_name: str = "markowitz_max_sharpe",
) -> dict[tuple[pd.Timestamp, int], int]:
    df = con.execute(
        """
        SELECT decision_date, risk_profile_id, portfolio_id
        FROM baseline_portfolios
        WHERE method_name = ?
        """,
        [method_name],
    ).df()
    if df.empty:
        return {}
    df["decision_date"] = pd.to_datetime(df["decision_date"])
    return {
        (row["decision_date"], int(row["risk_profile_id"])): int(row["portfolio_id"])
        for _, row in df.iterrows()
    }


def get_macro_row(con, d: pd.Timestamp) -> tuple:
    row = con.execute(
        """
        SELECT
            usd_rub_ret_1p,
            brent_ret_1p,
            imoex_ret_1p,
            rgbi_ret_1p,
            gold_ret_1p,
            cbr_key_rate
        FROM macro_factors
        WHERE date = ?
        """,
        [d.date()],
    ).fetchone()
    if row is None:
        return (None, None, None, None, None, None)
    return row


def get_deposit_daily_returns(con, date_from: pd.Timestamp, date_to: pd.Timestamp) -> pd.Series:
    df = con.execute(
        """
        SELECT date, money_market_rate, cbr_key_rate
        FROM macro_factors
        WHERE date > ? AND date <= ?
        ORDER BY date
        """,
        [date_from.date(), date_to.date()],
    ).df()

    if df.empty:
        return pd.Series(dtype=float)

    df["date"] = pd.to_datetime(df["date"])
    annual_pct = df["money_market_rate"].fillna(df["cbr_key_rate"]).fillna(0.0)
    daily_ret = (1.0 + annual_pct / 100.0) ** (1.0 / TRADING_DAYS_PER_YEAR) - 1.0
    return pd.Series(daily_ret.values, index=df["date"].values, dtype=float)


def get_interval_asset_returns(
    con,
    instrument_ids: list[int],
    date_from: pd.Timestamp,
    date_to: pd.Timestamp,
) -> pd.Series:
    if not instrument_ids:
        return pd.Series(dtype=float)

    placeholders = ",".join(["?"] * len(instrument_ids))
    df = con.execute(
        f"""
        SELECT date, instrument_id, close
        FROM market_prices
        WHERE instrument_id IN ({placeholders})
          AND date IN (?, ?)
        """,
        [*instrument_ids, date_from.date(), date_to.date()],
    ).df()

    if df.empty:
        return pd.Series(0.0, index=instrument_ids, dtype=float)

    df["date"] = pd.to_datetime(df["date"])
    piv = df.pivot(index="instrument_id", columns="date", values="close")

    out = {}
    for iid in instrument_ids:
        c0 = piv.loc[iid, date_from] if iid in piv.index and date_from in piv.columns else np.nan
        c1 = piv.loc[iid, date_to] if iid in piv.index and date_to in piv.columns else np.nan
        if pd.isna(c0) or pd.isna(c1) or c0 == 0:
            out[iid] = 0.0
        else:
            out[iid] = float(c1 / c0 - 1.0)

    return pd.Series(out, dtype=float)


def get_interval_portfolio_return(
    con,
    weights_by_instrument_id: pd.Series,
    deposit_instrument_id: int,
    date_from: pd.Timestamp,
    date_to: pd.Timestamp,
) -> float:
    if weights_by_instrument_id.empty:
        return 0.0

    weights_by_instrument_id = normalize_weights(weights_by_instrument_id)
    deposit_weight = float(weights_by_instrument_id.get(deposit_instrument_id, 0.0))
    risky = weights_by_instrument_id.drop(index=[deposit_instrument_id], errors="ignore")

    gross = 0.0

    if not risky.empty:
        asset_rets = get_interval_asset_returns(con, [int(i) for i in risky.index], date_from, date_to)
        aligned = risky.reindex(asset_rets.index).fillna(0.0)
        gross += float((aligned * asset_rets).sum())

    if deposit_weight > 0:
        dep_daily = get_deposit_daily_returns(con, date_from, date_to)
        dep_period = float((1.0 + dep_daily).prod() - 1.0) if not dep_daily.empty else 0.0
        gross += deposit_weight * dep_period

    return gross


def get_interval_daily_portfolio_returns(
    con,
    weights_by_instrument_id: pd.Series,
    deposit_instrument_id: int,
    date_from: pd.Timestamp,
    date_to: pd.Timestamp,
) -> pd.Series:
    weights_by_instrument_id = normalize_weights(weights_by_instrument_id)

    risky = weights_by_instrument_id.drop(index=[deposit_instrument_id], errors="ignore")
    deposit_weight = float(weights_by_instrument_id.get(deposit_instrument_id, 0.0))

    parts = []

    if not risky.empty:
        risky_ids = [int(i) for i in risky.index]
        placeholders = ",".join(["?"] * len(risky_ids))
        df = con.execute(
            f"""
            WITH priced AS (
                SELECT
                    date,
                    instrument_id,
                    close,
                    LAG(close) OVER (PARTITION BY instrument_id ORDER BY date) AS prev_close
                FROM market_prices
                WHERE instrument_id IN ({placeholders})
                  AND date > ? AND date <= ?
            )
            SELECT
                date,
                instrument_id,
                CASE
                    WHEN prev_close IS NULL OR prev_close = 0 OR close IS NULL THEN NULL
                    ELSE close / prev_close - 1
                END AS ret_1d
            FROM priced
            ORDER BY date, instrument_id
            """,
            [*risky_ids, date_from.date(), date_to.date()],
        ).df()

        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            piv = df.pivot(index="date", columns="instrument_id", values="ret_1d").fillna(0.0)
            w = risky.reindex(piv.columns).fillna(0.0)
            risky_port = piv.mul(w, axis=1).sum(axis=1)
            parts.append(risky_port)

    if deposit_weight > 0:
        dep_daily = get_deposit_daily_returns(con, date_from, date_to)
        if not dep_daily.empty:
            parts.append(dep_daily * deposit_weight)

    if not parts:
        return pd.Series(dtype=float)

    out = pd.concat(parts, axis=1).fillna(0.0).sum(axis=1)
    return out.sort_index()


def realized_vol_annual_from_daily(daily_rets: pd.Series) -> float:
    if daily_rets.empty or len(daily_rets) < 2:
        return 0.0
    return float(daily_rets.std(ddof=1) * sqrt(TRADING_DAYS_PER_YEAR))


def max_drawdown_from_daily(daily_rets: pd.Series) -> float:
    if daily_rets.empty:
        return 0.0
    wealth = (1.0 + daily_rets).cumprod()
    peak = wealth.cummax()
    dd = 1.0 - wealth / peak
    return float(dd.max())


def aggregate_asset_class_weights(
    weights_by_instrument_id: pd.Series,
    asset_class_by_id: dict[int, str],
    deposit_instrument_id: int,
) -> dict[str, float]:
    out = {
        "equity": 0.0,
        "bond": 0.0,
        "fx": 0.0,
        "metal": 0.0,
        "cash": 0.0,
    }

    for iid, w in weights_by_instrument_id.items():
        iid = int(iid)
        if iid == deposit_instrument_id:
            out["cash"] += float(w)
            continue
        ac = asset_class_by_id.get(iid)
        if ac in out:
            out[ac] += float(w)

    return out


def drift_weights_forward(
    con,
    current_target_weights: pd.Series,
    deposit_instrument_id: int,
    date_from: pd.Timestamp,
    date_to: pd.Timestamp,
) -> tuple[pd.Series, float]:
    current_target_weights = normalize_weights(current_target_weights)
    deposit_weight = float(current_target_weights.get(deposit_instrument_id, 0.0))
    risky = current_target_weights.drop(index=[deposit_instrument_id], errors="ignore")

    grown = {}

    if not risky.empty:
        risky_rets = get_interval_asset_returns(con, [int(i) for i in risky.index], date_from, date_to)
        for iid, w in risky.items():
            r = float(risky_rets.get(int(iid), 0.0))
            grown[int(iid)] = float(w) * (1.0 + r)

    if deposit_weight > 0:
        dep_daily = get_deposit_daily_returns(con, date_from, date_to)
        dep_period = float((1.0 + dep_daily).prod() - 1.0) if not dep_daily.empty else 0.0
        grown[int(deposit_instrument_id)] = deposit_weight * (1.0 + dep_period)

    grown_s = pd.Series(grown, dtype=float)
    total = float(grown_s.sum())
    if total <= 0:
        return pd.Series({int(deposit_instrument_id): 1.0}, dtype=float), 0.0

    next_weights = normalize_weights(grown_s)
    gross_return = total - 1.0
    return next_weights, gross_return
