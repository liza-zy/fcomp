from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Iterable

import numpy as np
import pandas as pd

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection


TRADING_DAYS_PER_YEAR = 252

DEPOSIT_UID = "synthetic:deposit:rub"
DEPOSIT_SECID = "DEPOSIT_RUB"
DEPOSIT_NAME = "Synthetic RUB Deposit"
DEPOSIT_ASSET_CLASS = "deposit"
DEPOSIT_BOARDID = "SYNTH"
DEPOSIT_RISK_PROFILE = "Ultra-Conservative"

WEIGHT_EPS = 1e-4

MIN_DEPOSIT_SHARE_BY_PROFILE = {
    "Ultra-Conservative": 0.50,
    "Conservative": 0.25,
    "Balanced": 0.10,
    "Growth": 0.00,
    "Aggressive": 0.00,
}


@dataclass(frozen=True)
class BaselineParams:
    lookback: int = 252
    min_obs_frac: float = 0.60
    min_risky_assets: int = 1
    only_missing: bool = False
    print_every_n_dates: int = 5


def _positive_normalize(w: pd.Series) -> pd.Series:
    w = w.fillna(0.0).clip(lower=0.0)
    s = float(w.sum())
    if s <= 0:
        if len(w) == 0:
            return w
        return pd.Series(1.0 / len(w), index=w.index, dtype=float)
    return w / s


def _clip_single_name(w: pd.Series, cap_single: float | None) -> pd.Series:
    if cap_single is None or cap_single <= 0:
        return _positive_normalize(w)

    w = _positive_normalize(w).copy()

    for _ in range(30):
        over = w > cap_single + 1e-12
        if not over.any():
            break

        excess = float((w[over] - cap_single).sum())
        w.loc[over] = cap_single

        under = w < cap_single - 1e-12
        under_sum = float(w.loc[under].sum())
        if excess <= 1e-12 or under_sum <= 1e-12:
            break

        w.loc[under] += excess * (w.loc[under] / under_sum)

    return _positive_normalize(w)


def _portfolio_metrics(mu_daily: pd.Series, cov_daily: pd.DataFrame, w: pd.Series) -> dict[str, float]:
    if w.empty:
        return {
            "exp_return_ann": 0.0,
            "vol_ann": 0.0,
            "sharpe_like": 0.0,
        }

    aligned = w.reindex(mu_daily.index).fillna(0.0)
    exp_ret_daily = float(mu_daily.values @ aligned.values)
    vol_daily = float(np.sqrt(aligned.values @ cov_daily.values @ aligned.values))

    exp_ret_ann = exp_ret_daily * TRADING_DAYS_PER_YEAR
    vol_ann = vol_daily * sqrt(TRADING_DAYS_PER_YEAR)
    sharpe = float(exp_ret_ann / vol_ann) if vol_ann > 1e-12 else 0.0

    return {
        "exp_return_ann": exp_ret_ann,
        "vol_ann": vol_ann,
        "sharpe_like": sharpe,
    }


def _max_sharpe_closed_form(mu_daily: pd.Series, cov_daily: pd.DataFrame) -> pd.Series:
    raw = np.linalg.pinv(cov_daily.values) @ mu_daily.values
    raw = np.maximum(raw, 0.0)
    w = pd.Series(raw, index=mu_daily.index, dtype=float)
    return _positive_normalize(w)


def _max_return_ranked(mu_daily: pd.Series) -> pd.Series:
    ranked = mu_daily.sort_values(ascending=False)
    if ranked.empty:
        return pd.Series(dtype=float)

    w = pd.Series(0.0, index=mu_daily.index, dtype=float)
    w.loc[ranked.index[0]] = 1.0
    return w


def _next_portfolio_id(con) -> int:
    row = con.execute(
        "SELECT COALESCE(MAX(portfolio_id), 0) + 1 FROM baseline_portfolios"
    ).fetchone()
    return int(row[0])


def _load_decision_dates(con, only_missing: bool) -> list:
    if not only_missing:
        rows = con.execute(
            """
            SELECT decision_date
            FROM decision_calendar
            ORDER BY decision_date
            """
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT dc.decision_date
            FROM decision_calendar dc
            WHERE NOT EXISTS (
                SELECT 1
                FROM baseline_portfolios bp
                WHERE bp.decision_date = dc.decision_date
            )
            ORDER BY dc.decision_date
            """
        ).fetchall()

    return [r[0] for r in rows]


def _load_profiles(con) -> list[dict]:
    rows = con.execute(
        """
        SELECT
            risk_profile_id,
            profile_name,
            target_vol_min,
            target_vol_max,
            max_single_asset_weight
        FROM risk_profiles
        ORDER BY risk_profile_id
        """
    ).fetchall()

    if not rows:
        raise ValueError("risk_profiles is empty")

    out = []
    for r in rows:
        out.append(
            {
                "risk_profile_id": int(r[0]),
                "profile_name": str(r[1]),
                "target_vol_min": float(r[2]) if r[2] is not None else None,
                "target_vol_max": float(r[3]) if r[3] is not None else None,
                "max_single_asset_weight": float(r[4]) if r[4] is not None else 0.15,
            }
        )
    return out


def _clear_baseline_tables(con) -> None:
    con.execute("DELETE FROM baseline_portfolio_weights")
    con.execute("DELETE FROM baseline_portfolios")
    con.execute("DELETE FROM baseline_universe")


def _ensure_deposit_instrument(con) -> int:
    row = con.execute(
        """
        SELECT instrument_id
        FROM instruments
        WHERE instrument_uid = ?
        """,
        [DEPOSIT_UID],
    ).fetchone()

    if row:
        return int(row[0])

    next_id = con.execute(
        "SELECT COALESCE(MAX(instrument_id), 0) + 1 FROM instruments"
    ).fetchone()[0]

    con.execute(
        """
        INSERT INTO instruments (
            instrument_id,
            instrument_uid,
            secid,
            name,
            isin,
            asset_class,
            sector,
            boardid,
            currency,
            is_qualified_only,
            first_trade_date,
            last_trade_date,
            is_active
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            int(next_id),
            DEPOSIT_UID,
            DEPOSIT_SECID,
            DEPOSIT_NAME,
            None,
            DEPOSIT_ASSET_CLASS,
            None,
            DEPOSIT_BOARDID,
            "RUB",
            False,
            None,
            None,
            True,
        ],
    )

    return int(next_id)


def _load_deposit_daily_return(con, decision_date) -> float:
    row = con.execute(
        """
        SELECT money_market_rate, cbr_key_rate
        FROM macro_factors
        WHERE date = ?
        """,
        [decision_date],
    ).fetchone()

    if not row:
        return 0.0

    money_market_rate, cbr_key_rate = row
    annual_pct = money_market_rate if money_market_rate is not None else cbr_key_rate
    if annual_pct is None:
        return 0.0

    annual_frac = float(annual_pct) / 100.0
    return (1.0 + annual_frac) ** (1.0 / TRADING_DAYS_PER_YEAR) - 1.0


def _insert_baseline_universe(
    con,
    decision_date,
    profile: dict,
    deposit_instrument_id: int,
) -> list[tuple]:
    """
    Возвращает список риск-активов:
    [(instrument_id, instrument_uid, asset_class, risk_profile_asset), ...]
    """
    rows = con.execute(
        """
        SELECT
            i.instrument_id,
            i.instrument_uid,
            i.asset_class,
            i.is_qualified_only,
            f.has_min_history,
            f.is_available,
            f.liquidity_score,
            f.risk_profile_asset,
            f.risk_score_asset
        FROM instrument_features f
        JOIN instruments i
          ON i.instrument_id = f.instrument_id
        WHERE f.decision_date = ?
        ORDER BY i.instrument_id
        """,
        [decision_date],
    ).fetchall()

    payload = []
    selected = []

    for row in rows:
        (
            instrument_id,
            instrument_uid,
            asset_class,
            is_qualified_only,
            has_min_history,
            is_available,
            liquidity_score,
            risk_profile_asset,
            risk_score_asset,
        ) = row

        passes_liquidity = liquidity_score is not None
        passes_qualification = not bool(is_qualified_only)
        passes_risk = risk_score_asset is not None and float(risk_score_asset) <= float(profile["risk_profile_id"])

        is_in_universe = (
            bool(has_min_history)
            and bool(is_available)
            and bool(passes_liquidity)
            and bool(passes_qualification)
            and bool(passes_risk)
        )

        inclusion_reason = "eligible" if is_in_universe else None

        reasons = []
        if not has_min_history:
            reasons.append("no_min_history")
        if not is_available:
            reasons.append("not_available")
        if not passes_liquidity:
            reasons.append("no_liquidity")
        if not passes_qualification:
            reasons.append("qualified_only")
        if not passes_risk:
            reasons.append("risk_above_profile")

        exclusion_reason = ",".join(reasons) if reasons else None

        payload.append(
            (
                decision_date,
                profile["risk_profile_id"],
                instrument_id,
                bool(is_in_universe),
                bool(has_min_history),
                bool(passes_liquidity),
                bool(passes_qualification),
                inclusion_reason,
                exclusion_reason,
            )
        )

        if is_in_universe:
            selected.append((instrument_id, instrument_uid, asset_class, risk_profile_asset))

    payload.append(
        (
            decision_date,
            profile["risk_profile_id"],
            deposit_instrument_id,
            True,
            True,
            True,
            True,
            "synthetic_deposit",
            None,
        )
    )

    con.executemany(
        """
        INSERT INTO baseline_universe (
            decision_date,
            risk_profile_id,
            instrument_id,
            is_in_universe,
            has_min_history,
            passes_liquidity_filter,
            passes_qualification_filter,
            inclusion_reason,
            exclusion_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )

    return selected


def _load_returns_wide(con, instrument_uids: list[str], decision_date, lookback: int) -> pd.DataFrame:
    if not instrument_uids:
        return pd.DataFrame()

    placeholders = ",".join(["?"] * len(instrument_uids))
    df = con.execute(
        f"""
        WITH windowed_returns AS (
            SELECT
                r.instrument_uid,
                r.dt,
                r.logret_1d,
                ROW_NUMBER() OVER (
                    PARTITION BY r.instrument_uid
                    ORDER BY r.dt DESC
                ) AS rn
            FROM src.returns_1d r
            WHERE r.instrument_uid IN ({placeholders})
              AND r.dt <= ?
        )
        SELECT instrument_uid, dt, logret_1d
        FROM windowed_returns
        WHERE rn <= ?
        """,
        [*instrument_uids, decision_date, lookback],
    ).df()

    if df.empty:
        return pd.DataFrame()

    wide = df.pivot(index="dt", columns="instrument_uid", values="logret_1d").sort_index()
    wide = wide.tail(lookback)
    return wide


def _prepare_window(wide: pd.DataFrame, min_obs_frac: float) -> pd.DataFrame:
    if wide.empty:
        return wide

    min_obs = max(20, int(len(wide) * min_obs_frac))
    wide = wide.dropna(axis=1, thresh=min_obs)

    if wide.shape[1] == 0:
        return pd.DataFrame()

    wide = wide.fillna(0.0)
    return wide


def _mix_with_deposit_by_target_vol(
    risky_weights: pd.Series,
    risky_metrics: dict[str, float],
    target_vol_max: float | None,
) -> tuple[pd.Series, float]:
    risky_weights = risky_weights.copy()

    if risky_weights.empty:
        return pd.Series({DEPOSIT_UID: 1.0}, dtype=float), 1.0

    risky_vol_ann = float(risky_metrics["vol_ann"])

    if target_vol_max is None:
        alpha = 1.0
    elif risky_vol_ann <= 1e-12:
        alpha = 0.0
    else:
        alpha = min(1.0, max(0.0, float(target_vol_max) / risky_vol_ann))

    final_risky = risky_weights * alpha
    deposit_weight = max(0.0, 1.0 - float(final_risky.sum()))

    final_weights = final_risky.copy()
    if deposit_weight > 0:
        final_weights.loc[DEPOSIT_UID] = deposit_weight

    final_weights = _positive_normalize(final_weights)
    deposit_weight = float(final_weights.get(DEPOSIT_UID, 0.0))
    return final_weights, deposit_weight

def _apply_min_deposit_floor(
    weights: pd.Series,
    min_deposit_share: float,
) -> tuple[pd.Series, float]:
    weights = weights.copy()

    current_deposit = float(weights.get(DEPOSIT_UID, 0.0))
    if current_deposit >= min_deposit_share - 1e-12:
        weights = _positive_normalize(weights)
        return weights, float(weights.get(DEPOSIT_UID, 0.0))

    risky = weights.drop(labels=[DEPOSIT_UID], errors="ignore").copy()
    risky_sum = float(risky.sum())

    target_risky_sum = max(0.0, 1.0 - min_deposit_share)

    if risky_sum <= 1e-12:
        out = pd.Series({DEPOSIT_UID: 1.0}, dtype=float)
        return out, 1.0

    scale = target_risky_sum / risky_sum
    risky = risky * scale

    out = risky.copy()
    out.loc[DEPOSIT_UID] = min_deposit_share
    out = _positive_normalize(out)

    return out, float(out.get(DEPOSIT_UID, 0.0))

def _count_effective_risky_assets(w: pd.Series) -> int:
    if w.empty:
        return 0
    risky = w.drop(labels=[DEPOSIT_UID], errors="ignore")
    return int((risky > WEIGHT_EPS).sum())


def _filter_small_weights(w: pd.Series) -> pd.Series:
    if w.empty:
        return w
    w = w.copy()
    w[w < WEIGHT_EPS] = 0.0
    return _positive_normalize(w)


def build_markowitz_baseline(
    params: BaselineParams = BaselineParams(),
    cfg: RLDatabaseConfig = RLDatabaseConfig(),
) -> None:
    con = get_connection(cfg.target_db)
    try:
        con.execute(f"ATTACH '{cfg.source_db.as_posix()}' AS src (READ_ONLY)")

        deposit_instrument_id = _ensure_deposit_instrument(con)
        decision_dates = _load_decision_dates(con, params.only_missing)
        profiles = _load_profiles(con)

        if not decision_dates:
            print("No decision dates to process")
            return

        if not params.only_missing:
            _clear_baseline_tables(con)

        total_portfolios = 0

        for idx, decision_date in enumerate(decision_dates):
            should_print_header = (idx % params.print_every_n_dates == 0)
            if should_print_header:
                print(f"\n=== decision_date={decision_date} ===")

            for profile in profiles:
                risky_selected = _insert_baseline_universe(
                    con=con,
                    decision_date=decision_date,
                    profile=profile,
                    deposit_instrument_id=deposit_instrument_id,
                )

                risky_uids = [x[1] for x in risky_selected]
                asset_class_by_uid = {x[1]: x[2] for x in risky_selected}
                risk_by_uid = {x[1]: x[3] for x in risky_selected}

                deposit_daily_return = _load_deposit_daily_return(con, decision_date)
                risky_wide = _load_returns_wide(con, risky_uids, decision_date, params.lookback)
                risky_wide = _prepare_window(risky_wide, params.min_obs_frac)

                if risky_wide.empty or risky_wide.shape[1] < params.min_risky_assets:
                    mu_daily = pd.Series({DEPOSIT_UID: deposit_daily_return}, dtype=float)
                    cov_daily = pd.DataFrame(
                        [[1e-10]],
                        index=[DEPOSIT_UID],
                        columns=[DEPOSIT_UID],
                        dtype=float,
                    )

                    final_portfolios = {
                        "markowitz_max_sharpe": pd.Series({DEPOSIT_UID: 1.0}, dtype=float),
                        "markowitz_max_return": pd.Series({DEPOSIT_UID: 1.0}, dtype=float),
                    }
                else:
                    mu_risky_daily = risky_wide.mean()
                    cov_risky_daily = risky_wide.cov()

                    cap_single = profile["max_single_asset_weight"] or 0.15

                    risky_sh = _max_sharpe_closed_form(mu_risky_daily, cov_risky_daily)
                    risky_sh = _clip_single_name(risky_sh, cap_single)
                    risky_sh = _filter_small_weights(risky_sh)
                    risky_sh_metrics = _portfolio_metrics(mu_risky_daily, cov_risky_daily, risky_sh)

                    final_sh, _ = _mix_with_deposit_by_target_vol(
                        risky_weights=risky_sh,
                        risky_metrics=risky_sh_metrics,
                        target_vol_max=profile["target_vol_max"],
                    )

                    min_deposit_share = MIN_DEPOSIT_SHARE_BY_PROFILE.get(
                        profile["profile_name"],
                        0.0,
                    )
                    final_sh, _ = _apply_min_deposit_floor(
                        weights=final_sh,
                        min_deposit_share=min_deposit_share,
                    )

                    final_sh = _filter_small_weights(final_sh)

                    risky_ret = _max_return_ranked(mu_risky_daily)
                    risky_ret = _clip_single_name(risky_ret, cap_single)
                    risky_ret = _filter_small_weights(risky_ret)
                    risky_ret_metrics = _portfolio_metrics(mu_risky_daily, cov_risky_daily, risky_ret)
                    final_ret, _ = _mix_with_deposit_by_target_vol(
                        risky_weights=risky_ret,
                        risky_metrics=risky_ret_metrics,
                        target_vol_max=profile["target_vol_max"],
                    )
                    final_ret = _filter_small_weights(final_ret)

                    final_portfolios = {
                        "markowitz_max_sharpe": final_sh,
                        "markowitz_max_return": final_ret,
                    }

                    mu_daily = mu_risky_daily.copy()
                    mu_daily.loc[DEPOSIT_UID] = deposit_daily_return

                    cov_daily = cov_risky_daily.copy()
                    cov_daily.loc[DEPOSIT_UID] = 0.0
                    cov_daily[DEPOSIT_UID] = 0.0
                    cov_daily.loc[DEPOSIT_UID, DEPOSIT_UID] = 1e-10

                    mu_daily = mu_daily.sort_index()
                    cov_daily = cov_daily.loc[mu_daily.index, mu_daily.index]

                for method_name, w in final_portfolios.items():
                    metrics = _portfolio_metrics(mu_daily, cov_daily, w)
                    portfolio_id = _next_portfolio_id(con)

                    deposit_weight = float(w.get(DEPOSIT_UID, 0.0))
                    num_assets = _count_effective_risky_assets(w)

                    con.execute(
                        """
                        INSERT INTO baseline_portfolios (
                            portfolio_id,
                            decision_date,
                            risk_profile_id,
                            method_name,
                            expected_return_portfolio,
                            expected_vol_portfolio,
                            expected_sharpe,
                            num_assets,
                            cash_weight,
                            optimization_status
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            portfolio_id,
                            decision_date,
                            profile["risk_profile_id"],
                            method_name,
                            metrics["exp_return_ann"],
                            metrics["vol_ann"],
                            metrics["sharpe_like"],
                            num_assets,
                            deposit_weight,
                            "ok",
                        ],
                    )

                    weights_payload = []

                    for uid, target_weight in w.items():
                        tw = float(target_weight)
                        if tw <= WEIGHT_EPS:
                            continue

                        if uid == DEPOSIT_UID:
                            instrument_id = deposit_instrument_id
                            asset_class = DEPOSIT_ASSET_CLASS
                            rp_asset = DEPOSIT_RISK_PROFILE
                        else:
                            instrument_id = next(x[0] for x in risky_selected if x[1] == uid)
                            asset_class = asset_class_by_uid[uid]
                            rp_asset = risk_by_uid.get(uid)

                        weights_payload.append(
                            (
                                portfolio_id,
                                decision_date,
                                profile["risk_profile_id"],
                                instrument_id,
                                tw,
                                asset_class,
                                rp_asset,
                            )
                        )

                    if weights_payload:
                        con.executemany(
                            """
                            INSERT INTO baseline_portfolio_weights (
                                portfolio_id,
                                decision_date,
                                risk_profile_id,
                                instrument_id,
                                target_weight,
                                asset_class,
                                risk_profile_asset
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            weights_payload,
                        )

                    total_portfolios += 1

                    if should_print_header:
                        print(
                            f"[ok] {decision_date} {profile['profile_name']} {method_name}: "
                            f"risky_assets={num_assets}, deposit={deposit_weight:.4f}, "
                            f"vol={metrics['vol_ann']:.4f}, ret={metrics['exp_return_ann']:.4f}"
                        )

        print("\nTotal baseline portfolios:", total_portfolios)
        con.execute("DETACH src")
    finally:
        con.close()



if __name__ == "__main__":
    build_markowitz_baseline()
