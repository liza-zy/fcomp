from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Iterable

import numpy as np
import pandas as pd

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection


TRADING_DAYS_PER_YEAR = 252


@dataclass(frozen=True)
class AssetRiskPTIParams:
    lookback: int = 252
    min_obs: int = 63
    method_name: str = "rolling_252d_vol_dd_v1"
    only_missing: bool = False


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
            WHERE EXISTS (
                SELECT 1
                FROM instrument_features f
                WHERE f.decision_date = dc.decision_date
                  AND f.risk_score_asset IS NULL
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
            target_vol_max,
            max_drawdown_target
        FROM risk_profiles
        ORDER BY risk_profile_id
        """
    ).fetchall()

    if not rows:
        raise ValueError("risk_profiles is empty; run seed_risk_profiles first")

    return [
        {
            "risk_profile_id": int(r[0]),
            "profile_name": str(r[1]),
            "target_vol_max": float(r[2]) if r[2] is not None else None,
            "max_drawdown_target": float(r[3]) if r[3] is not None else None,
        }
        for r in rows
    ]


def _assign_profile(
    ann_vol_frac: float | None,
    max_dd_frac: float | None,
    profiles: list[dict],
) -> tuple[str | None, float | None]:
    if ann_vol_frac is None or max_dd_frac is None or not profiles:
        return None, None

    for p in profiles:
        vol_ok = p["target_vol_max"] is None or ann_vol_frac <= p["target_vol_max"]
        dd_ok = p["max_drawdown_target"] is None or max_dd_frac <= p["max_drawdown_target"]
        if vol_ok and dd_ok:
            return p["profile_name"], float(p["risk_profile_id"])

    last = profiles[-1]
    return last["profile_name"], float(last["risk_profile_id"])


def _max_drawdown_from_log_returns(logrets: np.ndarray) -> float | None:
    if logrets.size == 0:
        return None

    wealth = np.exp(np.cumsum(logrets))
    peak = np.maximum.accumulate(wealth)
    drawdowns = 1.0 - wealth / peak
    return float(np.max(drawdowns))


def update_asset_risk_pti(
    params: AssetRiskPTIParams = AssetRiskPTIParams(),
    cfg: RLDatabaseConfig = RLDatabaseConfig(),
) -> None:
    con = get_connection(cfg.target_db)
    try:
        con.execute(f"ATTACH '{cfg.source_db.as_posix()}' AS src (READ_ONLY)")

        profiles = _load_profiles(con)
        decision_dates = _load_decision_dates(con, params.only_missing)

        if not decision_dates:
            print("No decision dates to process")
            return

        total_updates = 0

        for decision_date in decision_dates:
            df = con.execute(
                """
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
                    JOIN instruments i
                      ON i.instrument_uid = r.instrument_uid
                    WHERE r.dt <= ?
                )
                SELECT
                    w.instrument_uid,
                    i.instrument_id,
                    w.dt,
                    w.logret_1d
                FROM windowed_returns w
                JOIN instruments i
                  ON i.instrument_uid = w.instrument_uid
                WHERE w.rn <= ?
                ORDER BY w.instrument_uid, w.dt
                """,
                [decision_date, params.lookback],
            ).df()
            if df.empty:
                print(f"[skip] {decision_date}: no trailing returns")
                continue

            updates = []
            grouped = df.groupby(["instrument_uid", "instrument_id"], sort=False)

            for (_, instrument_id), g in grouped:
                vals = g["logret_1d"].dropna().to_numpy(dtype=float)
                n_obs = int(vals.size)

                if n_obs < params.min_obs:
                    ann_vol_pct = None
                    max_dd_pct = None
                    profile_name = None
                    risk_score = None
                else:
                    ann_vol_frac = float(np.std(vals, ddof=1) * sqrt(TRADING_DAYS_PER_YEAR))
                    max_dd_frac = _max_drawdown_from_log_returns(vals)
                    profile_name, risk_score = _assign_profile(ann_vol_frac, max_dd_frac, profiles)

                    ann_vol_pct = ann_vol_frac * 100.0
                    max_dd_pct = max_dd_frac * 100.0 if max_dd_frac is not None else None

                updates.append(
                    (
                        profile_name,
                        risk_score,
                        ann_vol_pct,
                        max_dd_pct,
                        params.method_name,
                        decision_date,
                        int(instrument_id),
                    )
                )

            con.executemany(
                """
                UPDATE instrument_features
                SET
                    risk_profile_asset = ?,
                    risk_score_asset = ?,
                    ann_vol_pct = ?,
                    max_drawdown_pct = ?,
                    risk_profile_method = ?,
                    is_eligible_for_universe =
                        CASE
                            WHEN has_min_history = TRUE
                             AND is_available = TRUE
                             AND ? IS NOT NULL
                            THEN TRUE
                            ELSE FALSE
                        END
                WHERE decision_date = ?
                  AND instrument_id = ?
                """,
                [
                    (u[0], u[1], u[2], u[3], u[4], u[1], u[5], u[6])
                    for u in updates
                ],
            )

            total_updates += len(updates)
            print(f"[ok] {decision_date}: updated {len(updates)} instruments")

        print("Total asset-risk updates:", total_updates)
        con.execute("DETACH src")
    finally:
        con.close()


if __name__ == "__main__":
    update_asset_risk_pti()
