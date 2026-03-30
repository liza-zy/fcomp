from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection


# ----------------------------
# Params
# ----------------------------

@dataclass
class RLRewardsParams:
    deposit_instrument_id: int = 1705
    transaction_cost_bps: float = 15.0   # 15 bps = 0.15%
    turnover_penalty_lambda: float = 1.0
    vol_penalty_lambda: float = 2.0
    dd_penalty_lambda: float = 2.0
    band_penalty_lambda: float = 0.25


# ----------------------------
# Helpers
# ----------------------------

def normalize_weights(w: pd.Series) -> pd.Series:
    if w is None or len(w) == 0:
        return pd.Series(dtype=float)
    w = pd.to_numeric(w, errors="coerce").fillna(0.0)
    s = float(w.sum())
    if s <= 1e-12:
        return pd.Series(dtype=float)
    return w / s


def drop_small_weights(w: pd.Series, eps: float = 1e-10) -> pd.Series:
    if w is None or len(w) == 0:
        return pd.Series(dtype=float)
    w = w[w.abs() > eps].copy()
    return normalize_weights(w) if len(w) else pd.Series(dtype=float)


def aggregate_asset_class_weights(
    weights: pd.Series,
    asset_class_by_id: Dict[int, str],
    deposit_id: int,
) -> Dict[str, float]:
    out = {"cash": 0.0, "equity": 0.0, "bond": 0.0, "fx": 0.0, "metal": 0.0}

    for instrument_id, weight in weights.items():
        iid = int(instrument_id)
        w = float(weight)
        if iid == deposit_id:
            out["cash"] += w
            continue

        cls = str(asset_class_by_id.get(iid, "equity"))
        if cls not in out:
            cls = "equity"
        out[cls] += w

    return out


def detect_drawdown_col(profiles_df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "max_drawdown_limit",
        "max_drawdown_pct",
        "max_drawdown",
        "drawdown_limit",
        "max_dd",
    ]
    for c in candidates:
        if c in profiles_df.columns:
            return c
    return None


def load_returns_between_dates(
    con,
    date_from,
    date_to,
) -> pd.DataFrame:
    """
    Берем last close <= date_from и last close <= date_to по каждому инструменту,
    чтобы получить realized return на интервале ребалансировки.
    """
    return con.execute(
        """
        WITH p0 AS (
            SELECT
                instrument_id,
                close,
                ROW_NUMBER() OVER (
                    PARTITION BY instrument_id
                    ORDER BY date DESC
                ) AS rn
            FROM market_prices
            WHERE date <= ?
              AND close IS NOT NULL
        ),
        p1 AS (
            SELECT
                instrument_id,
                close,
                ROW_NUMBER() OVER (
                    PARTITION BY instrument_id
                    ORDER BY date DESC
                ) AS rn
            FROM market_prices
            WHERE date <= ?
              AND close IS NOT NULL
        )
        SELECT
            p1.instrument_id,
            p0.close AS close_from,
            p1.close AS close_to,
            CASE
                WHEN p0.close IS NOT NULL AND p0.close > 0 AND p1.close IS NOT NULL
                    THEN p1.close / p0.close - 1
                ELSE NULL
            END AS ret_1p
        FROM p0
        JOIN p1
          ON p0.instrument_id = p1.instrument_id
        WHERE p0.rn = 1
          AND p1.rn = 1
        """,
        [date_from, date_to],
    ).df()


def compute_deposit_return(cbr_key_rate: float, date_from, date_to) -> float:
    if pd.isna(cbr_key_rate):
        return 0.0

    days = max((pd.Timestamp(date_to) - pd.Timestamp(date_from)).days, 1)
    annual_rate = float(cbr_key_rate)

    # если в таблице уже хранится 0.12 как 12%, оставляем
    # если вдруг хранится 12.0, переводим
    if annual_rate > 1.0:
        annual_rate = annual_rate / 100.0

    return annual_rate * days / 365.0


def compute_turnover(current_w: pd.Series, target_w: pd.Series) -> float:
    idx = sorted(set(current_w.index) | set(target_w.index))
    cw = current_w.reindex(idx).fillna(0.0)
    tw = target_w.reindex(idx).fillna(0.0)
    return float((cw - tw).abs().sum())


def compute_portfolio_return(
    target_w: pd.Series,
    ret_map: Dict[int, float],
    deposit_id: int,
    deposit_ret: float,
) -> float:
    gross = 0.0
    for instrument_id, w in target_w.items():
        iid = int(instrument_id)
        if iid == deposit_id:
            r = deposit_ret
        else:
            r = float(ret_map.get(iid, 0.0) or 0.0)
        gross += float(w) * r
    return gross


def drift_weights_forward(
    target_w: pd.Series,
    ret_map: Dict[int, float],
    deposit_id: int,
    deposit_ret: float,
) -> pd.Series:
    vals = {}
    for instrument_id, w in target_w.items():
        iid = int(instrument_id)
        if iid == deposit_id:
            r = deposit_ret
        else:
            r = float(ret_map.get(iid, 0.0) or 0.0)
        vals[iid] = float(w) * (1.0 + r)

    s = pd.Series(vals, dtype=float)
    return drop_small_weights(s)


def estimate_portfolio_risk_from_state_features(
    state_feature_df: pd.DataFrame,
    weights: pd.Series,
) -> tuple[float, float]:
    df = state_feature_df.copy()
    df["w"] = df["instrument_id"].map(weights).fillna(0.0)

    vol = float((pd.to_numeric(df["vol_3m"], errors="coerce").fillna(0.0) * df["w"]).sum())
    dd = float((pd.to_numeric(df["drawdown_3m"], errors="coerce").fillna(0.0) * df["w"]).sum())

    return vol, dd


# ----------------------------
# Main builder
# ----------------------------

def build_rl_rewards(
    params: RLRewardsParams = RLRewardsParams(),
    cfg: RLDatabaseConfig = RLDatabaseConfig(),
) -> None:
    con = get_connection(cfg.target_db)

    try:
        # чистим перед пересборкой
        con.execute("DELETE FROM rl_rewards")
        con.execute("DELETE FROM rl_next_state_features")
        con.execute("DELETE FROM rl_transitions")

        states_df = con.execute("""
            SELECT *
            FROM rl_states
            ORDER BY risk_profile_id, decision_date, state_id
        """).df()


        states_df["decision_date"] = pd.to_datetime(states_df["decision_date"]).dt.date
        states_df["next_decision_date"] = (
            states_df.groupby("risk_profile_id")["decision_date"].shift(-1)
        )
        states_df["next_state_id_seq"] = (
            states_df.groupby("risk_profile_id")["state_id"].shift(-1)
        )

        actions_df = con.execute("""
            SELECT *
            FROM rl_actions
            ORDER BY action_id
        """).df()

        target_portfolios_df = con.execute("""
            SELECT *
            FROM rl_target_portfolios
        """).df()

        target_weights_df = con.execute("""
            SELECT *
            FROM rl_target_portfolio_weights
        """).df()

        state_features_df = con.execute("""
            SELECT
                sif.*,
                i.asset_class
            FROM rl_state_instrument_features sif
            JOIN instruments i
              ON i.instrument_id = sif.instrument_id
        """).df()

        profiles_df = con.execute("""
            SELECT *
            FROM risk_profiles
        """).df()

        drawdown_col = detect_drawdown_col(profiles_df)

        # lookup'ы
        states_by_id = {
            int(row["state_id"]): row
            for _, row in states_df.iterrows()
        }

        profiles_by_id = {
            int(row["risk_profile_id"]): row
            for _, row in profiles_df.iterrows()
        }


        asset_class_by_id = {
            int(row["instrument_id"]): str(row["asset_class"])
            for _, row in state_features_df[["instrument_id", "asset_class"]].drop_duplicates().iterrows()
        }

        state_features_by_state = {
            int(state_id): grp.copy()
            for state_id, grp in state_features_df.groupby("state_id")
        }

        current_weights_by_state = {}
        baseline_weights_by_state = {}
        for state_id, grp in state_features_df.groupby("state_id"):
            g = grp.copy()
            current_w = pd.Series(
                g["current_weight"].fillna(0.0).values,
                index=g["instrument_id"].astype(int).values,
                dtype=float,
            )
            baseline_w = pd.Series(
                g["baseline_weight"].fillna(0.0).values,
                index=g["instrument_id"].astype(int).values,
                dtype=float,
            )
            current_weights_by_state[int(state_id)] = drop_small_weights(current_w)
            baseline_weights_by_state[int(state_id)] = drop_small_weights(baseline_w)

        target_weights_by_portfolio = {}
        for target_portfolio_id, grp in target_weights_df.groupby("target_portfolio_id"):
            s = pd.Series(
                grp["target_weight"].fillna(0.0).values,
                index=grp["instrument_id"].astype(int).values,
                dtype=float,
            )
            target_weights_by_portfolio[int(target_portfolio_id)] = drop_small_weights(s)

        action_rows = (
            actions_df.merge(
                target_portfolios_df,
                on=["state_id", "action_id", "decision_date"],
                how="inner",
            )
            .sort_values(["state_id", "action_id"])
            .reset_index(drop=True)
        )

        reward_rows = []
        next_rows = []
        transition_rows = []

        # считаем reward по state группами, чтобы baseline брать внутри state
        for state_id, grp in action_rows.groupby("state_id"):
            state_id = int(state_id)
            state_row = states_by_id[state_id]
            risk_profile_id = int(state_row["risk_profile_id"])
            profile_row = profiles_by_id[risk_profile_id]

            decision_date = pd.Timestamp(state_row["decision_date"]).date()
            state_feature_df = state_features_by_state[state_id].copy()
            current_w = current_weights_by_state[state_id]

            next_decision_date = state_row["next_decision_date"]
            done = pd.isna(next_decision_date)

            if done:
                next_date = None
                ret_df = pd.DataFrame(columns=["instrument_id", "ret_1p"])
                ret_map = {}
                deposit_ret = 0.0
            else:
                next_date = next_decision_date
                ret_df = load_returns_between_dates(con, decision_date, next_date)
                ret_map = {
                    int(row["instrument_id"]): float(row["ret_1p"])
                    for _, row in ret_df.iterrows()
                    if not pd.isna(row["ret_1p"])
                }
                deposit_ret = compute_deposit_return(
                    cbr_key_rate=float(state_row.get("cbr_key_rate", 0.0) or 0.0),
                    date_from=decision_date,
                    date_to=next_date,
                 )

            tmp_rows = []

            for _, row in grp.iterrows():
                action_id = int(row["action_id"])
                target_portfolio_id = int(row["target_portfolio_id"])
                action_label = str(row["action_label"])

                target_w = target_weights_by_portfolio[target_portfolio_id]

                turnover = compute_turnover(current_w, target_w)
                transaction_cost = turnover * (params.transaction_cost_bps / 10000.0)

                gross_return_1p = compute_portfolio_return(
                    target_w=target_w,
                    ret_map=ret_map,
                    deposit_id=params.deposit_instrument_id,
                    deposit_ret=deposit_ret,
                )
                net_return_1p = gross_return_1p - transaction_cost

                realized_next_w = drift_weights_forward(
                    target_w=target_w,
                    ret_map=ret_map,
                    deposit_id=params.deposit_instrument_id,
                    deposit_ret=deposit_ret,
                )

                realized_vol_1p, realized_drawdown_1p = estimate_portfolio_risk_from_state_features(
                    state_feature_df=state_feature_df,
                    weights=realized_next_w,
                )

                vol_min = float(state_row.get("target_vol_min", 0.0) or 0.0)
                vol_max = float(state_row.get("target_vol_max", 999.0) or 999.0)

                vol_penalty = 0.0
                if realized_vol_1p < vol_min:
                    vol_penalty += (vol_min - realized_vol_1p)
                if realized_vol_1p > vol_max:
                    vol_penalty += (realized_vol_1p - vol_max)

                dd_penalty = 0.0
                if drawdown_col is not None:
                    dd_max = float(profile_row.get(drawdown_col, 999.0) or 999.0)
                    if realized_drawdown_1p > dd_max:
                        dd_penalty += (realized_drawdown_1p - dd_max)

                risk_band_violation = (vol_penalty > 0.0) or (dd_penalty > 0.0)

                risk_penalty_raw = vol_penalty + dd_penalty
                band_penalty_raw = 1.0 if risk_band_violation else 0.0


                tmp_rows.append(
                    {
                        "state_id": state_id,
                        "action_id": action_id,
                        "decision_date": decision_date,
                        "next_decision_date": next_date,
                        "action_label": action_label,
                        "gross_return_1p": gross_return_1p,
                        "net_return_1p": net_return_1p,
                        "turnover": turnover,
                        "transaction_cost": transaction_cost,
                        "realized_vol_1p": realized_vol_1p,
                        "realized_drawdown_1p": realized_drawdown_1p,
                        "vol_penalty": vol_penalty,
                        "dd_penalty": dd_penalty,
                        "risk_band_violation": bool(risk_band_violation),
                        "target_w": target_w,
                        "realized_next_w": realized_next_w,
                        "risk_penalty_raw": risk_penalty_raw,
                        "band_penalty_raw": band_penalty_raw,
                    }
                )

            tmp_df = pd.DataFrame(tmp_rows)

            # baseline внутри state = rebalance_to_baseline
            baseline_mask = tmp_df["action_label"] == "rebalance_to_baseline"
            if baseline_mask.any():
                baseline_net_return = float(tmp_df.loc[baseline_mask, "net_return_1p"].iloc[0])
            else:
                baseline_net_return = 0.0

            # canonical next state id по календарю и risk_profile
            if next_date is not None and not pd.isna(state_row["next_state_id_seq"]):
                next_state_id = int(state_row["next_state_id_seq"])
                next_state_row = states_by_id.get(next_state_id)
                next_baseline_w = baseline_weights_by_state.get(next_state_id, pd.Series(dtype=float)) if next_state_id is not None else pd.Series(dtype=float)
            else:
                next_state_id = None
                next_state_row = None
                next_baseline_w = pd.Series(dtype=float)

            for _, rr in tmp_df.iterrows():
                reward_return_component =10*( float(rr["net_return_1p"]) - baseline_net_return)
                reward_risk_penalty = 0.01*(
                    params.vol_penalty_lambda * float(rr["vol_penalty"])
                    + params.dd_penalty_lambda * float(rr["dd_penalty"])
                )
                reward_turnover_penalty = params.turnover_penalty_lambda * float(rr["transaction_cost"])
                reward_band_penalty = 0.01 * float(rr["band_penalty_raw"])

                reward_total = (
                    reward_return_component
                    - reward_risk_penalty
                    - reward_turnover_penalty
                    - reward_band_penalty
                )

                # next state features action-specific
                realized_next_w = rr["realized_next_w"]
                cls = aggregate_asset_class_weights(
                    realized_next_w,
                    asset_class_by_id=asset_class_by_id,
                    deposit_id=params.deposit_instrument_id,
                )

                if next_state_row is not None:
                    deviation_from_baseline_l1_next = float(
                        (
                            realized_next_w.reindex(sorted(set(realized_next_w.index) | set(next_baseline_w.index))).fillna(0.0)
                            - next_baseline_w.reindex(sorted(set(realized_next_w.index) | set(next_baseline_w.index))).fillna(0.0)
                        ).abs().sum()
                    )
                    usd_next = float(next_state_row.get("usd_rub_ret_1p", 0.0) or 0.0)
                    brent_next = float(next_state_row.get("brent_ret_1p", 0.0) or 0.0)
                    imoex_next = float(next_state_row.get("imoex_ret_1p", 0.0) or 0.0)
                    rgbi_next = float(next_state_row.get("rgbi_ret_1p", 0.0) or 0.0)
                    gold_next = float(next_state_row.get("gold_ret_1p", 0.0) or 0.0)
                    rate_next = float(next_state_row.get("cbr_key_rate", 0.0) or 0.0)
                else:
                    deviation_from_baseline_l1_next = 0.0
                    usd_next = brent_next = imoex_next = rgbi_next = gold_next = rate_next = 0.0

                next_rows.append(
                    {
                        "state_id": int(rr["state_id"]),
                        "action_id": int(rr["action_id"]),
                        "next_decision_date": rr["next_decision_date"],
                        "portfolio_value_next": float(state_row.get("portfolio_value", 1.0) or 1.0) * (1.0 + float(rr["net_return_1p"])),
                        "current_equity_weight_next": float(cls.get("equity", 0.0)),
                        "current_bond_weight_next": float(cls.get("bond", 0.0)),
                        "current_fx_weight_next": float(cls.get("fx", 0.0)),
                        "current_metal_weight_next": float(cls.get("metal", 0.0)),
                        "current_cash_weight_next": float(cls.get("cash", 0.0)),
                        "portfolio_realized_vol_3m_next": float(rr["realized_vol_1p"]),
                        "portfolio_drawdown_3m_next": float(rr["realized_drawdown_1p"]),
                        "portfolio_turnover_prev_next": float(rr["turnover"]),
                        "deviation_from_baseline_l1_next": deviation_from_baseline_l1_next,
                        "usd_rub_ret_1p_next": usd_next,
                        "brent_ret_1p_next": brent_next,
                        "imoex_ret_1p_next": imoex_next,
                        "rgbi_ret_1p_next": rgbi_next,
                        "gold_ret_1p_next": gold_next,
                        "cbr_key_rate_next": rate_next,
                    }
                )

                reward_rows.append(
                    {
                        "state_id": int(rr["state_id"]),
                        "action_id": int(rr["action_id"]),
                        "decision_date": rr["decision_date"],
                        "next_decision_date": rr["next_decision_date"],
                        "gross_return_1p": float(rr["gross_return_1p"]),
                        "net_return_1p": float(rr["net_return_1p"]),
                        "turnover": float(rr["turnover"]),
                        "transaction_cost": float(rr["transaction_cost"]),
                        "realized_vol_1p": float(rr["realized_vol_1p"]),
                        "realized_drawdown_1p": float(rr["realized_drawdown_1p"]),
                        "risk_band_violation": bool(rr["risk_band_violation"]),
                        "reward_return_component": float(reward_return_component),
                        "reward_risk_penalty": float(reward_risk_penalty),
                        "reward_turnover_penalty": float(reward_turnover_penalty),
                        "reward_band_penalty": float(reward_band_penalty),
                        "reward_total": float(reward_total),
                    }
                )

                transition_rows.append(
                    {
                        "state_id": int(rr["state_id"]),
                        "action_id": int(rr["action_id"]),
                        "next_state_id": int(next_state_id) if next_state_id is not None else None,
                        "reward_total": float(reward_total),
                        "done": bool(next_state_id is None),
                    }
                )

        reward_df = pd.DataFrame(reward_rows)
        next_df = pd.DataFrame(next_rows)
        transition_df = pd.DataFrame(transition_rows)

        # insert rl_rewards
        for _, row in reward_df.iterrows():
            con.execute(
                """
                INSERT INTO rl_rewards (
                    state_id,
                    action_id,
                    decision_date,
                    next_decision_date,
                    gross_return_1p,
                    net_return_1p,
                    turnover,
                    transaction_cost,
                    realized_vol_1p,
                    realized_drawdown_1p,
                    risk_band_violation,
                    reward_return_component,
                    reward_risk_penalty,
                    reward_turnover_penalty,
                    reward_band_penalty,
                    reward_total
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    int(row["state_id"]),
                    int(row["action_id"]),
                    row["decision_date"],
                    row["next_decision_date"],
                    float(row["gross_return_1p"]),
                    float(row["net_return_1p"]),
                    float(row["turnover"]),
                    float(row["transaction_cost"]),
                    float(row["realized_vol_1p"]),
                    float(row["realized_drawdown_1p"]),
                    bool(row["risk_band_violation"]),
                    float(row["reward_return_component"]),
                    float(row["reward_risk_penalty"]),
                    float(row["reward_turnover_penalty"]),
                    float(row["reward_band_penalty"]),
                    float(row["reward_total"]),
                ],
            )

        # insert rl_next_state_features
        for _, row in next_df.iterrows():
            con.execute(
                """
                INSERT INTO rl_next_state_features (
                    state_id,
                    action_id,
                    next_decision_date,
                    portfolio_value_next,
                    current_equity_weight_next,
                    current_bond_weight_next,
                    current_fx_weight_next,
                    current_metal_weight_next,
                    current_cash_weight_next,
                    portfolio_realized_vol_3m_next,
                    portfolio_drawdown_3m_next,
                    portfolio_turnover_prev_next,
                    deviation_from_baseline_l1_next,
                    usd_rub_ret_1p_next,
                    brent_ret_1p_next,
                    imoex_ret_1p_next,
                    rgbi_ret_1p_next,
                    gold_ret_1p_next,
                    cbr_key_rate_next
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    int(row["state_id"]),
                    int(row["action_id"]),
                    row["next_decision_date"],
                    float(row["portfolio_value_next"]),
                    float(row["current_equity_weight_next"]),
                    float(row["current_bond_weight_next"]),
                    float(row["current_fx_weight_next"]),
                    float(row["current_metal_weight_next"]),
                    float(row["current_cash_weight_next"]),
                    float(row["portfolio_realized_vol_3m_next"]),
                    float(row["portfolio_drawdown_3m_next"]),
                    float(row["portfolio_turnover_prev_next"]),
                    float(row["deviation_from_baseline_l1_next"]),
                    float(row["usd_rub_ret_1p_next"]),
                    float(row["brent_ret_1p_next"]),
                    float(row["imoex_ret_1p_next"]),
                    float(row["rgbi_ret_1p_next"]),
                    float(row["gold_ret_1p_next"]),
                    float(row["cbr_key_rate_next"]),
                ],
            )

        # insert rl_transitions
        for _, row in transition_df.iterrows():
            con.execute(
                """
                INSERT INTO rl_transitions (
                    state_id,
                    action_id,
                    next_state_id,
                    reward_total,
                    done
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    int(row["state_id"]),
                    int(row["action_id"]),
                    None if pd.isna(row["next_state_id"]) else int(row["next_state_id"]),
                    float(row["reward_total"]),
                    bool(row["done"]),
                ],
            )

        print(f"rl_rewards built: {len(reward_df)}")
        print(f"rl_next_state_features built: {len(next_df)}")
        print(f"rl_transitions built: {len(transition_df)}")

    finally:
        con.close()


if __name__ == "__main__":
    build_rl_rewards()
