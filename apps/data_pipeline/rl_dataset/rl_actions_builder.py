from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection
from apps.data_pipeline.rl_dataset.rl_common import (
    drop_small_weights,
    get_deposit_instrument_id,
    load_instruments,
    normalize_weights,
    aggregate_asset_class_weights,
)


@dataclass(frozen=True)
class RLActionsParams:
    risk_shift: float = 0.10
    overwrite: bool = True
    deposit_instrument_id: int = 1705


def _series_from_df(g: pd.DataFrame, value_col: str) -> pd.Series:
    s = pd.Series(g[value_col].values, index=g["instrument_id"].values, dtype=float)
    return normalize_weights(s)


def _hold_current(current_w: pd.Series, baseline_w: pd.Series, deposit_id: int, shift: float) -> pd.Series:
    return current_w.copy()


def _rebalance_to_baseline(current_w: pd.Series, baseline_w: pd.Series, deposit_id: int, shift: float) -> pd.Series:
    return baseline_w.copy()

def compute_macro_bonus(asset_class: str, state_row: pd.Series) -> float:
    bonus = 0.0

    usd = float(state_row.get("usd_rub_ret_1p", 0.0) or 0.0)
    brent = float(state_row.get("brent_ret_1p", 0.0) or 0.0)
    gold = float(state_row.get("gold_ret_1p", 0.0) or 0.0)
    rate = float(state_row.get("cbr_key_rate", 0.0) or 0.0)

    if usd > 0:
        if asset_class == "fx":
            bonus += 0.05
        elif asset_class == "equity":
            bonus += 0.02

    if brent > 0 and asset_class == "equity":
        bonus += 0.03

    if gold > 0 and asset_class == "metal":
        bonus += 0.05

    if rate > 0.12 and asset_class in ("equity", "fx", "metal"):
        bonus -= 0.03

    return bonus

def compute_risk_penalty(asset_row: pd.Series, profile_row: pd.Series) -> float:
    penalty = 0.0

    asset_risk = asset_row.get("risk_score_asset")
    profile_id = int(profile_row["risk_profile_id"])

    if pd.isna(asset_risk):
        return 0.0

    asset_risk = float(asset_risk)

    if profile_id <= 1 and asset_risk >= 4:
        penalty += 0.10
    elif profile_id == 2 and asset_risk >= 5:
        penalty += 0.05

    return penalty

def compute_asset_scores(asset_df: pd.DataFrame, state_row: pd.Series, profile_row: pd.Series) -> pd.DataFrame:
    df = asset_df.copy()

    for col in ["ret_1p", "ret_3m", "vol_3m", "drawdown_3m", "liquidity_score"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # простая стандартизация внутри state
    def zscore(s: pd.Series) -> pd.Series:
        std = float(s.std())
        if std < 1e-12:
            return pd.Series(0.0, index=s.index)
        return (s - float(s.mean())) / std

    df["ret_1p_z"] = zscore(df["ret_1p"])
    df["ret_3m_z"] = zscore(df["ret_3m"])
    df["vol_3m_z"] = zscore(df["vol_3m"])
    df["drawdown_3m_z"] = zscore(df["drawdown_3m"])
    df["liquidity_z"] = zscore(df["liquidity_score"])

    df["macro_bonus"] = df["asset_class"].apply(lambda x: compute_macro_bonus(str(x), state_row))
    df["risk_penalty"] = df.apply(lambda row: compute_risk_penalty(row, profile_row), axis=1)

    df["score"] = (
        0.50 * df["ret_3m_z"]
        + 0.20 * df["ret_1p_z"]
        - 0.20 * df["vol_3m_z"]
        - 0.20 * df["drawdown_3m_z"]
        + 0.10 * df["liquidity_z"]
        + df["macro_bonus"]
        - df["risk_penalty"]
    )

    df["score"] = df["score"].fillna(0.0)
    return df

def estimate_portfolio_risk(asset_df: pd.DataFrame, weights: pd.Series) -> tuple[float, float]:
    df = asset_df.copy()
    df["w"] = df["instrument_id"].map(weights).fillna(0.0)

    vol = float((df["vol_3m"].fillna(0.0) * df["w"]).sum())
    dd = float((df["drawdown_3m"].fillna(0.0) * df["w"]).sum())

    return vol, dd


def is_feasible(vol: float, dd: float, profile_row: pd.Series) -> bool:
    vol_max = float(profile_row.get("target_vol_max", 10.0) or 10.0)
    dd_max = float(profile_row.get("max_drawdown_limit", 10.0) or 10.0)

    return (vol <= vol_max + 1e-9) and (dd <= dd_max + 1e-9)


def _decrease_risk(
    current_w: pd.Series,
    baseline_w: pd.Series,
    asset_df: pd.DataFrame,
    state_row: pd.Series,
    profile_row: pd.Series,
    deposit_id: int,
    shift: float,
    k_donors: int = 5,
) -> pd.Series:
    current_w = normalize_weights(current_w)

    df = compute_asset_scores(asset_df, state_row, profile_row).copy()
    df["current_weight"] = df["instrument_id"].map(current_w).fillna(0.0)

    out = current_w.copy()

    donors = df[
        (df["instrument_id"] != deposit_id)
        & (df["current_weight"] > 1e-12)
    ].sort_values("score", ascending=True).head(k_donors).copy()

    if donors.empty:
        return drop_small_weights(current_w)

    need_cut = float(shift)
    actually_cut = 0.0

    for _, row in donors.iterrows():
        iid = int(row["instrument_id"])
        can_take = float(out.get(iid, 0.0))
        take = min(can_take, need_cut)
        if take > 0:
            out.loc[iid] = can_take - take
            actually_cut += take
            need_cut -= take
        if need_cut <= 1e-12:
            break

    if actually_cut <= 1e-12:
        return drop_small_weights(current_w)

    out.loc[deposit_id] = float(out.get(deposit_id, 0.0)) + actually_cut
    out = normalize_weights(out)
    return drop_small_weights(out)


def _increase_risk(
    current_w: pd.Series,
    baseline_w: pd.Series,
    asset_df: pd.DataFrame,
    state_row: pd.Series,
    profile_row: pd.Series,
    deposit_id: int,
    shift: float,
    k_receivers: int = 5,
    k_donors: int = 5,
) -> pd.Series:
    current_w = normalize_weights(current_w)
    baseline_w = normalize_weights(baseline_w)

    df = compute_asset_scores(asset_df, state_row, profile_row).copy()

    df["current_weight"] = df["instrument_id"].map(current_w).fillna(0.0)
    df["baseline_weight"] = df["instrument_id"].map(baseline_w).fillna(0.0)

    deposit = float(current_w.get(deposit_id, 0.0))
    need = float(shift)

    # кому добавляем вес: top-k доступных risky активов
    receivers = df[
        (df["instrument_id"] != deposit_id)
        & (df["is_available"].fillna(False))
    ].sort_values("score", ascending=False).head(k_receivers).copy()

    if receivers.empty:
        return drop_small_weights(current_w)

    # сначала берем из депозита
    taken_from_deposit = min(deposit, need)
    need_remaining = need - taken_from_deposit

    out = current_w.copy()
    out.loc[deposit_id] = deposit - taken_from_deposit

    # если депозита не хватило, забираем из худших текущих активов
    if need_remaining > 1e-12:
        donors = df[
            (df["instrument_id"] != deposit_id)
            & (df["current_weight"] > 1e-12)
        ].sort_values("score", ascending=True).head(k_donors).copy()

        for _, row in donors.iterrows():
            iid = int(row["instrument_id"])
            can_take = float(out.get(iid, 0.0))
            take = min(can_take, need_remaining)
            if take > 0:
                out.loc[iid] = can_take - take
                need_remaining -= take
            if need_remaining <= 1e-12:
                break

    allocated = need - need_remaining
    if allocated <= 1e-12:
        return drop_small_weights(current_w)

    receiver_scores = receivers["score"].clip(lower=0.0)
    if float(receiver_scores.sum()) <= 1e-12:
        receiver_scores = pd.Series(1.0, index=receivers.index)

    receiver_scores = receiver_scores / float(receiver_scores.sum())

    for idx, row in receivers.iterrows():
        iid = int(row["instrument_id"])
        add_w = float(receiver_scores.loc[idx]) * allocated
        out.loc[iid] = float(out.get(iid, 0.0)) + add_w

    out = normalize_weights(out)
    out = drop_small_weights(out)

    vol, dd = estimate_portfolio_risk(df, out)
    if is_feasible(vol, dd, profile_row):
        return out

    # shrink-to-feasible
    local_shift = shift * 0.5
    if local_shift < 1e-4:
        return drop_small_weights(current_w)

    return _increase_risk(
        current_w=current_w,
        baseline_w=baseline_w,
        asset_df=asset_df,
        state_row=state_row,
        profile_row=profile_row,
        deposit_id=deposit_id,
        shift=local_shift,
        k_receivers=k_receivers,
        k_donors=k_donors,
    )

ACTION_BUILDERS = {
    "hold_current": _hold_current,
    "rebalance_to_baseline": _rebalance_to_baseline,
    "decrease_risk_10": _decrease_risk,
    "increase_risk_10": _increase_risk,
}


def load_states_df(con) -> pd.DataFrame:
    return con.execute("""
        SELECT
            state_id,
            decision_date,
            risk_profile_id,
            baseline_portfolio_id,
            current_equity_weight,
            current_bond_weight,
            current_fx_weight,
            current_metal_weight,
            current_cash_weight,
            target_vol_min,
            target_vol_max,
            usd_rub_ret_1p,
            brent_ret_1p,
            imoex_ret_1p,
            rgbi_ret_1p,
            gold_ret_1p,
            cbr_key_rate
        FROM rl_states
        ORDER BY state_id
    """).df()


def load_profiles_df(con) -> pd.DataFrame:
    return con.execute("""
        SELECT *
        FROM risk_profiles
        ORDER BY risk_profile_id
    """).df()


def load_state_features_df(con) -> pd.DataFrame:
    return con.execute("""
        SELECT
            sif.state_id,
            sif.instrument_id,
            sif.current_weight,
            sif.baseline_weight,
            sif.ret_1p,
            sif.ret_3m,
            sif.ret_6m,
            sif.vol_3m,
            sif.drawdown_3m,
            sif.liquidity_score,
            sif.risk_profile_asset,
            sif.risk_score_asset,
            sif.is_available,
            i.asset_class
        FROM rl_state_instrument_features sif
        JOIN instruments i
          ON i.instrument_id = sif.instrument_id
    """).df()


def load_baseline_weights_df(con) -> pd.DataFrame:
    return con.execute("""
        SELECT
            bp.portfolio_id,
            bpw.instrument_id,
            bpw.target_weight
        FROM baseline_portfolios bp
        JOIN baseline_portfolio_weights bpw
          ON bpw.portfolio_id = bp.portfolio_id
    """).df()

def build_rl_actions(
    params: RLActionsParams = RLActionsParams(),
    cfg: RLDatabaseConfig = RLDatabaseConfig(),
) -> None:
    con = get_connection(cfg.target_db)
    try:
        # очищаем action-слой перед пересборкой
        con.execute("DELETE FROM rl_target_portfolio_weights")
        con.execute("DELETE FROM rl_target_portfolios")
        con.execute("DELETE FROM rl_actions")

        states_df = load_states_df(con)
        profiles_df = load_profiles_df(con)
        state_features_df = load_state_features_df(con)
        baseline_weights_df = load_baseline_weights_df(con)

        # словари быстрого доступа
        profiles_by_id = {
            int(row["risk_profile_id"]): row
            for _, row in profiles_df.iterrows()
        }

        baseline_weights_by_portfolio = {}
        if not baseline_weights_df.empty:
            for portfolio_id, grp in baseline_weights_df.groupby("portfolio_id"):
                s = pd.Series(
                    grp["target_weight"].values,
                    index=grp["instrument_id"].astype(int).values,
                    dtype=float,
                )
                baseline_weights_by_portfolio[int(portfolio_id)] = normalize_weights(s)

        # asset_class_by_id нужен для aggregate_asset_class_weights
        asset_class_by_id = {}
        if not state_features_df.empty:
            tmp = (
                state_features_df[["instrument_id", "asset_class"]]
                .drop_duplicates()
                .copy()
            )
            asset_class_by_id = {
                int(row["instrument_id"]): str(row["asset_class"])
                for _, row in tmp.iterrows()
            }

        deposit_id = int(params.deposit_instrument_id)

        action_specs = [
            ("hold_current", None),
            ("rebalance_to_baseline", None),
            ("decrease_risk_10", float(params.risk_shift)),
            ("increase_risk_10", float(params.risk_shift)),
        ]

        n_states = 0
        n_actions = 0

        for _, state_row in states_df.iterrows():
            state_id = int(state_row["state_id"])
            decision_date = state_row["decision_date"]
            risk_profile_id = int(state_row["risk_profile_id"])
            profile_row = profiles_by_id[risk_profile_id]

            # features по активам для конкретного state
            state_asset_df = state_features_df[state_features_df["state_id"] == state_id].copy()

            # current weights по инструментам
            current_w = pd.Series(dtype=float)
            if not state_asset_df.empty:
                cw = state_asset_df[["instrument_id", "current_weight"]].copy()
                cw["instrument_id"] = cw["instrument_id"].astype(int)
                current_w = pd.Series(
                    cw["current_weight"].fillna(0.0).values,
                    index=cw["instrument_id"].values,
                    dtype=float,
                )
                current_w = drop_small_weights(normalize_weights(current_w))

            # baseline weights по baseline_portfolio_id
            baseline_portfolio_id = state_row["baseline_portfolio_id"]
            if pd.isna(baseline_portfolio_id):
                baseline_w = pd.Series({deposit_id: 1.0}, dtype=float)
            else:
                baseline_w = baseline_weights_by_portfolio.get(int(baseline_portfolio_id))
                if baseline_w is None or float(baseline_w.sum()) <= 1e-12:
                    baseline_w = pd.Series({deposit_id: 1.0}, dtype=float)
                else:
                    baseline_w = drop_small_weights(normalize_weights(baseline_w))

            # если current_w вдруг пустой — fallback в deposit
            if current_w.empty or float(current_w.sum()) <= 1e-12:
                current_w = pd.Series({deposit_id: 1.0}, dtype=float)

            for action_label, shift in action_specs:
                con.execute(
                    """
                    INSERT INTO rl_actions (
                        state_id,
                        decision_date,
                        action_label,
                        action_strength,
                        policy_probability
                    )
                    VALUES (?, ?, ?, ?, NULL)
                    """,
                    [
                        state_id,
                        decision_date,
                        action_label,
                        shift,
                    ],
                )
                action_id = con.execute("SELECT currval('rl_action_id_seq')").fetchone()[0]

                # === выбираем ровно одно действие ===
                if action_label == "hold_current":
                    target_w = drop_small_weights(current_w.copy())

                elif action_label == "rebalance_to_baseline":
                    target_w = drop_small_weights(baseline_w.copy())

                elif action_label == "increase_risk_10":
                    target_w = _increase_risk(
                        current_w=current_w,
                        baseline_w=baseline_w,
                        asset_df=state_asset_df,
                        state_row=state_row,
                        profile_row=profile_row,
                        deposit_id=deposit_id,
                        shift=float(shift),
                    )

                elif action_label == "decrease_risk_10":
                    target_w = _decrease_risk(
                        current_w=current_w,
                        baseline_w=baseline_w,
                        asset_df=state_asset_df,
                        state_row=state_row,
                        profile_row=profile_row,
                        deposit_id=deposit_id,
                        shift=float(shift),
                    )

                else:
                    raise ValueError(f"Unknown action_label: {action_label}")

                target_w = drop_small_weights(normalize_weights(target_w))

                cls = aggregate_asset_class_weights(target_w, asset_class_by_id, deposit_id)

                con.execute(
                    """
                    INSERT INTO rl_target_portfolios (
                        state_id,
                        action_id,
                        decision_date,
                        risk_profile_id,
                        cash_weight,
                        equity_weight,
                        bond_weight,
                        fx_weight,
                        metal_weight
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        state_id,
                        action_id,
                        decision_date,
                        risk_profile_id,
                        float(cls.get("cash", 0.0)),
                        float(cls.get("equity", 0.0)),
                        float(cls.get("bond", 0.0)),
                        float(cls.get("fx", 0.0)),
                        float(cls.get("metal", 0.0)),
                    ],
                )
                target_portfolio_id = con.execute(
                    "SELECT currval('rl_target_portfolio_id_seq')"
                ).fetchone()[0]

                for instrument_id, weight in target_w.items():
                    con.execute(
                        """
                        INSERT INTO rl_target_portfolio_weights (
                            target_portfolio_id,
                            instrument_id,
                            target_weight
                        )
                        VALUES (?, ?, ?)
                        """,
                        [
                            int(target_portfolio_id),
                            int(instrument_id),
                            float(weight),
                        ],
                    )

                n_actions += 1

            n_states += 1
            if n_states % 100 == 0:
                print(f"processed states: {n_states}, actions: {n_actions}")

        print(f"rl_actions built: states={n_states}, actions={n_actions}")

    finally:
        con.close()

if __name__ == "__main__":
    build_rl_actions()
