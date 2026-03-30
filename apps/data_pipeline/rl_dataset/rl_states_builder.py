from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection
from apps.data_pipeline.rl_dataset.rl_common import (
    aggregate_asset_class_weights,
    drift_weights_forward,
    get_deposit_instrument_id,
    get_interval_daily_portfolio_returns,
    get_macro_row,
    load_baseline_portfolio_ids,
    load_baseline_weights,
    load_decision_calendar,
    load_instruments,
    load_risk_profiles,
    l1_turnover,
    max_drawdown_from_daily,
    normalize_weights,
    realized_vol_annual_from_daily,
)


@dataclass(frozen=True)
class RLStatesParams:
    baseline_method_name: str = "markowitz_max_sharpe"
    overwrite: bool = True


def build_rl_states(
    params: RLStatesParams = RLStatesParams(),
    cfg: RLDatabaseConfig = RLDatabaseConfig(),
) -> None:
    con = get_connection(cfg.target_db)
    try:
        if params.overwrite:
            con.execute("DELETE FROM rl_state_instrument_features")
            con.execute("DELETE FROM rl_states")

        profiles = load_risk_profiles(con)
        calendar = load_decision_calendar(con)
        instr = load_instruments(con)

        deposit_instrument_id = get_deposit_instrument_id(con)
        asset_class_by_id = dict(zip(instr["instrument_id"], instr["asset_class"]))

        baseline = load_baseline_weights(con, params.baseline_method_name)
        baseline_ids = load_baseline_portfolio_ids(con, params.baseline_method_name)

        state_rows = []
        state_instr_rows = []

        for rp_id, prof in profiles.items():
            rp_calendar = calendar.sort_values("decision_date").reset_index(drop=True)

            current_weights = None
            portfolio_value = 1.0
            prev_turnover = 0.0

            for idx, row in rp_calendar.iterrows():
                d = pd.Timestamp(row["decision_date"])
                prev_d = pd.Timestamp(row["prev_decision_date"]) if pd.notna(row["prev_decision_date"]) else None
                split = row["split"]

                baseline_w = baseline.get((d, rp_id))
                baseline_portfolio_id = baseline_ids.get((d, rp_id))

                if baseline_w is None:
                    baseline_w = pd.Series({deposit_instrument_id: 1.0}, dtype=float)
                else:
                    baseline_w = normalize_weights(baseline_w)

                if idx == 0:
                    current_weights = baseline_w.copy()
                    portfolio_value = 1.0
                    prev_turnover = 0.0
                else:
                    current_weights, gross_ret = drift_weights_forward(
                        con=con,
                        current_target_weights=current_weights,
                        deposit_instrument_id=deposit_instrument_id,
                        date_from=prev_d,
                        date_to=d,
                    )
                    portfolio_value *= (1.0 + gross_ret)

                classes = aggregate_asset_class_weights(
                    current_weights, asset_class_by_id, deposit_instrument_id
                )
                deviation_l1 = l1_turnover(current_weights, baseline_w)

                trailing_start = d - pd.Timedelta(days=95)
                daily_rets = get_interval_daily_portfolio_returns(
                    con=con,
                    weights_by_instrument_id=current_weights,
                    deposit_instrument_id=deposit_instrument_id,
                    date_from=trailing_start,
                    date_to=d,
                )
                trailing_3m = daily_rets.tail(63)

                port_vol_3m = realized_vol_annual_from_daily(trailing_3m)
                port_dd_3m = max_drawdown_from_daily(trailing_3m)

                macro_vals = get_macro_row(con, d)

                con.execute(
                    """
                    INSERT INTO rl_states (
                        decision_date,
                        split,
                        risk_profile_id,
                        baseline_portfolio_id,
                        portfolio_value,
                        current_equity_weight,
                        current_bond_weight,
                        current_fx_weight,
                        current_metal_weight,
                        current_cash_weight,
                        portfolio_realized_vol_3m,
                        portfolio_drawdown_3m,
                        portfolio_turnover_prev,
                        deviation_from_baseline_l1,
                        target_vol_min,
                        target_vol_max,
                        usd_rub_ret_1p,
                        brent_ret_1p,
                        imoex_ret_1p,
                        rgbi_ret_1p,
                        gold_ret_1p,
                        cbr_key_rate
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        d.date(),
                        split,
                        rp_id,
                        baseline_portfolio_id,
                        float(portfolio_value),
                        classes["equity"],
                        classes["bond"],
                        classes["fx"],
                        classes["metal"],
                        classes["cash"],
                        port_vol_3m,
                        port_dd_3m,
                        float(prev_turnover),
                        float(deviation_l1),
                        prof["target_vol_min"],
                        prof["target_vol_max"],
                        macro_vals[0],
                        macro_vals[1],
                        macro_vals[2],
                        macro_vals[3],
                        macro_vals[4],
                        macro_vals[5],
                    ],
                )

                state_id = con.execute("SELECT currval('rl_state_id_seq')").fetchone()[0]

                feat_df = con.execute(
                    """
                    SELECT
                        instrument_id,
                        ret_1p,
                        ret_3m,
                        ret_6m,
                        vol_3m,
                        drawdown_3m,
                        liquidity_score,
                        risk_profile_asset,
                        risk_score_asset,
                        is_available
                    FROM instrument_features
                    WHERE decision_date = ?
                    """,
                    [d.date()],
                ).df()
                feat_map = feat_df.set_index("instrument_id").to_dict("index") if not feat_df.empty else {}

                all_ids = sorted(set(current_weights.index) | set(baseline_w.index))
                for iid in all_ids:
                    cur_w = float(current_weights.get(iid, 0.0))
                    base_w = float(baseline_w.get(iid, 0.0))

                    if int(iid) == int(deposit_instrument_id):
                        con.execute(
                            """
                            INSERT INTO rl_state_instrument_features (
                                state_id,
                                instrument_id,
                                current_weight,
                                baseline_weight,
                                weight_diff,
                                ret_1p,
                                ret_3m,
                                ret_6m,
                                vol_3m,
                                drawdown_3m,
                                liquidity_score,
                                risk_profile_asset,
                                risk_score_asset,
                                is_available
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            [
                                int(state_id),
                                int(iid),
                                cur_w,
                                base_w,
                                cur_w - base_w,
                                0.0,
                                0.0,
                                0.0,
                                0.0,
                                0.0,
                                None,
                                "Ultra-Conservative",
                                0.0,
                                True,
                            ],
                        )
                        continue

                    f = feat_map.get(int(iid), {})
                    con.execute(
                        """
                        INSERT INTO rl_state_instrument_features (
                            state_id,
                            instrument_id,
                            current_weight,
                            baseline_weight,
                            weight_diff,
                            ret_1p,
                            ret_3m,
                            ret_6m,
                            vol_3m,
                            drawdown_3m,
                            liquidity_score,
                            risk_profile_asset,
                            risk_score_asset,
                            is_available
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            int(state_id),
                            int(iid),
                            cur_w,
                            base_w,
                            cur_w - base_w,
                            f.get("ret_1p"),
                            f.get("ret_3m"),
                            f.get("ret_6m"),
                            f.get("vol_3m"),
                            f.get("drawdown_3m"),
                            f.get("liquidity_score"),
                            f.get("risk_profile_asset"),
                            f.get("risk_score_asset"),
                            bool(f.get("is_available")) if f.get("is_available") is not None else False,
                        ],
                    )

                prev_turnover = float(deviation_l1)
                # после даты решения считаем, что reference policy
                # ребалансируется в baseline на этой же дате,
                # и именно этот портфель идет в следующий интервал
                current_weights = baseline_w.copy()

        print("rl_states:", con.execute("SELECT COUNT(*) FROM rl_states").fetchone()[0])
        print(
            "rl_state_instrument_features:",
            con.execute("SELECT COUNT(*) FROM rl_state_instrument_features").fetchone()[0],
        )

    finally:
        con.close()


if __name__ == "__main__":
    build_rl_states()
