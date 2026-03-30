from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class RLDatabaseConfig:
    source_db: Path = Path("data_lake/moex.duckdb")
    target_db: Path = Path("data_lake/rl_training.duckdb")


def get_connection(db_path: Path | str):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def create_rl_training_schema(cfg: RLDatabaseConfig = RLDatabaseConfig()) -> None:
    con = get_connection(cfg.target_db)
    try:
        # -----------------------------
        # Base dictionaries / market / baseline
        # -----------------------------
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS instruments (
                instrument_id BIGINT PRIMARY KEY,
                instrument_uid VARCHAR UNIQUE,
                secid VARCHAR,
                name VARCHAR,
                isin VARCHAR,
                asset_class VARCHAR,
                sector VARCHAR,
                boardid VARCHAR,
                currency VARCHAR,
                is_qualified_only BOOLEAN,
                first_trade_date DATE,
                last_trade_date DATE,
                is_active BOOLEAN
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_profiles (
                risk_profile_id INTEGER PRIMARY KEY,
                profile_name VARCHAR,
                target_vol_min DOUBLE,
                target_vol_max DOUBLE,
                max_drawdown_target DOUBLE,
                max_equity_weight DOUBLE,
                max_bond_weight DOUBLE,
                max_fx_weight DOUBLE,
                max_metal_weight DOUBLE,
                max_single_asset_weight DOUBLE,
                risk_penalty_lambda DOUBLE,
                turnover_penalty_lambda DOUBLE
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS decision_calendar (
                decision_date DATE PRIMARY KEY,
                prev_decision_date DATE,
                next_decision_date DATE,
                split VARCHAR,
                window_id INTEGER,
                rebalance_index INTEGER
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS market_prices (
                date DATE,
                instrument_id BIGINT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                value_traded DOUBLE,
                num_trades DOUBLE,
                is_traded BOOLEAN,
                PRIMARY KEY (date, instrument_id)
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS macro_factors (
                date DATE PRIMARY KEY,
                usd_rub DOUBLE,
                brent DOUBLE,
                imoex DOUBLE,
                rgbi DOUBLE,
                gold DOUBLE,
                cbr_key_rate DOUBLE,
                money_market_rate DOUBLE,
                cpi_yoy DOUBLE,
                usd_rub_ret_1p DOUBLE,
                brent_ret_1p DOUBLE,
                imoex_ret_1p DOUBLE,
                rgbi_ret_1p DOUBLE,
                gold_ret_1p DOUBLE
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS instrument_features (
                decision_date DATE,
                instrument_id BIGINT,
                ret_1p DOUBLE,
                ret_1m DOUBLE,
                ret_3m DOUBLE,
                ret_6m DOUBLE,
                ret_12m DOUBLE,
                vol_3m DOUBLE,
                vol_6m DOUBLE,
                vol_12m DOUBLE,
                drawdown_3m DOUBLE,
                drawdown_6m DOUBLE,
                liquidity_score DOUBLE,
                has_min_history BOOLEAN,
                is_available BOOLEAN,
                risk_profile_asset VARCHAR,
                risk_score_asset DOUBLE,
                ann_vol_pct DOUBLE,
                max_drawdown_pct DOUBLE,
                risk_profile_method VARCHAR,
                is_eligible_for_universe BOOLEAN,
                PRIMARY KEY (decision_date, instrument_id)
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS baseline_universe (
                decision_date DATE,
                risk_profile_id INTEGER,
                instrument_id BIGINT,
                is_in_universe BOOLEAN,
                has_min_history BOOLEAN,
                passes_liquidity_filter BOOLEAN,
                passes_qualification_filter BOOLEAN,
                inclusion_reason VARCHAR,
                exclusion_reason VARCHAR,
                PRIMARY KEY (decision_date, risk_profile_id, instrument_id)
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS baseline_portfolios (
                portfolio_id BIGINT PRIMARY KEY,
                decision_date DATE,
                risk_profile_id INTEGER,
                method_name VARCHAR,
                expected_return_portfolio DOUBLE,
                expected_vol_portfolio DOUBLE,
                expected_sharpe DOUBLE,
                num_assets INTEGER,
                cash_weight DOUBLE,
                optimization_status VARCHAR
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS baseline_portfolio_weights (
                portfolio_id BIGINT,
                decision_date DATE,
                risk_profile_id INTEGER,
                instrument_id BIGINT,
                target_weight DOUBLE,
                asset_class VARCHAR,
                risk_profile_asset VARCHAR,
                PRIMARY KEY (portfolio_id, instrument_id)
            )
            """
        )

        # -----------------------------
        # RL layer
        # -----------------------------
        con.execute(
            """
            CREATE SEQUENCE IF NOT EXISTS rl_state_id_seq START 1
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rl_states (
                state_id BIGINT PRIMARY KEY DEFAULT nextval('rl_state_id_seq'),
                decision_date DATE,
                split VARCHAR,
                risk_profile_id INTEGER,
                baseline_portfolio_id BIGINT,
                portfolio_value DOUBLE,
                current_equity_weight DOUBLE,
                current_bond_weight DOUBLE,
                current_fx_weight DOUBLE,
                current_metal_weight DOUBLE,
                current_cash_weight DOUBLE,
                portfolio_realized_vol_3m DOUBLE,
                portfolio_drawdown_3m DOUBLE,
                portfolio_turnover_prev DOUBLE,
                deviation_from_baseline_l1 DOUBLE,
                target_vol_min DOUBLE,
                target_vol_max DOUBLE,
                usd_rub_ret_1p DOUBLE,
                brent_ret_1p DOUBLE,
                imoex_ret_1p DOUBLE,
                rgbi_ret_1p DOUBLE,
                gold_ret_1p DOUBLE,
                cbr_key_rate DOUBLE
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rl_state_instrument_features (
                state_id BIGINT,
                instrument_id BIGINT,
                current_weight DOUBLE,
                baseline_weight DOUBLE,
                weight_diff DOUBLE,
                ret_1p DOUBLE,
                ret_3m DOUBLE,
                ret_6m DOUBLE,
                vol_3m DOUBLE,
                drawdown_3m DOUBLE,
                liquidity_score DOUBLE,
                risk_profile_asset VARCHAR,
                risk_score_asset DOUBLE,
                is_available BOOLEAN,
                PRIMARY KEY (state_id, instrument_id)
            )
            """
        )

        con.execute(
            """
            CREATE SEQUENCE IF NOT EXISTS rl_action_id_seq START 1
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rl_actions (
                action_id BIGINT PRIMARY KEY DEFAULT nextval('rl_action_id_seq'),
                state_id BIGINT,
                decision_date DATE,
                action_label VARCHAR,
                action_strength DOUBLE,
                policy_probability DOUBLE
            )
            """
        )

        con.execute(
            """
            CREATE SEQUENCE IF NOT EXISTS rl_target_portfolio_id_seq START 1
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rl_target_portfolios (
                target_portfolio_id BIGINT PRIMARY KEY DEFAULT nextval('rl_target_portfolio_id_seq'),
                state_id BIGINT,
                action_id BIGINT,
                decision_date DATE,
                risk_profile_id INTEGER,
                cash_weight DOUBLE,
                equity_weight DOUBLE,
                bond_weight DOUBLE,
                fx_weight DOUBLE,
                metal_weight DOUBLE
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rl_target_portfolio_weights (
                target_portfolio_id BIGINT,
                instrument_id BIGINT,
                target_weight DOUBLE,
                PRIMARY KEY (target_portfolio_id, instrument_id)
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rl_rewards (
                state_id BIGINT,
                action_id BIGINT,
                decision_date DATE,
                next_decision_date DATE,
                gross_return_1p DOUBLE,
                net_return_1p DOUBLE,
                turnover DOUBLE,
                transaction_cost DOUBLE,
                realized_vol_1p DOUBLE,
                realized_drawdown_1p DOUBLE,
                risk_band_violation BOOLEAN,
                reward_return_component DOUBLE,
                reward_risk_penalty DOUBLE,
                reward_turnover_penalty DOUBLE,
                reward_band_penalty DOUBLE,
                reward_total DOUBLE,
                PRIMARY KEY (state_id, action_id)
            )
            """
        )

        # action-conditioned next state
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rl_next_state_features (
                state_id BIGINT,
                action_id BIGINT,
                next_decision_date DATE,
                portfolio_value_next DOUBLE,
                current_equity_weight_next DOUBLE,
                current_bond_weight_next DOUBLE,
                current_fx_weight_next DOUBLE,
                current_metal_weight_next DOUBLE,
                current_cash_weight_next DOUBLE,
                portfolio_realized_vol_3m_next DOUBLE,
                portfolio_drawdown_3m_next DOUBLE,
                portfolio_turnover_prev_next DOUBLE,
                deviation_from_baseline_l1_next DOUBLE,
                usd_rub_ret_1p_next DOUBLE,
                brent_ret_1p_next DOUBLE,
                imoex_ret_1p_next DOUBLE,
                rgbi_ret_1p_next DOUBLE,
                gold_ret_1p_next DOUBLE,
                cbr_key_rate_next DOUBLE,
                PRIMARY KEY (state_id, action_id)
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS rl_transitions (
                state_id BIGINT,
                action_id BIGINT,
                next_state_id BIGINT,
                reward_total DOUBLE,
                done BOOLEAN,
                PRIMARY KEY (state_id, action_id)
            )
            """
        )

        # -----------------------------
        # Backtest layer (can stay for later, but schema is ready)
        # -----------------------------
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id BIGINT PRIMARY KEY,
                strategy_name VARCHAR,
                risk_profile_id INTEGER,
                split VARCHAR,
                window_id INTEGER,
                start_date DATE,
                end_date DATE,
                rebalance_frequency VARCHAR,
                cost_model VARCHAR,
                agent_version VARCHAR
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_portfolio_path (
                run_id BIGINT,
                date DATE,
                portfolio_value DOUBLE,
                gross_return DOUBLE,
                net_return DOUBLE,
                cum_return DOUBLE,
                drawdown DOUBLE,
                realized_vol_3m DOUBLE,
                turnover DOUBLE,
                transaction_cost DOUBLE,
                in_target_risk_band BOOLEAN,
                PRIMARY KEY (run_id, date)
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_trades (
                run_id BIGINT,
                trade_date DATE,
                decision_date DATE,
                instrument_id BIGINT,
                trade_weight_change DOUBLE,
                trade_value DOUBLE,
                transaction_cost DOUBLE,
                trade_reason VARCHAR
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_metrics (
                run_id BIGINT PRIMARY KEY,
                sharpe DOUBLE,
                sortino DOUBLE,
                cagr DOUBLE,
                max_drawdown DOUBLE,
                annual_volatility DOUBLE,
                avg_turnover DOUBLE,
                total_turnover DOUBLE,
                total_transaction_cost DOUBLE,
                share_in_target_risk_band DOUBLE,
                num_rebalances INTEGER
            )
            """
        )

        print("RL training schema is ready:", cfg.target_db)

    finally:
        con.close()


def drop_rl_layer_tables(cfg: RLDatabaseConfig = RLDatabaseConfig()) -> None:
    con = get_connection(cfg.target_db)
    try:
        tables = [
            "rl_transitions",
            "rl_next_state_features",
            "rl_rewards",
            "rl_target_portfolio_weights",
            "rl_target_portfolios",
            "rl_actions",
            "rl_state_instrument_features",
            "rl_states",
        ]
        for t in tables:
            con.execute(f"DROP TABLE IF EXISTS {t}")
        con.execute("DROP SEQUENCE IF EXISTS rl_state_id_seq")
        con.execute("DROP SEQUENCE IF EXISTS rl_action_id_seq")
        con.execute("DROP SEQUENCE IF EXISTS rl_target_portfolio_id_seq")
        print("Dropped RL layer tables")
    finally:
        con.close()


if __name__ == "__main__":
    create_rl_training_schema()
