from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import duckdb


ROOT = Path(__file__).resolve().parents[3]
DATA_LAKE_DIR = ROOT / "data_lake"
SOURCE_DB = DATA_LAKE_DIR / "moex.duckdb"
TARGET_DB = DATA_LAKE_DIR / "rl_training.duckdb"


@dataclass(frozen=True)
class RLDatabaseConfig:
    source_db: Path = SOURCE_DB
    target_db: Path = TARGET_DB


def get_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def ensure_rl_schema(cfg: RLDatabaseConfig = RLDatabaseConfig()) -> None:
    con = get_connection(cfg.target_db)
    try:
        con.execute("PRAGMA enable_object_cache=true;")

        # =========================
        # 1. Справочники
        # =========================
        con.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            instrument_id BIGINT PRIMARY KEY,
            instrument_uid VARCHAR NOT NULL UNIQUE,
            secid VARCHAR NOT NULL,
            name VARCHAR,
            isin VARCHAR,
            asset_class VARCHAR NOT NULL,
            sector VARCHAR,
            boardid VARCHAR,
            currency VARCHAR,
            is_qualified_only BOOLEAN DEFAULT FALSE,
            first_trade_date DATE,
            last_trade_date DATE,
            is_active BOOLEAN DEFAULT TRUE
        );
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS risk_profiles (
            risk_profile_id INTEGER PRIMARY KEY,
            profile_name VARCHAR NOT NULL UNIQUE,
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
        );
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS decision_calendar (
            decision_date DATE PRIMARY KEY,
            prev_decision_date DATE,
            next_decision_date DATE,
            split VARCHAR NOT NULL,
            window_id INTEGER,
            rebalance_index INTEGER NOT NULL
        );
        """)

        # =========================
        # 2. Рыночные данные
        # =========================
        con.execute("""
        CREATE TABLE IF NOT EXISTS market_prices (
            date DATE NOT NULL,
            instrument_id BIGINT NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            value_traded DOUBLE,
            num_trades DOUBLE,
            is_traded BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (date, instrument_id)
        );
        """)

        con.execute("""
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
        );
        """)

        # ВАЖНО:
        # Я объединил сюда и признаки, и риск-профиль актива на дату,
        # потому что отдельная instrument_risk_profile дублирует данные.
        con.execute("""
        CREATE TABLE IF NOT EXISTS instrument_features (
            decision_date DATE NOT NULL,
            instrument_id BIGINT NOT NULL,
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
            has_min_history BOOLEAN DEFAULT FALSE,
            is_available BOOLEAN DEFAULT FALSE,

            risk_profile_asset VARCHAR,
            risk_score_asset DOUBLE,
            ann_vol_pct DOUBLE,
            max_drawdown_pct DOUBLE,
            risk_profile_method VARCHAR,
            is_eligible_for_universe BOOLEAN DEFAULT FALSE,

            PRIMARY KEY (decision_date, instrument_id)
        );
        """)

        # Если все-таки хочешь отдельную таблицу — раскомментируй:
        # con.execute("""
        # CREATE TABLE IF NOT EXISTS instrument_risk_profile (
        #     decision_date DATE NOT NULL,
        #     instrument_id BIGINT NOT NULL,
        #     risk_profile_asset VARCHAR,
        #     risk_score_asset DOUBLE,
        #     ann_vol_pct DOUBLE,
        #     max_drawdown_pct DOUBLE,
        #     method VARCHAR,
        #     is_eligible_for_universe BOOLEAN DEFAULT FALSE,
        #     PRIMARY KEY (decision_date, instrument_id)
        # );
        # """)

        # =========================
        # 3. Baseline
        # =========================
        con.execute("""
        CREATE TABLE IF NOT EXISTS baseline_universe (
            decision_date DATE NOT NULL,
            risk_profile_id INTEGER NOT NULL,
            instrument_id BIGINT NOT NULL,
            is_in_universe BOOLEAN DEFAULT FALSE,
            has_min_history BOOLEAN DEFAULT FALSE,
            passes_liquidity_filter BOOLEAN DEFAULT FALSE,
            passes_qualification_filter BOOLEAN DEFAULT FALSE,
            inclusion_reason VARCHAR,
            exclusion_reason VARCHAR,
            PRIMARY KEY (decision_date, risk_profile_id, instrument_id)
        );
        """)

        con.execute("""
        CREATE SEQUENCE IF NOT EXISTS baseline_portfolio_id_seq START 1;
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS baseline_portfolios (
            portfolio_id BIGINT PRIMARY KEY DEFAULT nextval('baseline_portfolio_id_seq'),
            decision_date DATE NOT NULL,
            risk_profile_id INTEGER NOT NULL,
            method_name VARCHAR NOT NULL,
            expected_return_portfolio DOUBLE,
            expected_vol_portfolio DOUBLE,
            expected_sharpe DOUBLE,
            num_assets INTEGER,
            cash_weight DOUBLE,
            optimization_status VARCHAR
        );
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS baseline_portfolio_weights (
            portfolio_id BIGINT NOT NULL,
            decision_date DATE NOT NULL,
            risk_profile_id INTEGER NOT NULL,
            instrument_id BIGINT NOT NULL,
            target_weight DOUBLE NOT NULL,
            asset_class VARCHAR,
            risk_profile_asset VARCHAR,
            PRIMARY KEY (portfolio_id, instrument_id)
        );
        """)

        # =========================
        # 4. RL dataset
        # =========================
        con.execute("""
        CREATE SEQUENCE IF NOT EXISTS rl_state_id_seq START 1;
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS rl_states (
            state_id BIGINT PRIMARY KEY DEFAULT nextval('rl_state_id_seq'),
            decision_date DATE NOT NULL,
            split VARCHAR NOT NULL,
            risk_profile_id INTEGER NOT NULL,
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
            cbr_key_rate DOUBLE,
            UNIQUE (decision_date, risk_profile_id, baseline_portfolio_id)
        );
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS rl_state_instrument_features (
            state_id BIGINT NOT NULL,
            instrument_id BIGINT NOT NULL,
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
            is_available BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (state_id, instrument_id)
        );
        """)

        con.execute("""
        CREATE SEQUENCE IF NOT EXISTS rl_action_id_seq START 1;
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS rl_actions (
            action_id BIGINT PRIMARY KEY DEFAULT nextval('rl_action_id_seq'),
            state_id BIGINT NOT NULL,
            decision_date DATE NOT NULL,
            action_label VARCHAR NOT NULL,
            action_strength DOUBLE,
            policy_probability DOUBLE
        );
        """)

        con.execute("""
        CREATE SEQUENCE IF NOT EXISTS rl_target_portfolio_id_seq START 1;
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS rl_target_portfolios (
            target_portfolio_id BIGINT PRIMARY KEY DEFAULT nextval('rl_target_portfolio_id_seq'),
            state_id BIGINT NOT NULL,
            action_id BIGINT NOT NULL,
            decision_date DATE NOT NULL,
            risk_profile_id INTEGER NOT NULL,
            cash_weight DOUBLE,
            equity_weight DOUBLE,
            bond_weight DOUBLE,
            fx_weight DOUBLE,
            metal_weight DOUBLE
        );
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS rl_target_portfolio_weights (
            target_portfolio_id BIGINT NOT NULL,
            instrument_id BIGINT NOT NULL,
            target_weight DOUBLE NOT NULL,
            PRIMARY KEY (target_portfolio_id, instrument_id)
        );
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS rl_rewards (
            state_id BIGINT NOT NULL,
            action_id BIGINT NOT NULL,
            decision_date DATE NOT NULL,
            next_decision_date DATE,
            gross_return_1p DOUBLE,
            net_return_1p DOUBLE,
            turnover DOUBLE,
            transaction_cost DOUBLE,
            realized_vol_1p DOUBLE,
            risk_band_violation BOOLEAN DEFAULT FALSE,
            reward_return_component DOUBLE,
            reward_risk_penalty DOUBLE,
            reward_turnover_penalty DOUBLE,
            reward_band_penalty DOUBLE,
            reward_total DOUBLE,
            PRIMARY KEY (state_id, action_id)
        );
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS rl_transitions (
            state_id BIGINT NOT NULL,
            action_id BIGINT NOT NULL,
            next_state_id BIGINT,
            reward_total DOUBLE,
            done BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (state_id, action_id)
        );
        """)

        # =========================
        # 5. Backtest
        # =========================
        con.execute("""
        CREATE SEQUENCE IF NOT EXISTS backtest_run_id_seq START 1;
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS backtest_runs (
            run_id BIGINT PRIMARY KEY DEFAULT nextval('backtest_run_id_seq'),
            strategy_name VARCHAR NOT NULL,
            risk_profile_id INTEGER NOT NULL,
            split VARCHAR NOT NULL,
            window_id INTEGER,
            start_date DATE,
            end_date DATE,
            rebalance_frequency VARCHAR,
            cost_model VARCHAR,
            agent_version VARCHAR
        );
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS backtest_portfolio_path (
            run_id BIGINT NOT NULL,
            date DATE NOT NULL,
            portfolio_value DOUBLE,
            gross_return DOUBLE,
            net_return DOUBLE,
            cum_return DOUBLE,
            drawdown DOUBLE,
            realized_vol_3m DOUBLE,
            turnover DOUBLE,
            transaction_cost DOUBLE,
            in_target_risk_band BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (run_id, date)
        );
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS backtest_trades (
            run_id BIGINT NOT NULL,
            trade_date DATE NOT NULL,
            decision_date DATE NOT NULL,
            instrument_id BIGINT NOT NULL,
            trade_weight_change DOUBLE,
            trade_value DOUBLE,
            transaction_cost DOUBLE,
            trade_reason VARCHAR,
            PRIMARY KEY (run_id, trade_date, instrument_id)
        );
        """)

        con.execute("""
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
        );
        """)
    finally:
        con.close()


if __name__ == "__main__":
    ensure_rl_schema()
    print(f"RL schema created in {TARGET_DB}")
