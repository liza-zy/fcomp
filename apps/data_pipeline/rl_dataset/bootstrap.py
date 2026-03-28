from __future__ import annotations

from pathlib import Path
import duckdb

from apps.data_pipeline.rl_dataset.schema import (
    RLDatabaseConfig,
    ensure_rl_schema,
    get_connection,
)


BOOTSTRAPPABLE_TARGET_TABLES = [
    "instrument_features",
    "market_prices",
    "instruments",
]


def _attach_source(con: duckdb.DuckDBPyConnection, source_db: Path) -> None:
    con.execute(f"ATTACH '{source_db.as_posix()}' AS src (READ_ONLY)")


def _detach_source(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.execute("DETACH src")
    except Exception:
        pass


def truncate_bootstrappable_tables(cfg: RLDatabaseConfig = RLDatabaseConfig()) -> None:
    con = get_connection(cfg.target_db)
    try:
        for table_name in BOOTSTRAPPABLE_TARGET_TABLES:
            con.execute(f"DELETE FROM {table_name}")
    finally:
        con.close()


def bootstrap_instruments(cfg: RLDatabaseConfig = RLDatabaseConfig()) -> None:
    con = get_connection(cfg.target_db)
    try:
        _attach_source(con, cfg.source_db)

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
            WITH src_instruments AS (
                SELECT
                    r.instrument_uid,
                    r.asset_class,
                    r.boardid,
                    r.secid,
                    COALESCE(r.name, r.shortname, r.secid) AS resolved_name,
                    r.isin,
                    r.currencyid,
                    r.investor_access,
                    r.is_traded,
                    s.first_dt,
                    s.last_dt
                FROM src.ref_instruments r
                LEFT JOIN src.instrument_stats_1d s
                    ON s.instrument_uid = r.instrument_uid
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY instrument_uid) AS instrument_id,
                instrument_uid,
                secid,
                resolved_name AS name,
                isin,
                asset_class,
                NULL AS sector,
                boardid,
                currencyid AS currency,
                CASE
                    WHEN investor_access = 'qualified_only' THEN TRUE
                    ELSE FALSE
                END AS is_qualified_only,
                first_dt AS first_trade_date,
                last_dt AS last_trade_date,
                COALESCE(is_traded, TRUE) AS is_active
            FROM src_instruments
            """
        )

        _detach_source(con)
    finally:
        con.close()


def bootstrap_market_prices(cfg: RLDatabaseConfig = RLDatabaseConfig()) -> None:
    con = get_connection(cfg.target_db)
    try:
        _attach_source(con, cfg.source_db)

        con.execute(
            """
            INSERT INTO market_prices (
                date,
                instrument_id,
                open,
                high,
                low,
                close,
                volume,
                value_traded,
                num_trades,
                is_traded
            )
            SELECT
                b.dt AS date,
                i.instrument_id,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                b.value::DOUBLE AS value_traded,
                NULL::DOUBLE AS num_trades,
                CASE WHEN b.close IS NOT NULL THEN TRUE ELSE FALSE END AS is_traded
            FROM src.bars_1d b
            JOIN instruments i
              ON i.instrument_uid = b.instrument_uid
            """
        )

        _detach_source(con)
    finally:
        con.close()


def bootstrap_instrument_features(cfg: RLDatabaseConfig = RLDatabaseConfig()) -> None:
    con = get_connection(cfg.target_db)
    try:
        _attach_source(con, cfg.source_db)

        con.execute(
            """
            INSERT INTO instrument_features (
                decision_date,
                instrument_id,
                ret_1p,
                ret_1m,
                ret_3m,
                ret_6m,
                ret_12m,
                vol_3m,
                vol_6m,
                vol_12m,
                drawdown_3m,
                drawdown_6m,
                liquidity_score,
                has_min_history,
                is_available,
                risk_profile_asset,
                risk_score_asset,
                ann_vol_pct,
                max_drawdown_pct,
                risk_profile_method,
                is_eligible_for_universe
            )
            WITH ranked_features AS (
                SELECT
                    f.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY f.instrument_uid
                        ORDER BY f.dt
                    ) AS obs_num
                FROM src.features_1d f
            )
            SELECT
                f.dt AS decision_date,
                i.instrument_id,

                CASE
                    WHEN f.logret_1d IS NOT NULL THEN exp(f.logret_1d) - 1
                    ELSE NULL
                END AS ret_1p,

                f.mom_20 AS ret_1m,
                f.mom_60 AS ret_3m,
                NULL::DOUBLE AS ret_6m,
                NULL::DOUBLE AS ret_12m,

                f.vol_60 AS vol_3m,
                NULL::DOUBLE AS vol_6m,
                NULL::DOUBLE AS vol_12m,

                f.maxdd_60 AS drawdown_3m,
                NULL::DOUBLE AS drawdown_6m,

                CASE
                    WHEN f.adv_value_20 IS NOT NULL AND f.adv_value_20 > 0
                        THEN ln(1 + f.adv_value_20)
                    ELSE NULL
                END AS liquidity_score,

                CASE WHEN obs_num >= 252 THEN TRUE ELSE FALSE END AS has_min_history,
                CASE WHEN f.close IS NOT NULL THEN TRUE ELSE FALSE END AS is_available,

                NULL::VARCHAR AS risk_profile_asset,
                NULL::DOUBLE AS risk_score_asset,
                NULL::DOUBLE AS ann_vol_pct,
                NULL::DOUBLE AS max_drawdown_pct,
                NULL::VARCHAR AS risk_profile_method,

                CASE
                    WHEN obs_num >= 252
                     AND f.close IS NOT NULL
                     AND COALESCE(f.adv_value_20, 0) > 0
                    THEN TRUE
                    ELSE FALSE
                END AS is_eligible_for_universe
            FROM ranked_features f
            JOIN instruments i
              ON i.instrument_uid = f.instrument_uid
            """
        )

        _detach_source(con)
    finally:
        con.close()


def print_bootstrap_summary(cfg: RLDatabaseConfig = RLDatabaseConfig()) -> None:
    con = get_connection(cfg.target_db)
    try:
        print("\n=== BOOTSTRAP SUMMARY ===")
        for table_name in ["instruments", "market_prices", "instrument_features"]:
            cnt = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"{table_name}: {cnt}")
    finally:
        con.close()


def run_bootstrap(cfg: RLDatabaseConfig = RLDatabaseConfig()) -> None:
    ensure_rl_schema(cfg)
    truncate_bootstrappable_tables(cfg)
    bootstrap_instruments(cfg)
    bootstrap_market_prices(cfg)
    bootstrap_instrument_features(cfg)
    print_bootstrap_summary(cfg)


if __name__ == "__main__":
    run_bootstrap()
