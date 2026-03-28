from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import duckdb

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection


@dataclass(frozen=True)
class DecisionCalendarParams:
    start_date: date
    end_date: date
    rebalance_frequency: str = "month_end"  # month_end | week_end
    train_ratio: float = 0.70
    validation_ratio: float = 0.15


def build_decision_calendar(
    params: DecisionCalendarParams,
    cfg: RLDatabaseConfig = RLDatabaseConfig(),
) -> None:
    con = get_connection(cfg.target_db)
    try:
        con.execute("DELETE FROM decision_calendar")

        if params.rebalance_frequency == "month_end":
            query = f"""
            WITH dates AS (
                SELECT DISTINCT date::DATE AS dt
                FROM read_parquet([])
            )
            """
        # DuckDB не умеет магически брать даты отсюда, поэтому берем из market_prices
        if params.rebalance_frequency == "month_end":
            rows = con.execute(
                """
                WITH base AS (
                    SELECT DISTINCT date
                    FROM market_prices
                    WHERE date BETWEEN ? AND ?
                ),
                ranked AS (
                    SELECT
                        date,
                        date_trunc('month', date) AS month_bucket,
                        ROW_NUMBER() OVER (
                            PARTITION BY date_trunc('month', date)
                            ORDER BY date DESC
                        ) AS rn
                    FROM base
                )
                SELECT date
                FROM ranked
                WHERE rn = 1
                ORDER BY date
                """,
                [params.start_date, params.end_date],
            ).fetchall()
        elif params.rebalance_frequency == "week_end":
            rows = con.execute(
                """
                WITH base AS (
                    SELECT DISTINCT date
                    FROM market_prices
                    WHERE date BETWEEN ? AND ?
                ),
                ranked AS (
                    SELECT
                        date,
                        date_trunc('week', date) AS week_bucket,
                        ROW_NUMBER() OVER (
                            PARTITION BY date_trunc('week', date)
                            ORDER BY date DESC
                        ) AS rn
                    FROM base
                )
                SELECT date
                FROM ranked
                WHERE rn = 1
                ORDER BY date
                """,
                [params.start_date, params.end_date],
            ).fetchall()
        else:
            raise ValueError(f"Unsupported rebalance_frequency={params.rebalance_frequency}")

        decision_dates = [r[0] for r in rows]
        n = len(decision_dates)
        if n < 3:
            raise ValueError("Not enough decision dates to create train/validation/test split")

        train_end = int(n * params.train_ratio)
        val_end = int(n * (params.train_ratio + params.validation_ratio))

        payload = []
        for i, d in enumerate(decision_dates):
            if i < train_end:
                split = "train"
            elif i < val_end:
                split = "validation"
            else:
                split = "test"

            prev_d = decision_dates[i - 1] if i > 0 else None
            next_d = decision_dates[i + 1] if i < n - 1 else None

            payload.append((d, prev_d, next_d, split, 1, i))

        con.executemany(
            """
            INSERT INTO decision_calendar (
                decision_date,
                prev_decision_date,
                next_decision_date,
                split,
                window_id,
                rebalance_index
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    finally:
        con.close()
