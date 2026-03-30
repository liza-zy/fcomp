from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection


Freq = Literal["month_end", "week_end", "biweekly"]


@dataclass(frozen=True)
class DecisionCalendarParams:
    start_date: date | None = None
    end_date: date | None = None
    rebalance_frequency: Freq = "biweekly"
    train_ratio: float = 0.70
    validation_ratio: float = 0.15
    window_id: int = 1


def _resolve_bounds(con, start_date: date | None, end_date: date | None) -> tuple[date, date]:
    row = con.execute(
        """
        SELECT MIN(date), MAX(date)
        FROM market_prices
        """
    ).fetchone()
    db_min, db_max = row
    if db_min is None or db_max is None:
        raise ValueError("market_prices is empty; run bootstrap first")
    return (start_date or db_min, end_date or db_max)


def build_decision_calendar(
    params: DecisionCalendarParams = DecisionCalendarParams(),
    cfg: RLDatabaseConfig = RLDatabaseConfig(),
) -> None:
    con = get_connection(cfg.target_db)
    try:
        start_date, end_date = _resolve_bounds(con, params.start_date, params.end_date)

        con.execute("DELETE FROM decision_calendar")

        if params.rebalance_frequency == "month_end":
            sql = """
            WITH base AS (
                SELECT DISTINCT date
                FROM market_prices
                WHERE date BETWEEN ? AND ?
            ),
            ranked AS (
                SELECT
                    date,
                    date_trunc('month', date) AS bucket,
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
            """

        elif params.rebalance_frequency == "week_end":
            sql = """
            WITH base AS (
                SELECT DISTINCT date
                FROM market_prices
                WHERE date BETWEEN ? AND ?
            ),
            ranked AS (
                SELECT
                    date,
                    date_trunc('week', date) AS bucket,
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
            """

        elif params.rebalance_frequency == "biweekly":
            sql = """
            WITH base AS (
                SELECT DISTINCT date
                FROM market_prices
                WHERE date BETWEEN ? AND ?
            ),
            week_ends AS (
                SELECT
                    date,
                    date_trunc('week', date) AS week_bucket,
                    ROW_NUMBER() OVER (
                        PARTITION BY date_trunc('week', date)
                        ORDER BY date DESC
                    ) AS rn
                FROM base
            ),
            weekly_points AS (
                SELECT
                    date,
                    ROW_NUMBER() OVER (ORDER BY date) AS week_index
                FROM week_ends
                WHERE rn = 1
            )
            SELECT date
            FROM weekly_points
            WHERE (week_index - 1) % 2 = 0
            ORDER BY date
            """

        else:
            raise ValueError(f"Unsupported rebalance_frequency={params.rebalance_frequency}")

        rows = con.execute(sql, [start_date, end_date]).fetchall()
        decision_dates = [r[0] for r in rows]

        if len(decision_dates) < 3:
            raise ValueError("Not enough decision dates")

        n = len(decision_dates)
        train_end = max(1, int(n * params.train_ratio))
        val_end = max(train_end + 1, int(n * (params.train_ratio + params.validation_ratio)))
        val_end = min(val_end, n - 1)

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

            payload.append((d, prev_d, next_d, split, params.window_id, i))

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

        print("Built decision_calendar:", len(payload))
        for row in con.execute(
            """
            SELECT *
            FROM decision_calendar
            ORDER BY decision_date
            LIMIT 20
            """
        ).fetchall():
            print(row)

    finally:
        con.close()


if __name__ == "__main__":
    build_decision_calendar()
