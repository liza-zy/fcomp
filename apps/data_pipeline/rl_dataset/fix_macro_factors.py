from __future__ import annotations

import pandas as pd

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection


FILL_COLS = [
    "usd_rub",
    "gold",
    "cbr_key_rate",
    "money_market_rate",
]

RET_BASE_COLS = [
    "usd_rub",
    "brent",
    "imoex",
    "rgbi",
    "gold",
]


def _fill_initial_nulls_only(s: pd.Series) -> pd.Series:
    s = s.copy()
    first_valid = s.first_valid_index()
    if first_valid is None:
        return s
    first_val = s.loc[first_valid]
    mask = s.index < first_valid
    s.loc[mask] = first_val
    return s


def fix_macro_factors(cfg: RLDatabaseConfig = RLDatabaseConfig()) -> None:
    con = get_connection(cfg.target_db)
    try:
        df = con.execute(
            """
            SELECT *
            FROM macro_factors
            ORDER BY date
            """
        ).df()

        if df.empty:
            raise ValueError("macro_factors is empty")

        for col in FILL_COLS:
            if col in df.columns:
                df[col] = _fill_initial_nulls_only(df[col])

        for col in RET_BASE_COLS:
            ret_col = f"{col}_ret_1p"
            if col in df.columns:
                df[ret_col] = pd.to_numeric(df[col], errors="coerce").pct_change()

        con.execute("DELETE FROM macro_factors")
        con.register("tmp_macro_fixed", df)

        con.execute(
            """
            INSERT INTO macro_factors
            SELECT
                date,
                usd_rub,
                brent,
                imoex,
                rgbi,
                gold,
                cbr_key_rate,
                money_market_rate,
                cpi_yoy,
                usd_rub_ret_1p,
                brent_ret_1p,
                imoex_ret_1p,
                rgbi_ret_1p,
                gold_ret_1p
            FROM tmp_macro_fixed
            """
        )

        print("macro_factors fixed:", len(df))
        for row in con.execute(
            """
            SELECT date, usd_rub, gold, cbr_key_rate, money_market_rate
            FROM macro_factors
            ORDER BY date
            LIMIT 10
            """
        ).fetchall():
            print(row)

    finally:
        con.close()


if __name__ == "__main__":
    fix_macro_factors()
