from __future__ import annotations

import random
import time
from datetime import date, timedelta
from typing import Optional

import pandas as pd

from apps.data_pipeline.moex.client import MoexISSClient


def _date_chunks(date_from: date, date_to: date, days: int):
    cur = date_from
    while cur <= date_to:
        chunk_end = min(date_to, cur + timedelta(days=days - 1))
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def fetch_daily_bars_for_instrument(
    client: MoexISSClient,
    *,
    engine: str,
    market: str,
    boardid: str,
    secid: str,
    date_from: date,
    date_to: date,
    chunk_days: int = 365,          # ✅ режем по годам
    sleep_min: float = 0.15,
    sleep_max: float = 0.45,
) -> pd.DataFrame:
    path = f"engines/{engine}/markets/{market}/boards/{boardid}/securities/{secid}/candles.json"

    all_parts = []

    for f, t in _date_chunks(date_from, date_to, days=chunk_days):
        params = {
            "from": f.isoformat(),
            "till": t.isoformat(),
            "interval": 24,
            "iss.meta": "off",
            "iss.only": "candles",
        }

        all_rows = []
        columns_ref: Optional[list[str]] = None

        for columns, rows in client.paged_table(
            path=path,
            table_name="candles",
            params=params,
            page_size=500,
            max_pages=200,
        ):
            columns_ref = columns
            all_rows.extend(rows)
            time.sleep(random.uniform(sleep_min, sleep_max))

        # пауза между чанками (важно на больших объёмах)
        time.sleep(random.uniform(sleep_min, sleep_max))

        if not all_rows:
            continue

        df = pd.DataFrame(all_rows, columns=columns_ref)
        df["begin_ts"] = pd.to_datetime(df["begin"])
        df["end_ts"] = pd.to_datetime(df["end"])
        df["dt"] = df["begin_ts"].dt.date

        out = df[
            ["dt", "open", "high", "low", "close", "volume", "value", "begin_ts", "end_ts"]
        ].copy()

        all_parts.append(out)

    if not all_parts:
        return pd.DataFrame()

    res = pd.concat(all_parts, ignore_index=True)
    # на всякий случай уберём дубли дат
    res = res.drop_duplicates(subset=["dt"], keep="last").sort_values("dt")
    return res