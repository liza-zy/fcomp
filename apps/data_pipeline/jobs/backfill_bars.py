"""Job: backfill daily bars for a date range."""

from __future__ import annotations

import argparse
from datetime import date
import duckdb
import pandas as pd

from apps.data_pipeline.moex.client import MoexISSClient
from apps.data_pipeline.moex.bars_daily import fetch_daily_bars_for_instrument
from apps.data_pipeline.duckdb_store import upsert_bars_1d, ensure_bars_1d_schema

DB_PATH = "data_lake/moex.duckdb"


def parse_date(s: str) -> date:
    return pd.to_datetime(s).date()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="date_from", required=True)
    p.add_argument("--to", dest="date_to", required=True)
    p.add_argument("--asset-classes", default="equity,fund,fx,metal")
    p.add_argument("--limit", type=int, default=0, help="0 = без лимита (для теста поставь 10/50)")
    p.add_argument("--sleep-min", type=float, default=0.15)
    p.add_argument("--sleep-max", type=float, default=0.45)
    p.add_argument("--chunk-days", type=int, default=365)
    args = p.parse_args()

    date_from = parse_date(args.date_from)
    date_to = parse_date(args.date_to)
    asset_classes = [x.strip() for x in args.asset_classes.split(",") if x.strip()]

    con = duckdb.connect(DB_PATH)
    ensure_bars_1d_schema(con)

    q = f"""
      select instrument_uid, asset_class, engine, market, boardid, secid
      from ref_instruments_universe_v2
      where asset_class in ({",".join(["?"]*len(asset_classes))})
      order by asset_class, secid
    """
    uni = con.execute(q, asset_classes).fetch_df()

    if args.limit and args.limit > 0:
        uni = uni.head(args.limit)

    client = MoexISSClient()

    total = 0
    for i, r in uni.iterrows():
        instrument_uid = r["instrument_uid"]
        asset_class = r["asset_class"]
        engine = r["engine"]
        market = r["market"]
        boardid = r["boardid"]
        secid = r["secid"]

        print(f"[{i+1}/{len(uni)}] {asset_class} {secid} ({boardid}) ...")

        bars = fetch_daily_bars_for_instrument(
            client,
            engine=engine,
            market=market,
            boardid=boardid,
            secid=secid,
            date_from=date_from,
            date_to=date_to,
            sleep_min=args.sleep_min,
            sleep_max=args.sleep_max,
            chunk_days=args.chunk_days,
        )

        if bars.empty:
            continue

        bars["instrument_uid"] = instrument_uid
        bars["asset_class"] = asset_class
        bars["secid"] = secid
        bars["boardid"] = boardid

        n = upsert_bars_1d(con, bars)
        total += n
        print(f"   saved: {n} rows (total {total})")

    print("✅ Done. Total rows saved:", total)


if __name__ == "__main__":
    main()