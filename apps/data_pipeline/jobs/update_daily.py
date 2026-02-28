"""Job: daily incremental update (last N days, upsert)."""

from __future__ import annotations

from datetime import date, timedelta
import random
import time

import duckdb

from apps.data_pipeline.moex.client import MoexISSClient
from apps.data_pipeline.moex.bars_daily import fetch_daily_bars_for_instrument
from apps.data_pipeline.duckdb_store import ensure_bars_1d_schema, upsert_bars_1d

DB_PATH = "data_lake/moex.duckdb"


def _sleep_between_requests():
    # MOEX иногда режет частые запросы — делаем рандомную паузу
    time.sleep(random.uniform(0.15, 0.65))


def main():
    con = duckdb.connect(DB_PATH)
    ensure_bars_1d_schema(con)

    uni = con.sql("""
      select instrument_uid, asset_class, engine, market, boardid, secid
      from ref_instruments_universe_v2
      order by asset_class, secid
    """).df()

    client = MoexISSClient()

    # чтобы не тащить “сегодня”, берём до вчера
    till = date.today() - timedelta(days=1)

    total_saved = 0
    updated_instruments = 0
    skipped_up_to_date = 0

    for i, r in uni.iterrows():
        uid = r["instrument_uid"]

        last_dt = con.execute(
            "select max(dt) from bars_1d where instrument_uid = ?",
            [uid],
        ).fetchone()[0]

        if last_dt is None:
            # если по инструменту нет истории — этот кейс закрывает backfill job
            # тут просто пропустим, чтобы update_daily был быстрым
            continue

        start = last_dt + timedelta(days=1)
        if start > till:
            skipped_up_to_date += 1
            continue

        print(f"[{i+1}/{len(uni)}] {r['asset_class']} {r['secid']} ({r['boardid']}) {start} → {till}")

        df = fetch_daily_bars_for_instrument(
            client=client,
            engine=r["engine"],
            market=r["market"],
            boardid=r["boardid"],
            secid=r["secid"],
            date_from=start,
            date_to=till,
        )

        if df is None or df.empty:
            _sleep_between_requests()
            continue

        # обязательно проставляем мета-поля
        df["instrument_uid"] = uid
        df["asset_class"] = r["asset_class"]
        df["secid"] = r["secid"]
        df["boardid"] = r["boardid"]

        saved = upsert_bars_1d(con, df)
        total_saved += saved
        updated_instruments += 1

        print(f"   saved: {saved} rows (total {total_saved})")
        _sleep_between_requests()

    print("✅ update_daily done")
    print("instruments updated:", updated_instruments)
    print("skipped (already up-to-date):", skipped_up_to_date)
    print("total rows saved:", total_saved)
    print("bars_1d max(dt):", con.execute("select max(dt) from bars_1d").fetchone()[0])


if __name__ == "__main__":
    main()