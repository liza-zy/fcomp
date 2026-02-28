from __future__ import annotations

import argparse

from apps.data_pipeline.duckdb_store import connect, seed_asset_classes, upsert_instruments
from apps.data_pipeline.moex.client import MoexISSClient
from apps.data_pipeline.moex.reference import fetch_all_instruments


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--asset-classes",
        default="equity,fund,fx,metal",
        help="Comma-separated: equity,fund,fx,metal",
    )
    return p.parse_args()


def main():
    args = parse_args()
    asset_classes = [x.strip() for x in args.asset_classes.split(",") if x.strip()]

    con = connect()
    seed_asset_classes(con)

    client = MoexISSClient()
    df = fetch_all_instruments(client, asset_classes)

    # если грузили fx — выделим из него металлы автоматически
    if "fx" in asset_classes:
        from apps.data_pipeline.moex.reference import derive_metals_from_fx

    fx_df = df[df["asset_class"] == "fx"].copy()
    metals_df = derive_metals_from_fx(fx_df)

    if not metals_df.empty:
        df = df[df["asset_class"] != "metal"]  # на всякий
        df = df.reset_index(drop=True)
        df = df._append(metals_df, ignore_index=True)

    upsert_instruments(con, df)

    print("✅ Seeded reference")
    print(con.sql("select asset_class, count(*) as n from ref_instruments group by 1 order by 1").df())


if __name__ == "__main__":
    main()