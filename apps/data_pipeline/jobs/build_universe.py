from __future__ import annotations

import duckdb
import pandas as pd

DB_PATH = "data_lake/moex.duckdb"


def main():
    con = duckdb.connect(DB_PATH)

    # берём полный справочник из DuckDB
    full = con.sql("select * from ref_instruments").df()

    # 1) equity: только TQBR
    equity = full[(full["asset_class"] == "equity") & (full["boardid"] == "TQBR")].copy()

    # 2) fund: только TQTF
    fund = full[(full["asset_class"] == "fund") & (full["boardid"] == "TQTF")].copy()

    # 3) fx: все *_RUB_TOM
    fx = full[(full["asset_class"] == "fx")].copy()
    fx_secid_u = fx["secid"].fillna("").str.upper()

    fx_rub_tom = fx[fx_secid_u.str.endswith("RUB_TOM")].copy()

    # 4) metal: только GLD/PLT RUB_TOM на CETS
    metal_mask = fx_rub_tom["secid"].fillna("").str.upper().isin(["GLDRUB_TOM", "PLTRUB_TOM"])
    metals = fx_rub_tom[metal_mask & (fx_rub_tom["boardid"] == "CETS")].copy()

    if not metals.empty:
        metals["asset_class"] = "metal"
        metals["instrument_uid"] = metals.apply(
            lambda r: f"metal:{r['engine']}:{r['market']}:{r['boardid'] or 'NA'}:{r['secid']}",
            axis=1,
        )

    # fx без металлов (убираем GLDRUB_TOM/PLTRUB_TOM во всех boardid)
    fx_universe = fx_rub_tom[~metal_mask].copy()

    universe = pd.concat([equity, fund, fx_universe, metals], ignore_index=True)
    universe = universe.drop_duplicates(subset=["instrument_uid"], keep="last")
    universe = universe.sort_values(["asset_class", "engine", "market", "boardid", "secid"], kind="mergesort")

    # --- грузим в DuckDB ---
    con.execute("drop table if exists ref_instruments_universe;")
    con.register("tmp_universe", universe)
    con.execute("create table ref_instruments_universe as select * from tmp_universe;")
    con.unregister("tmp_universe")

    # индексы (ускорят выборки на backfill)
    con.execute("create index if not exists idx_universe_asset on ref_instruments_universe(asset_class);")
    con.execute("create index if not exists idx_universe_secid on ref_instruments_universe(secid);")
    con.execute("create index if not exists idx_universe_uid on ref_instruments_universe(instrument_uid);")

    print("✅ Universe table created: ref_instruments_universe")
    print(con.sql("select asset_class, count(*) n from ref_instruments_universe group by 1 order by 1").df())

    print("\nMetals (universe):")
    print(con.sql("select secid, boardid, instrument_uid from ref_instruments_universe where asset_class='metal' order by secid, boardid").df())


if __name__ == "__main__":
    main()