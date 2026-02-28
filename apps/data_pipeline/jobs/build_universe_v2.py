from __future__ import annotations

import argparse
import duckdb

DB_PATH = "data_lake/moex.duckdb"
METAL_PREFIXES = ("GLD", "SLV", "PLD", "PLT")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--min-fx-bars", type=int, default=250, help="минимум дневных баров для FX (по умолчанию ~1 год)")
    p.add_argument("--replace", action="store_true", help="перезаписать ref_instruments_universe")
    args = p.parse_args()

    con = duckdb.connect(DB_PATH)

    # базовый universe (уже отфильтрованный equity/fund/fx_rub_tom + metal)
    base = con.sql("select * from ref_instruments_universe").df()

    # equity/fund оставляем как есть
    equity = base[base["asset_class"] == "equity"].copy()
    fund = base[base["asset_class"] == "fund"].copy()

    # текущие свечи -> сколько баров на каждый (secid, boardid)
    fx_counts = con.sql("""
    select secid, boardid, count(*) as n
    from bars_1d
    where asset_class in ('fx','metal')
    group by 1,2
    """).df()

    # ✅ кандидаты FX+metal: берём оба класса из base, но только CETS
    fxm = base[base["asset_class"].isin(["fx", "metal"]) & (base["boardid"] == "CETS")].copy()

    # (опционально, но полезно) оставим только *_RUB_TOM
    secid_u_all = fxm["secid"].fillna("").str.upper()
    fxm = fxm[secid_u_all.str.endswith("RUB_TOM")].copy()

    # подтягиваем покрытие по барам (уже правильно: fx+metal)
    fxm = fxm.merge(fx_counts, on=["secid", "boardid"], how="left")
    fxm["n"] = fxm["n"].fillna(0).astype(int)

    # фильтр по истории
    fxm = fxm[fxm["n"] >= args.min_fx_bars].copy()

    # ✅ выделяем металлы по префиксам SECID
    secid_u = fxm["secid"].fillna("").str.upper()
    is_metal = secid_u.str.startswith(METAL_PREFIXES)

    metal = fxm[is_metal].copy()
    metal["asset_class"] = "metal"
    metal = metal.drop(columns=["n"])

    fx = fxm[~is_metal].copy()
    fx["asset_class"] = "fx"
    fx = fx.drop(columns=["n"])

    universe_v2 = duckdb.query_df(
        equity, "equity",
        "select * from equity"
    ).df()
    # concat через pandas не нужен, но проще:
    import pandas as pd
    universe_v2 = pd.concat([equity, fund, fx, metal], ignore_index=True)
    universe_v2 = universe_v2.drop_duplicates(subset=["instrument_uid"], keep="last")

    target = "ref_instruments_universe_v2"
    if args.replace:
        target = "ref_instruments_universe"

    con.execute(f"drop table if exists {target};")
    con.register("tmp_uni", universe_v2)
    con.execute(f"create table {target} as select * from tmp_uni;")
    con.unregister("tmp_uni")

    con.execute(f"create index if not exists idx_{target}_asset on {target}(asset_class);")
    con.execute(f"create index if not exists idx_{target}_secid on {target}(secid);")
    con.execute(f"create index if not exists idx_{target}_uid on {target}(instrument_uid);")

    print(f"✅ Built {target}")
    print(con.sql(f"select asset_class, count(*) n from {target} group by 1 order by 1").df())

    print("\nFX kept (CETS) sample:")
    print(con.sql(f"""
        select secid, boardid
        from {target}
        where asset_class='fx'
        order by secid
        limit 50
    """).df())


if __name__ == "__main__":
    main()