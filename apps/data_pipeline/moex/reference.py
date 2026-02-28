from __future__ import annotations

import json
from typing import Dict, List

import pandas as pd

from apps.data_pipeline.moex.client import MoexISSClient
from apps.data_pipeline.moex.mapping import ASSET_CLASS_QUERIES


def _safe_get(row: Dict, key: str):
    return row.get(key)


def fetch_instruments_for_asset_class(client: MoexISSClient, asset_class: str) -> pd.DataFrame:
    queries = ASSET_CLASS_QUERIES.get(asset_class, [])
    all_rows: List[dict] = []

    for q in queries:
        engine = q["engine"]
        market = q["market"]

        boardids = set(q.get("boardids", []) or [])

        path = f"engines/{engine}/markets/{market}/securities.json"
        print(f"[seed_reference] asset_class={asset_class} engine={engine} market={market} ...")

        # tables:
        # - securities (secid, shortname, name, isin, currencyid, ...)
        # - marketdata / marketdata_yields — нам пока не нужно
        page_no = 0
        for columns, rows in client.paged_table(path=path, table_name="securities", page_size=100):
            page_no += 1
            if page_no % 5 == 0:
                print(f"[seed_reference] {asset_class} {engine}/{market}: page {page_no}, +{len(rows)} rows")
            for r in rows:
                d = dict(zip(columns, r))
                secid = d.get("SECID")
                boardid = d.get("BOARDID")  # бывает, бывает null
                # если для класса задан фильтр boardids — пропускаем всё лишнее
                if boardids and boardid not in boardids:
                    continue

                if not secid:
                    continue

                instrument_uid = f"{asset_class}:{engine}:{market}:{boardid or 'NA'}:{secid}"

                meta = {
                    "moex_engine": engine,
                    "moex_market": market,
                    "raw": d,
                }

                all_rows.append(
                    {
                        "instrument_uid": instrument_uid,
                        "asset_class": asset_class,
                        "engine": engine,
                        "market": market,
                        "boardid": boardid,
                        "secid": secid,
                        "shortname": d.get("SHORTNAME"),
                        "name": d.get("NAME"),
                        "isin": d.get("ISIN"),
                        "currencyid": d.get("CURRENCYID"),
                        "lot": d.get("LOT"),
                        "type": d.get("TYPE"),
                        "group_name": d.get("GROUP"),
                        "is_traded": (d.get("IS_TRADED") == 1) if d.get("IS_TRADED") is not None else None,
                        "meta_json": json.dumps(meta, ensure_ascii=False),
                    }
                )

    df = pd.DataFrame(all_rows)
    if df.empty:
        # чтобы не падать дальше
        return pd.DataFrame(
            columns=[
                "instrument_uid",
                "asset_class",
                "engine",
                "market",
                "boardid",
                "secid",
                "shortname",
                "name",
                "isin",
                "currencyid",
                "lot",
                "type",
                "group_name",
                "is_traded",
                "meta_json",
            ]
        )
    return df


def fetch_all_instruments(client: MoexISSClient, asset_classes: List[str]) -> pd.DataFrame:
    dfs = []
    for ac in asset_classes:
        dfs.append(fetch_instruments_for_asset_class(client, ac))
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)



def derive_metals_from_fx(fx_df: pd.DataFrame) -> pd.DataFrame:
    if fx_df.empty:
        return fx_df

    df = fx_df.copy()
    # фильтр по названию/коду (можно расширять)
    mask = (
        df["secid"].fillna("").str.upper().str.contains("XAU|XAG|PLT|PAL|GLD|GOLD|SLV|SILV")
        | df["name"].fillna("").str.upper().str.contains("GOLD|SILVER|PLATIN|PALLAD")
        | df["shortname"].fillna("").str.upper().str.contains("GOLD|SILVER|PLATIN|PALLAD")
    )
    metals = df[mask].copy()
    if metals.empty:
        return metals

    metals["asset_class"] = "metal"
    # instrument_uid должен стать уникальным для metal
    metals["instrument_uid"] = metals.apply(
        lambda r: f"metal:{r['engine']}:{r['market']}:{r['boardid'] or 'NA'}:{r['secid']}",
        axis=1,
    )
    return metals