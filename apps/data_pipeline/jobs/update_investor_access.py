from __future__ import annotations

import duckdb

from apps.data_pipeline.moex.client import MoexISSClient

DB_PATH = "data_lake/moex.duckdb"


def _normalize_access(value: str | None) -> str:
    if value is None or value == "":
        return "unknown"
    if str(value) == "1":
        return "qualified_only"
    if str(value) == "0":
        return "non_qualified"
    return "unknown"


def _extract_is_qualified(description_block: dict) -> str:
    columns = description_block.get("columns", [])
    data = description_block.get("data", [])

    if not columns or not data:
        return "unknown"

    try:
        name_idx = columns.index("name")
        value_idx = columns.index("value")
    except ValueError:
        return "unknown"

    for row in data:
        if len(row) <= max(name_idx, value_idx):
            continue
        if row[name_idx] == "ISQUALIFIEDINVESTORS":
            return _normalize_access(row[value_idx])

    return "unknown"


def main() -> None:
    con = duckdb.connect(DB_PATH)
    client = MoexISSClient()

    rows = con.execute("""
        select instrument_uid, secid, asset_class
        from ref_instruments
        where secid is not null
        order by asset_class, secid
    """).fetchall()

    if not rows:
        print("No instruments found in ref_instruments")
        return

    updates = []

    for i, (instrument_uid, secid, asset_class) in enumerate(rows, start=1):
        try:
            data = client.get_json(f"/securities/{secid}.json", params={"iss.meta": "off"})
            access = _extract_is_qualified(data.get("description", {}))
        except Exception as e:
            access = "unknown"

        updates.append((instrument_uid, access))

        if i % 100 == 0:
            print(f"[{i}/{len(rows)}] processed")

    con.execute("""
        alter table ref_instruments
        add column if not exists investor_access varchar
    """)

    con.executemany("""
        update ref_instruments
        set investor_access = ?
        where instrument_uid = ?
    """, [(access, instrument_uid) for instrument_uid, access in updates])

    print("✅ update_investor_access done")
    print(
        con.execute("""
            select investor_access, count(*) as cnt
            from ref_instruments
            group by 1
            order by 1
        """).fetchdf().to_string(index=False)
    )


if __name__ == "__main__":
    main()
