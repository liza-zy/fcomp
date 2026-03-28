from __future__ import annotations

import math
from pathlib import Path

import duckdb
import yaml


TRADING_DAYS = 252
MIN_OBS = 60


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_profile_bands(yaml_path: Path) -> list[dict]:
    with yaml_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    profiles = data.get("profiles", {})
    bands: list[dict] = []

    for name, cfg in profiles.items():
        bands.append(
            {
                "risk_profile": name,
                "risk_score": int(cfg["index"]),
                "vol_min": float(cfg["volatility_min"]),
                "vol_max": float(cfg["volatility_max"]),
            }
        )

    bands.sort(key=lambda x: x["risk_score"])
    return bands


def classify_volatility(vol_pct: float | None, bands: list[dict]) -> tuple[str | None, int | None]:
    if vol_pct is None or (isinstance(vol_pct, float) and math.isnan(vol_pct)):
        return None, None

    for band in bands:
        if band["vol_min"] <= vol_pct <= band["vol_max"]:
            return band["risk_profile"], band["risk_score"]

    if vol_pct > bands[-1]["vol_max"]:
        return bands[-1]["risk_profile"], bands[-1]["risk_score"]

    for band in bands:
        if vol_pct < band["vol_min"]:
            return band["risk_profile"], band["risk_score"]

    return None, None


def main() -> None:
    root = repo_root()
    duckdb_path = root / "data_lake" / "moex.duckdb"
    yaml_path = root / "services" / "risk_quiz" / "domain" / "questions.yaml"

    if not duckdb_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {duckdb_path}")

    if not yaml_path.exists():
        raise FileNotFoundError(f"YAML file not found: {yaml_path}")

    bands = load_profile_bands(yaml_path)
    con = duckdb.connect(str(duckdb_path))

    con.execute("""
        CREATE TABLE IF NOT EXISTS asset_risk_profile (
            instrument_uid VARCHAR PRIMARY KEY,
            secid VARCHAR,
            asset_class VARCHAR,
            risk_profile VARCHAR,
            risk_score INTEGER,
            ann_vol_pct DOUBLE
        );
    """)

    vol_df = con.execute(f"""
        WITH ranked AS (
            SELECT
                instrument_uid,
                dt,
                logret_1d,
                ROW_NUMBER() OVER (
                    PARTITION BY instrument_uid
                    ORDER BY dt DESC
                ) AS rn
            FROM returns_1d
            WHERE logret_1d IS NOT NULL
        ),
        sample_252 AS (
            SELECT
                instrument_uid,
                dt,
                logret_1d
            FROM ranked
            WHERE rn <= {TRADING_DAYS}
        ),
        stats AS (
            SELECT
                instrument_uid,
                COUNT(*) AS n_obs,
                STDDEV_SAMP(logret_1d) * SQRT({TRADING_DAYS}) * 100.0 AS ann_vol_pct
            FROM sample_252
            GROUP BY instrument_uid
            HAVING COUNT(*) >= {MIN_OBS}
        )
        SELECT
            s.instrument_uid,
            ri.secid,
            ri.asset_class,
            s.ann_vol_pct
        FROM stats s
        LEFT JOIN ref_instruments ri
            ON ri.instrument_uid = s.instrument_uid
    """).fetchdf()

    if vol_df.empty:
        print("No data found to update asset_risk_profile")
        return

    vol_df["risk_profile"] = None
    vol_df["risk_score"] = None

    for idx, row in vol_df.iterrows():
        profile, score = classify_volatility(row["ann_vol_pct"], bands)
        vol_df.at[idx, "risk_profile"] = profile
        vol_df.at[idx, "risk_score"] = score

    result_df = vol_df[
        ["instrument_uid", "secid", "asset_class", "risk_profile", "risk_score", "ann_vol_pct"]
    ].copy()
    result_df = result_df[result_df["risk_profile"].notna()].copy()

    con.register("asset_risk_profile_stage", result_df)

    con.execute("""
        CREATE OR REPLACE TABLE asset_risk_profile AS
        SELECT
            instrument_uid,
            secid,
            asset_class,
            risk_profile,
            risk_score,
            ann_vol_pct
        FROM asset_risk_profile_stage
    """)

    print("asset_risk_profile updated")
    print(
        con.execute("""
            SELECT
                risk_profile,
                COUNT(*) AS cnt,
                ROUND(MIN(ann_vol_pct), 2) AS min_vol,
                ROUND(MAX(ann_vol_pct), 2) AS max_vol
            FROM asset_risk_profile
            GROUP BY 1
            ORDER BY MIN(risk_score)
        """).fetchdf().to_string(index=False)
    )


if __name__ == "__main__":
    main()
