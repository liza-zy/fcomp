from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection
from services.portfolio.risk_profiles import load_risk_profiles


DEFAULT_MAX_SINGLE_BY_RANK = {
    0: 0.10,
    1: 0.12,
    2: 0.15,
    3: 0.18,
    4: 0.20,
}


def _rank_to_max_single(rank: int, n: int) -> float:
    if n <= 1:
        return 0.15
    scaled = round(rank * 4 / (n - 1))
    return DEFAULT_MAX_SINGLE_BY_RANK.get(scaled, 0.15)


def seed_risk_profiles(
    cfg: RLDatabaseConfig = RLDatabaseConfig(),
    yaml_path: str | Path = "services/risk_quiz/domain/questions.yaml",
) -> None:
    profiles = load_risk_profiles(yaml_path)
    ordered = list(profiles.values())

    con = get_connection(cfg.target_db)
    try:
        con.execute("DELETE FROM risk_profiles")

        payload = []
        for rank, p in enumerate(ordered):
            payload.append(
                (
                    int(p.index),
                    p.key,
                    float(p.vol_min) / 100.0,
                    float(p.vol_max) / 100.0,
                    float(p.max_drawdown) / 100.0,
                    float(p.equity_max) / 100.0,
                    None,  # max_bond_weight
                    None,  # max_fx_weight
                    None,  # max_metal_weight
                    _rank_to_max_single(rank, len(ordered)),
                    None,  # risk_penalty_lambda
                    None,  # turnover_penalty_lambda
                )
            )

        con.executemany(
            """
            INSERT INTO risk_profiles (
                risk_profile_id,
                profile_name,
                target_vol_min,
                target_vol_max,
                max_drawdown_target,
                max_equity_weight,
                max_bond_weight,
                max_fx_weight,
                max_metal_weight,
                max_single_asset_weight,
                risk_penalty_lambda,
                turnover_penalty_lambda
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )

        print("Seeded risk_profiles:", len(payload))
        for row in con.execute(
            """
            SELECT *
            FROM risk_profiles
            ORDER BY risk_profile_id
            """
        ).fetchall():
            print(row)
    finally:
        con.close()


if __name__ == "__main__":
    seed_risk_profiles()
