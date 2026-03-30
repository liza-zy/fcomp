from __future__ import annotations

from pathlib import Path

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection


EXPORT_TABLES = [
    "rl_states",
    "rl_state_instrument_features",
    "rl_actions",
    "rl_target_portfolios",
    "rl_target_portfolio_weights",
    "rl_rewards",
    "rl_next_state_features",
    "rl_transitions",
]


def export_rl_to_parquet(
    cfg: RLDatabaseConfig = RLDatabaseConfig(),
    out_dir: str = "data_lake/rl_export",
) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    con = get_connection(cfg.target_db)
    try:
        for table in EXPORT_TABLES:
            file_path = out_path / f"{table}.parquet"
            con.execute(f"COPY {table} TO '{file_path.as_posix()}' (FORMAT PARQUET)")
            print(f"exported: {file_path}")
    finally:
        con.close()


if __name__ == "__main__":
    export_rl_to_parquet()
