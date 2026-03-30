from __future__ import annotations

from apps.data_pipeline.rl_dataset.schema import (
    RLDatabaseConfig,
    create_rl_training_schema,
    drop_rl_layer_tables,
)


def init_rl_layer(
    cfg: RLDatabaseConfig = RLDatabaseConfig(),
    drop_existing_rl_layer: bool = True,
) -> None:
    if drop_existing_rl_layer:
        drop_rl_layer_tables(cfg)
    create_rl_training_schema(cfg)


if __name__ == "__main__":
    init_rl_layer()
