from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import yaml

RISK_YAML_PATH_DEFAULT = Path("services/risk_quiz/domain/questions.yaml")

@dataclass(frozen=True)
class RiskProfileParams:
    key: str
    index: int
    max_drawdown: float
    vol_min: float
    vol_max: float
    equity_min: float
    equity_max: float
    exp_ret_min: float
    exp_ret_max: float
    text: str

    @property
    def target_vol_annual(self) -> float:
        # берём середину диапазона (в % -> доля)
        return (self.vol_min + self.vol_max) / 2 / 100.0

    @property
    def equity_target_share(self) -> float:
        return (self.equity_min + self.equity_max) / 2 / 100.0


def load_risk_profiles(path: str | Path = RISK_YAML_PATH_DEFAULT) -> dict[str, RiskProfileParams]:
    path = Path(path)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))

    profiles_raw = data.get("profiles", {})
    profiles: dict[str, RiskProfileParams] = {}

    for key, p in profiles_raw.items():
        profiles[key] = RiskProfileParams(
            key=key,
            index=int(p["index"]),
            max_drawdown=float(p["max_drawdown"]),
            vol_min=float(p["volatility_min"]),
            vol_max=float(p["volatility_max"]),
            equity_min=float(p["equity_share_min"]),
            equity_max=float(p["equity_share_max"]),
            exp_ret_min=float(p["expected_return_min"]),
            exp_ret_max=float(p["expected_return_max"]),
            text=str(p.get("text", "")).strip(),
        )

    # сортировка “на всякий” по index (полезно для UI)
    profiles = dict(sorted(profiles.items(), key=lambda kv: kv[1].index))
    return profiles