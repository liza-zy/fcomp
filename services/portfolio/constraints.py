from __future__ import annotations
import pandas as pd
from .schemas import PortfolioConstraints

def apply_constraints_universe(universe: pd.DataFrame, c: PortfolioConstraints) -> pd.DataFrame:
    df = universe.copy()

    if c.exclude_secids:
        df = df[~df["secid"].isin(c.exclude_secids)]

    if c.currencies_include:
        # ожидаем колонку currencyid (например "RUB", "CNY")
        df = df[df["currencyid"].isin(c.currencies_include)]

    if c.sectors_include:
        # ожидаем колонку sector_name (или sector_code)
        df = df[df["sector_name"].isin(c.sectors_include)]

    return df.reset_index(drop=True)

def normalize_max_weight(weights: pd.Series, max_weight: float) -> pd.Series:
    # простой клиппинг, потом нормировка
    w = weights.clip(upper=max_weight)
    s = w.sum()
    return (w / s) if s > 0 else w