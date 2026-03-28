from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import date
from fastapi import HTTPException

from .schemas import BuildPortfolioRequest, BuildPortfolioResponse, PortfolioResult
from .data_duckdb import DuckDBMarketData
from .constraints import apply_constraints_universe, normalize_max_weight
from .risk_profiles import load_risk_profiles
from .visualize import weights_pie_b64

# Если используешь riskfolio:
# import riskfolio as rp

class PortfolioEngine:
    def __init__(self, duckdb_path: str = "data_lake/moex.duckdb", risk_yaml_path: str = "services/risk_quiz/domain/questions.yaml"):
        self.market = DuckDBMarketData(duckdb_path)
        self.risk_profiles = load_risk_profiles(risk_yaml_path)

    def _estimate_mu(self, rets: pd.DataFrame) -> pd.Series:
        # простая оценка: средняя дневная доходность
        return rets.mean()

    def _max_sharpe_closed_form(self, mu: pd.Series, cov: pd.DataFrame) -> pd.Series:
        # w ~ inv(C) * mu, затем нормировка; (без rf)
        Cinv = np.linalg.pinv(cov.values)
        w = Cinv @ mu.values
        w = np.maximum(w, 0)  # long-only
        if w.sum() == 0:
            w = np.ones_like(w)
        w = w / w.sum()
        return pd.Series(w, index=cov.index)

    def _max_return_with_vol_cap(self, mu: pd.Series, cov: pd.DataFrame, target_vol: float) -> pd.Series:
        # очень простой эвристический вариант:
        # берем top-N по mu и нормируем, если вола > cap -> уменьшаем концентрацию
        ranked = mu.sort_values(ascending=False)
        for n in [20, 30, 40, 60, 80, 120]:
            pick = ranked.head(min(n, len(ranked)))
            w = pd.Series(0.0, index=mu.index)
            w.loc[pick.index] = 1.0 / len(pick)
            vol = float(np.sqrt(w.values @ cov.values @ w.values))
            if vol <= target_vol or n == 120:
                return w
        return w

    def top_n_renormalize(self, w: dict[str, float], n: int) -> dict[str, float]:
        items = sorted(w.items(), key=lambda x: x[1], reverse=True)[: int(n)]
        s = sum(v for _, v in items)
        if s <= 0:
            return {k: 0.0 for k, _ in items}
        return {k: float(v / s) for k, v in items}

    def build(
        self,
        req: BuildPortfolioRequest,
        risk_profile_key: str,
        as_of: date,
    ) -> BuildPortfolioResponse:
        rp = self.risk_profiles.get(risk_profile_key)
        if rp is None:
            raise HTTPException(status_code=400, detail=f"Unknown risk profile: {risk_profile_key}")

        # 1) universe зависит от риск-профиля
        universe = self.market.load_universe_for_risk_profile(as_of, rp.index)
        universe = apply_constraints_universe(universe, req.constraints)

        if universe.empty:
            return BuildPortfolioResponse(portfolios=[])

        uids = universe["instrument_uid"].tolist()

        # 2) returns
        wide = self.market.load_returns_wide(uids, as_of, req.lookback)

        min_periods = int(req.lookback * 0.6)
        nn = wide.notna().sum(axis=0)
        wide = wide.loc[:, nn >= min_periods]
        wide = wide.fillna(0.0)

        if wide.shape[1] < 10:
            return BuildPortfolioResponse(portfolios=[])

        # 3) covariance
        cov = self.market.load_cov_matrix(list(wide.columns), as_of, req.cov_method, req.lookback)

        if not cov.empty:
            cols = [c for c in wide.columns if c in cov.index]
            wide = wide[cols]
            cov = cov.loc[cols, cols]

        if cov.empty or wide.shape[1] < 10:
            cov = wide.cov(min_periods=min_periods)
            cols = list(cov.index.intersection(wide.columns))
            cov = cov.loc[cols, cols]
            wide = wide[cols]

        if wide.shape[1] < 10 or cov.empty:
            return BuildPortfolioResponse(portfolios=[])

        mu = self._estimate_mu(wide)

        # 4) target_vol из risk_profile
        vmin = getattr(rp, "vol_min", None)
        vmax = getattr(rp, "vol_max", None)

        if vmin is None and vmax is None:
            target_vol = 0.20
        elif vmax is not None:
            target_vol = float(vmax) / 100.0
        else:
            target_vol = float(vmin) / 100.0

        n_assets = getattr(req.constraints, "max_assets", 10) or 10

        # 5) метод 1: max_sharpe
        w_sharpe = self._max_sharpe_closed_form(mu, cov)
        w_sharpe = normalize_max_weight(w_sharpe, req.constraints.max_weight)
        w_sharpe = self.top_n_renormalize(w_sharpe.to_dict(), n_assets)
        w_sharpe = pd.Series(w_sharpe, dtype=float)

        # 6) метод 2: max_return с ограничением волатильности
        w_ret = self._max_return_with_vol_cap(mu, cov, target_vol=target_vol)
        w_ret = normalize_max_weight(w_ret, req.constraints.max_weight)
        w_ret = self.top_n_renormalize(w_ret.to_dict(), n_assets)
        w_ret = pd.Series(w_ret, dtype=float)

        # 7) metrics
        def metrics(w: pd.Series) -> dict[str, float]:
            w = pd.Series(w, dtype=float)
            w = w.reindex(mu.index).fillna(0.0)
            exp_ret = float(mu.values @ w.values)
            vol = float(np.sqrt(w.values @ cov.values @ w.values))
            sharpe = float(exp_ret / vol) if vol > 0 else 0.0
            return {
                "exp_return_1d": exp_ret,
                "vol_1d": vol,
                "sharpe_like": sharpe,
            }

        # 8) mapping uid -> secid
        uid2secid = dict(zip(universe["instrument_uid"], universe["secid"]))

        def to_api_weights(w: pd.Series) -> dict[str, float]:
            out = {}
            for uid, weight in w.items():
                if float(weight) <= 0:
                    continue
                secid = uid2secid.get(uid, uid)
                out[secid] = float(weight)
            return dict(sorted(out.items(), key=lambda kv: kv[1], reverse=True))

        weights_sharpe = to_api_weights(w_sharpe)
        weights_ret = to_api_weights(w_ret)

        p1 = PortfolioResult(
            method="max_sharpe",
            as_of=as_of.isoformat(),
            risk_profile=risk_profile_key,
            weights=weights_sharpe,
            metrics=metrics(w_sharpe),
            chart_png_b64=weights_pie_b64(weights_sharpe, f"Max Sharpe ({risk_profile_key})"),
        )

        p2 = PortfolioResult(
            method="max_return",
            as_of=as_of.isoformat(),
            risk_profile=risk_profile_key,
            weights=weights_ret,
            metrics=metrics(w_ret),
            chart_png_b64=weights_pie_b64(weights_ret, f"Max Return ({risk_profile_key})"),
        )

        return BuildPortfolioResponse(portfolios=[p1, p2])
