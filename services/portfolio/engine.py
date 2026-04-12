from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
from fastapi import HTTPException
from scipy.optimize import minimize
import riskfolio as rp

from .constraints import apply_constraints_universe, normalize_max_weight
from .data_duckdb import DuckDBMarketData
from .risk_profiles import load_risk_profiles
from .schemas import BuildPortfolioRequest, BuildPortfolioResponse, PortfolioResult
from .visualize import weights_pie_b64


class PortfolioEngine:
    def __init__(
        self,
        duckdb_path: str = "data_lake/moex.duckdb",
        risk_yaml_path: str = "services/risk_quiz/domain/questions.yaml",
    ):
        self.market = DuckDBMarketData(duckdb_path)
        self.risk_profiles = load_risk_profiles(risk_yaml_path)

    def _estimate_mu(self, rets: pd.DataFrame) -> pd.Series:
        return rets.mean()

    def _select_sharpe_candidates(
        self,
        mu: pd.Series,
        cov: pd.DataFrame,
        n_assets: int,
    ) -> list[str]:
        diag = np.diag(cov.values).copy()
        diag = np.where(diag <= 1e-12, 1e-12, diag)
        sigma = np.sqrt(diag)

        score = pd.Series(mu.values / sigma, index=mu.index)
        k = min(len(score), max(n_assets * 3, 20))
        return score.sort_values(ascending=False).head(k).index.tolist()

    def _max_sharpe_constrained(
        self,
        mu: pd.Series,
        cov: pd.DataFrame,
        max_weight: float,
        n_assets: int,
    ) -> pd.Series:
        candidate_ids = self._select_sharpe_candidates(mu, cov, n_assets=n_assets)
        mu_sub = mu.loc[candidate_ids].astype(float)
        cov_sub = cov.loc[candidate_ids, candidate_ids].astype(float)

        n = len(mu_sub)
        if n == 0:
            return pd.Series(dtype=float)

        upper_bound = float(max_weight) if max_weight and max_weight > 0 else 1.0
        upper_bound = min(max(upper_bound, 1.0 / n), 1.0)

        x0 = np.full(n, 1.0 / n, dtype=float)

        mu_vec = mu_sub.values.astype(float)
        cov_mat = cov_sub.values.astype(float)

        def objective(w: np.ndarray) -> float:
            port_ret = float(mu_vec @ w)
            port_var = float(w @ cov_mat @ w)
            port_vol = np.sqrt(max(port_var, 1e-12))
            sharpe = port_ret / port_vol
            return -sharpe

        constraints = [
            {"type": "eq", "fun": lambda w: np.sum(w) - 1.0},
        ]
        bounds = [(0.0, upper_bound) for _ in range(n)]

        result = minimize(
            objective,
            x0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"maxiter": 500, "ftol": 1e-9, "disp": False},
        )

        if not result.success or result.x is None:
            w = x0.copy()
        else:
            w = np.maximum(result.x, 0.0)
            s = w.sum()
            if s <= 0:
                w = x0.copy()
            else:
                w = w / s

        w_series = pd.Series(w, index=mu_sub.index, dtype=float)

        # Оставляем top-n и потом решаем задачу еще раз уже на суженном множестве
        top_ids = (
            w_series.sort_values(ascending=False)
            .head(min(n_assets, len(w_series)))
            .index.tolist()
        )
        mu_top = mu.loc[top_ids].astype(float)
        cov_top = cov.loc[top_ids, top_ids].astype(float)

        n2 = len(mu_top)
        if n2 == 0:
            return pd.Series(0.0, index=mu.index, dtype=float)

        upper_bound2 = min(max(float(max_weight), 1.0 / n2), 1.0)
        x02 = np.full(n2, 1.0 / n2, dtype=float)

        mu_vec2 = mu_top.values.astype(float)
        cov_mat2 = cov_top.values.astype(float)

        def objective2(w: np.ndarray) -> float:
            port_ret = float(mu_vec2 @ w)
            port_var = float(w @ cov_mat2 @ w)
            port_vol = np.sqrt(max(port_var, 1e-12))
            sharpe = port_ret / port_vol
            return -sharpe

        result2 = minimize(
            objective2,
            x02,
            method="SLSQP",
            bounds=[(0.0, upper_bound2) for _ in range(n2)],
            constraints=[{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}],
            options={"maxiter": 500, "ftol": 1e-9, "disp": False},
        )

        if not result2.success or result2.x is None:
            w2 = x02.copy()
        else:
            w2 = np.maximum(result2.x, 0.0)
            s2 = w2.sum()
            if s2 <= 0:
                w2 = x02.copy()
            else:
                w2 = w2 / s2

        out = pd.Series(0.0, index=mu.index, dtype=float)
        out.loc[mu_top.index] = w2
        return out

    def _filter_small_weights(
        self,
        w: pd.Series,
        min_weight: float,
    ) -> pd.Series:
        w = pd.Series(w, dtype=float).fillna(0.0)

        if min_weight is None or min_weight <= 0:
            s = float(w.sum())
            if s <= 0:
                return pd.Series(dtype=float)
            return pd.Series(w / s, index=w.index, dtype=float)

        w = w[w >= float(min_weight)]

        if w.empty:
            return pd.Series(dtype=float)

        s = float(w.sum())
        if s <= 0:
            return pd.Series(dtype=float)

        return pd.Series(w / s, index=w.index, dtype=float)


    def _max_sharpe_riskfolio(
        self,
        rets: pd.DataFrame,
        max_weight: float,
    ) -> pd.Series:
        if rets.empty or rets.shape[1] < 2:
            return pd.Series(dtype=float)

        n = rets.shape[1]
        max_weight = float(max_weight) if max_weight and max_weight > 0 else 1.0

        # Проверка достижимости только по max_weight
        if n * max_weight < 1.0:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Infeasible constraints: {n} assets are not enough "
                    f"to satisfy max_weight={max_weight:.4f}"
                ),
            )

        port = rp.Portfolio(returns=rets)

        port.assets_stats(
            method_mu="hist",
            method_cov="hist",
        )

        port.sht = False
        port.upperlng = max_weight

        w = port.optimization(
            model="Classic",
            rm="MV",
            obj="Sharpe",
            rf=0,
            l=0,
            hist=True,
        )

        if w is None or w.empty:
            fallback = np.full(n, 1.0 / n, dtype=float)
            return pd.Series(fallback, index=rets.columns, dtype=float)

        if isinstance(w, pd.DataFrame):
            if "weights" in w.columns:
                w = w["weights"]
            else:
                w = w.iloc[:, 0]

        w = pd.Series(w, index=rets.columns, dtype=float).fillna(0.0)
        w = np.maximum(w, 0.0)

        s = float(w.sum())
        if s <= 0:
            fallback = np.full(n, 1.0 / n, dtype=float)
            return pd.Series(fallback, index=rets.columns, dtype=float)

        return pd.Series(w / s, index=rets.columns, dtype=float)

    def _max_return_with_vol_cap(
        self,
        mu: pd.Series,
        cov: pd.DataFrame,
        target_vol: float,
    ) -> pd.Series:
        ranked = mu.sort_values(ascending=False)
        last_w = None

        for n in [20, 30, 40, 60, 80, 120]:
            pick = ranked.head(min(n, len(ranked)))
            w = pd.Series(0.0, index=mu.index, dtype=float)
            w.loc[pick.index] = 1.0 / len(pick)
            vol = float(np.sqrt(w.values @ cov.values @ w.values))
            last_w = w
            if vol <= target_vol or n == 120:
                return w

        return last_w if last_w is not None else pd.Series(0.0, index=mu.index, dtype=float)

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
            raise HTTPException(
                status_code=400,
                detail=f"Unknown risk profile: {risk_profile_key}",
            )

        universe = self.market.load_universe_for_risk_profile(
            as_of,
            rp.index,
            req.constraints.is_qualified_investor,
        )
        universe = apply_constraints_universe(universe, req.constraints)

        if universe.empty:
            return BuildPortfolioResponse(portfolios=[])

        uids = universe["instrument_uid"].tolist()

        wide = self.market.load_returns_wide(uids, as_of, req.lookback)

        min_periods = int(req.lookback * 0.6)
        nn = wide.notna().sum(axis=0)
        wide = wide.loc[:, nn >= min_periods]
        wide = wide.fillna(0.0)

        if wide.shape[1] < 10:
            return BuildPortfolioResponse(portfolios=[])

        cov = self.market.load_cov_matrix(
            list(wide.columns),
            as_of,
            req.cov_method,
            req.lookback,
        )

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

        cov = cov.copy()
        eps = 1e-6
        for i in range(len(cov)):
            cov.iat[i, i] += eps

        mu = self._estimate_mu(wide)

        vmax = getattr(rp, "vol_max", None)

        if vmax is None:
            target_vol = 0.20
        else:
            target_vol = float(vmax) / 100.0

        n_assets = getattr(req.constraints, "max_assets", 10) or 10
        max_weight = float(getattr(req.constraints, "max_weight", 0.15) or 0.15)

        # 1) riskfolio max_sharpe
        max_weight = float(getattr(req.constraints, "max_weight", 0.15) or 0.15)
        min_weight = float(getattr(req.constraints, "min_weight", 0.01) or 0.0)

        w_sharpe = self._max_sharpe_riskfolio(
            rets=wide,
            max_weight=max_weight,
        )

        # только защита от численного мусора, без top_n cut
        w_sharpe = normalize_max_weight(w_sharpe, max_weight)
        w_sharpe = self._filter_small_weights(w_sharpe, min_weight)
        w_sharpe = normalize_max_weight(w_sharpe, max_weight)
        w_sharpe = pd.Series(w_sharpe, dtype=float)

        # 2) текущая эвристика max_return
        w_ret = self._max_return_with_vol_cap(mu, cov, target_vol=target_vol)
        w_ret = normalize_max_weight(w_ret, max_weight)
        w_ret = self.top_n_renormalize(w_ret.to_dict(), n_assets)
        w_ret = pd.Series(w_ret, dtype=float)

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

        uid2secid = dict(zip(universe["instrument_uid"], universe["secid"]))

        def to_api_weights(w: pd.Series) -> dict[str, float]:
            out: dict[str, float] = {}
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
            chart_png_b64=weights_pie_b64(
                weights_sharpe,
                f"Max Sharpe ({risk_profile_key})",
            ),
        )

        p2 = PortfolioResult(
            method="max_return",
            as_of=as_of.isoformat(),
            risk_profile=risk_profile_key,
            weights=weights_ret,
            metrics=metrics(w_ret),
            chart_png_b64=weights_pie_b64(
                weights_ret,
                f"Max Return ({risk_profile_key})",
            ),
        )

        return BuildPortfolioResponse(portfolios=[p1, p2])
