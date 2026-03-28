"""
daily_run.py — единый ежедневный пайплайн.

Рекомендуемый порядок:
1) update_daily            — инкремент bars_1d
2) update_returns_daily    — инкремент returns_1d
3) compute_features_1d      — инкремент features_1d (rolling)
4) update_investor_access   — обновить доступность для квал/неквал
5) refresh_universes        — пересобрать universe_new/core по stats
6) refresh_core_views       — обновить VIEW для core
7) rebuild_cov_cache        — ковариации по returns_1d_core (core)
Запуск:
python -m apps.data_pipeline.jobs.daily_run
"""

from __future__ import annotations

import argparse
import duckdb

DB_PATH = "data_lake/moex.duckdb"

from apps.data_pipeline.jobs.update_daily import main as update_daily_main
from apps.data_pipeline.jobs.update_returns_daily import main as update_returns_main
from apps.data_pipeline.jobs.compute_features_1d import main as compute_features_main
from apps.data_pipeline.jobs.update_investor_access import main as update_investor_access_main
from apps.data_pipeline.jobs.refresh_universes import main as refresh_universes_main
from apps.data_pipeline.jobs.refresh_core_views import main as refresh_core_views_main
from apps.data_pipeline.jobs.rebuild_cov_cache import main as rebuild_cov_main


def _print_state(label: str) -> None:
    con = duckdb.connect(DB_PATH)

    bars_max = con.sql("select max(dt) from bars_1d").fetchone()[0]
    ret_max = con.sql("select max(dt) from returns_1d").fetchone()[0]
    feat_max = con.sql("select max(dt) from features_1d").fetchone()[0]

    # могут отсутствовать на самом первом прогоне
    tables = {r[0] for r in con.sql("show tables").fetchall()}
    views = {r[0] for r in con.sql("select table_name from information_schema.tables where table_type='VIEW'").fetchall()}

    cov_max = None
    if "cov_cache_1d" in tables:
        cov_max = con.sql("select max(as_of_date) from cov_cache_1d").fetchone()[0]

    core_n = None
    new_n = None
    if "universe_core" in tables:
        core_n = con.sql("select count(*) from universe_core").fetchone()[0]
    if "universe_new" in tables:
        new_n = con.sql("select count(*) from universe_new").fetchone()[0]

    print(f"\n[{label}]")
    print("  bars_1d max(dt):      ", bars_max)
    print("  returns_1d max(dt):   ", ret_max)
    print("  features_1d max(dt):  ", feat_max)
    print("  universe_core count:  ", core_n)
    print("  universe_new count:   ", new_n)
    print("  cov_cache max(date):  ", cov_max)

    # sanity: core views exist?
    core_views = ["returns_1d_core", "features_1d_core", "bars_1d_clean"]
    present = {v: (v in views) for v in core_views}
    print("  core views:", present)

def common_as_of(con):
    return con.execute("""
      select min(max_dt) from (
        select asset_class, max(dt) as max_dt
        from bars_1d_clean
        where asset_class in ('equity','fx','metal')
        group by 1
      )
    """).fetchone()[0]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--skip-bars", action="store_true")
    p.add_argument("--skip-returns", action="store_true")
    p.add_argument("--skip-features", action="store_true")
    p.add_argument("--skip-investor-access", action="store_true")
    p.add_argument("--skip-universe", action="store_true")
    p.add_argument("--skip-views", action="store_true")
    p.add_argument("--skip-cov", action="store_true")

    p.add_argument("--as-of", default=None, help="YYYY-MM-DD; if omitted uses max(dt) from bars_1d")
    p.add_argument("--min-core-bars", type=int, default=250)
    p.add_argument("--min-new-bars", type=int, default=60)
    p.add_argument("--new-recency-days", type=int, default=90)
    p.add_argument("--core-recency-days", type=int, default=30)

    p.add_argument("--lookback", type=int, default=252)
    p.add_argument("--ewma-span", type=int, default=60)

    args = p.parse_args()

    _print_state("before")

    if not args.skip_bars:
        print("\n▶ step 1/7: update_daily (bars_1d)")
        update_daily_main()

    if not args.skip_returns:
        print("\n▶ step 2/7: update_returns_daily (returns_1d)")
        update_returns_main()

    if not args.skip_features:
        print("\n▶ step 3/7: compute_features_1d (features_1d)")
        compute_features_main()

    if not args.skip_investor_access:
        print("\n▶ step 4/7: update_investor_access (qualified/non-qualified)")
        update_investor_access_main()

    if not args.skip_universe:
        print("\n▶ step 5/7: refresh_universes (universe_new/core)")
        refresh_universes_main(
            as_of_date=args.as_of,
            min_core_bars=args.min_core_bars,
            min_new_bars=args.min_new_bars,
            new_recency_days=args.new_recency_days,
            core_recency_days=args.core_recency_days,
        )

    if not args.skip_views:
        print("\n▶ step 6/7: refresh_core_views (views for core)")
        refresh_core_views_main()

    if not args.skip_cov:
        print("\n▶ step 7/7: rebuild_cov_cache (cov_cache_1d)")
        con = duckdb.connect(DB_PATH)
        as_of = common_as_of(con)
        rebuild_cov_main(as_of_date=as_of, lookback=args.lookback)


    _print_state("after")
    print("\n✅ daily_run done")


if __name__ == "__main__":
    main()
