from __future__ import annotations

import math
import pandas as pd
import duckdb
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

router = APIRouter(prefix="/market", tags=["market"])

DUCKDB_PATH = "/app/data_lake/moex.duckdb"


def get_con():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def is_missing(v):
    try:
        return pd.isna(v)
    except Exception:
        return v is None


def safe_str(v):
    if is_missing(v):
        return None
    return str(v)


def safe_float(v):
    if is_missing(v):
        return None
    return float(v)


def safe_int(v):
    if is_missing(v):
        return None
    return int(v)

class MarketItem(BaseModel):
    instrument_uid: str
    secid: str
    title: str
    subtitle: Optional[str] = None
    sector: Optional[str] = None
    asset_class: Optional[str] = None
    currency: Optional[str] = None
    boardid: Optional[str] = None
    price: Optional[float] = None
    change_percent: Optional[float] = None
    risk_profile: Optional[str] = None
    risk_score: Optional[int] = None
    ann_vol_pct: Optional[float] = None


class MarketListResponse(BaseModel):
    items: list[MarketItem]


class MarketDetailsResponse(BaseModel):
    instrument_uid: str
    secid: str
    title: str
    full_name: Optional[str] = None
    sector: Optional[str] = None
    asset_class: Optional[str] = None
    currency: Optional[str] = None
    boardid: Optional[str] = None
    isin: Optional[str] = None
    lot: Optional[int] = None
    price: Optional[float] = None
    prev_close: Optional[float] = None
    change_percent: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[float] = None
    value: Optional[float] = None
    last_dt: Optional[str] = None
    risk_profile: Optional[str] = None
    risk_score: Optional[int] = None
    ann_vol_pct: Optional[float] = None

class ChartPoint(BaseModel):
    label: str
    value: float


class MarketChartResponse(BaseModel):
    secid: str
    period: str
    points: list[ChartPoint]


class MetricItem(BaseModel):
    key: str
    label: str
    value: str
    help: str


class MarketMetricsResponse(BaseModel):
    secid: str
    items: list[MetricItem]

@router.get("", response_model=MarketListResponse)
def market_list(q: str = Query(default="")):
    con = get_con()

    sql = """
    with ranked as (
      select
        b.instrument_uid,
        b.secid,
        b.dt,
        b.close,
        row_number() over (partition by b.instrument_uid order by b.dt desc) as rn
      from bars_1d b
    ),
    latest as (
      select instrument_uid, secid, dt, close
      from ranked
      where rn = 1
    ),
    prev as (
      select instrument_uid, close as prev_close
      from ranked
      where rn = 2
    )
    select
      i.instrument_uid,
      i.secid,
      coalesce(i.shortname, i.name, i.secid) as title,
      i.name as subtitle,
      i.group_name as sector,
      i.asset_class,
      i.currencyid as currency,
      i.boardid,
      l.close as price,
      case
        when p.prev_close is null or p.prev_close = 0 or l.close is null then null
        else round((l.close - p.prev_close) / p.prev_close * 100, 2)
      end as change_percent,
      arp.risk_profile,
      arp.risk_score,
      arp.ann_vol_pct
    from ref_instruments i
    left join latest l on l.instrument_uid = i.instrument_uid
    left join prev p on p.instrument_uid = i.instrument_uid
    left join asset_risk_profile arp on arp.instrument_uid = i.instrument_uid
    where i.secid is not null
      and (
        ? = ''
        or lower(i.secid) like '%' || lower(?) || '%'
        or lower(coalesce(i.shortname, '')) like '%' || lower(?) || '%'
        or lower(coalesce(i.name, '')) like '%' || lower(?) || '%'
        or lower(coalesce(i.group_name, '')) like '%' || lower(?) || '%'
      )
    order by
      case when l.close is null then 1 else 0 end,
      i.secid
    limit 100
    """

    df = con.execute(sql, [q, q, q, q, q]).fetchdf()

    items = []
    for _, row in df.iterrows():
        items.append(
            MarketItem(
                instrument_uid=safe_str(row["instrument_uid"]) or "",
                secid=safe_str(row["secid"]) or "",
                title=safe_str(row["title"]) or "",
                subtitle=safe_str(row["subtitle"]),
                sector=safe_str(row["sector"]),
                asset_class=safe_str(row["asset_class"]),
                currency=safe_str(row["currency"]),
                boardid=safe_str(row["boardid"]),
                price=safe_float(row["price"]),
                change_percent=safe_float(row["change_percent"]),
                risk_profile=safe_str(row["risk_profile"]),
                risk_score=safe_int(row["risk_score"]),
                ann_vol_pct=safe_float(row["ann_vol_pct"]),
            )
        )

    return MarketListResponse(items=items)


@router.get("/{secid}", response_model=MarketDetailsResponse)
def market_details(secid: str):
    con = get_con()

    sql = """
    with ranked as (
      select
        b.*,
        row_number() over (partition by b.instrument_uid order by b.dt desc) as rn
      from bars_1d b
    ),
    latest as (
      select * from ranked where rn = 1
    ),
    prev as (
      select instrument_uid, close as prev_close
      from ranked
      where rn = 2
    )
    select
      i.instrument_uid,
      i.secid,
      coalesce(i.shortname, i.name, i.secid) as title,
      i.name as full_name,
      i.group_name as sector,
      i.asset_class,
      i.currencyid as currency,
      i.boardid,
      i.isin,
      i.lot,
      l.close as price,
      p.prev_close,
      case
        when p.prev_close is null or p.prev_close = 0 or l.close is null then null
        else round((l.close - p.prev_close) / p.prev_close * 100, 2)
      end as change_percent,
      l.open,
      l.high,
      l.low,
      l.volume,
      l.value,
      cast(l.dt as varchar) as last_dt,
      arp.risk_profile,
      arp.risk_score,
      arp.ann_vol_pct
    from ref_instruments i
    left join latest l on l.instrument_uid = i.instrument_uid
    left join prev p on p.instrument_uid = i.instrument_uid
    left join asset_risk_profile arp on arp.instrument_uid = i.instrument_uid
    where i.secid = ?
    limit 1
    """

    row = con.execute(sql, [secid]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Instrument not found")

    cols = [d[0] for d in con.description]
    data = dict(zip(cols, row))

    return MarketDetailsResponse(
        instrument_uid=safe_str(data["instrument_uid"]) or "",
        secid=safe_str(data["secid"]) or "",
        title=safe_str(data["title"]) or "",
        full_name=safe_str(data["full_name"]),
        sector=safe_str(data["sector"]),
        asset_class=safe_str(data["asset_class"]),
        currency=safe_str(data["currency"]),
        boardid=safe_str(data["boardid"]),
        isin=safe_str(data["isin"]),
        lot=safe_int(data["lot"]),
        price=safe_float(data["price"]),
        prev_close=safe_float(data["prev_close"]),
        change_percent=safe_float(data["change_percent"]),
        open=safe_float(data["open"]),
        high=safe_float(data["high"]),
        low=safe_float(data["low"]),
        volume=safe_float(data["volume"]),
        value=safe_float(data["value"]),
        last_dt=safe_str(data["last_dt"]),
        risk_profile=safe_str(data["risk_profile"]),
        risk_score=safe_int(data["risk_score"]),
        ann_vol_pct=safe_float(data["ann_vol_pct"]),
    )


@router.get("/{secid}/chart", response_model=MarketChartResponse)
def market_chart(secid: str, period: str = Query(default="1M")):
    con = get_con()

    if period == "1M":
        limit_days = 30
    elif period == "3M":
        limit_days = 90
    elif period == "1Y":
        limit_days = 365
    else:
        limit_days = 99999

    sql = f"""
    with filtered as (
      select
        dt,
        close
      from bars_1d
      where secid = ?
        and close is not null
      order by dt desc
      limit {limit_days}
    )
    select *
    from filtered
    order by dt asc
    """

    df = con.execute(sql, [secid]).fetchdf()

    if df.empty:
        raise HTTPException(status_code=404, detail="Chart data not found")

    points = []

    for _, row in df.iterrows():
        dt = row["dt"]
        label = dt.strftime("%d.%m")

        points.append(
            ChartPoint(
                label=label,
                value=float(row["close"]),
            )
        )

    return MarketChartResponse(
        secid=secid,
        period=period,
        points=points,
    )


@router.get("/{secid}/metrics", response_model=MarketMetricsResponse)
def market_metrics(secid: str):
    con = get_con()

    sql = """
    with ranked as (
      select
        b.*,
        row_number() over (partition by b.instrument_uid order by b.dt desc) as rn
      from bars_1d b
      where b.secid = ?
    ),
    latest as (
      select * from ranked where rn = 1
    ),
    range_52 as (
      select
        min(low) as min_52w,
        max(high) as max_52w
      from bars_1d
      where secid = ?
    )
    select
      i.secid,
      i.asset_class,
      i.currencyid,
      l.close,
      l.volume,
      l.value,
      r.min_52w,
      r.max_52w
    from ref_instruments i
    left join latest l on l.instrument_uid = i.instrument_uid
    left join range_52 r on true
    where i.secid = ?
    limit 1
    """

    row = con.execute(sql, [secid, secid, secid]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Metrics not found")

    cols = [d[0] for d in con.description]
    data = dict(zip(cols, row))

    items = [
        MetricItem(
            key="asset_class",
            label="Класс актива",
            value=str(data["asset_class"] or "—"),
            help="Тип инструмента, например акция, облигация или фонд."
        ),
        MetricItem(
            key="currency",
            label="Валюта",
            value=str(data["currencyid"] or "—"),
            help="Валюта торгов и расчета цены актива."
        ),
        MetricItem(
            key="volume",
            label="Объем",
            value=str(round(float(data["volume"]), 2)) if data["volume"] is not None else "—",
            help="Количество бумаг, проторгованных за последнюю доступную сессию."
        ),
        MetricItem(
            key="value",
            label="Оборот",
            value=str(round(float(data["value"]), 2)) if data["value"] is not None else "—",
            help="Денежный объем торгов по активу за последнюю доступную сессию."
        ),
        MetricItem(
            key="week_52_range",
            label="Диапазон 52 недель",
            value=(
                f'{round(float(data["min_52w"]), 2)} – {round(float(data["max_52w"]), 2)}'
                if data["min_52w"] is not None and data["max_52w"] is not None
                else "—"
            ),
            help="Минимальная и максимальная цена актива за последний доступный период наблюдений."
        ),
        MetricItem(
            key="sharpe",
            label="Коэффициент Шарпа",
            value="—",
            help="Показывает доходность актива с поправкой на риск. Чем выше значение, тем эффективнее соотношение доходности и волатильности."
        ),
        MetricItem(
            key="beta",
            label="Бета",
            value="—",
            help="Показывает, насколько актив чувствителен к движениям рынка. Значение выше 1 означает, что актив обычно движется сильнее рынка."
        ),
    ]

    return MarketMetricsResponse(secid=secid, items=items)
