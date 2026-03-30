from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
import xml.etree.ElementTree as ET

import pandas as pd
import requests
import yfinance as yf

from apps.data_pipeline.rl_dataset.schema import RLDatabaseConfig, get_connection


@dataclass(frozen=True)
class MacroFactorsParams:
    start_date: date | None = None
    end_date: date | None = None
    money_market_code: str = "1"
    money_market_field: str = "C1"
    overwrite: bool = True


def _parse_cbr_decimal(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", ".")
    if s in {"", "-", "None"}:
        return None
    return float(s)


def _resolve_bounds(con, start_date, end_date):
    db_min, db_max = con.execute(
        "SELECT MIN(date), MAX(date) FROM market_prices"
    ).fetchone()
    return (start_date or db_min, end_date or db_max)


def _load_dates(con, start_date, end_date):
    df = con.execute(
        """
        SELECT DISTINCT date
        FROM market_prices
        WHERE date BETWEEN ? AND ?
        ORDER BY date
        """,
        [start_date, end_date],
    ).df()
    df["date"] = pd.to_datetime(df["date"])
    return df


# ---------- MOEX ----------
def _moex(secid, engine, market, start_date, end_date):
    url = f"https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/securities/{secid}.json"
    params = {
        "from": start_date.isoformat(),
        "till": end_date.isoformat(),
        "iss.meta": "off",
        "history.columns": "TRADEDATE,CLOSE",
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()["history"]

    df = pd.DataFrame(data["data"], columns=data["columns"])
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["TRADEDATE"])
    df["close"] = pd.to_numeric(df["CLOSE"], errors="coerce")
    return df[["date", "close"]].dropna()


# ---------- CBR ----------
def _usd_rub(start_date, end_date):
    r = requests.get(
        "https://www.cbr.ru/scripts/XML_dynamic.asp",
        params={
            "date_req1": start_date.strftime("%d/%m/%Y"),
            "date_req2": end_date.strftime("%d/%m/%Y"),
            "VAL_NM_RQ": "R01235",
        },
        timeout=60,
    )
    root = ET.fromstring(r.content)

    rows = []
    for rec in root.findall(".//Record"):
        rows.append({
            "date": pd.to_datetime(rec.attrib["Date"], dayfirst=True),
            "usd_rub": _parse_cbr_decimal(rec.findtext("Value")),
        })

    return pd.DataFrame(rows)


def _gold(start_date, end_date):
    r = requests.get(
        "https://www.cbr.ru/scripts/xml_metall.asp",
        params={
            "date_req1": start_date.strftime("%d/%m/%Y"),
            "date_req2": end_date.strftime("%d/%m/%Y"),
        },
        timeout=60,
    )
    root = ET.fromstring(r.content)

    rows = []
    for rec in root.findall(".//Record"):
        if rec.attrib["Code"] != "1":
            continue
        rows.append({
            "date": pd.to_datetime(rec.attrib["Date"], dayfirst=True),
            "gold": _parse_cbr_decimal(rec.findtext("Buy")),
        })

    return pd.DataFrame(rows)


def _key_rate(start_date, end_date):
    url = (
        "https://www.cbr.ru/eng/hd_base/KeyRate/"
        f"?UniDbQuery.Posted=True"
        f"&UniDbQuery.From={start_date.strftime('%d.%m.%Y')}"
        f"&UniDbQuery.To={end_date.strftime('%d.%m.%Y')}"
    )

    html = requests.get(url, timeout=60).text
    rows = re.findall(r"<tr>(.*?)</tr>", html, re.DOTALL)

    data = []
    for row in rows:
        cols = re.findall(r"<td.*?>(.*?)</td>", row)
        if len(cols) != 2:
            continue

        d = pd.to_datetime(cols[0], dayfirst=True, errors="coerce")
        r = _parse_cbr_decimal(cols[1])

        if pd.notna(d) and r is not None:
            data.append({"date": d, "cbr_key_rate": r})

    return pd.DataFrame(data)


def _money_market(start_date, end_date, code, field):
    r = requests.get(
        "https://www.cbr.ru/scripts/xml_mkr.asp",
        params={
            "date_req1": start_date.strftime("%d/%m/%Y"),
            "date_req2": end_date.strftime("%d/%m/%Y"),
        },
        timeout=60,
    )
    root = ET.fromstring(r.content)

    rows = []
    for rec in root.findall(".//Record"):
        if rec.attrib["Code"] != code:
            continue

        rows.append({
            "date": pd.to_datetime(rec.attrib["Date"], dayfirst=True),
            "money_market_rate": _parse_cbr_decimal(rec.findtext(field)),
        })

    return pd.DataFrame(rows)


# ---------- Brent ----------
def _brent(start_date, end_date):
    df = yf.download("BZ=F", start=start_date, end=end_date, progress=False)

    if df.empty:
        return df

    if isinstance(df.columns, pd.MultiIndex):
        close = df["Close"]["BZ=F"]
    else:
        close = df["Close"]

    df = close.reset_index()
    df.columns = ["date", "brent"]
    return df


# ---------- MAIN ----------
def run_macro_factors(params=MacroFactorsParams(), cfg=RLDatabaseConfig()):
    con = get_connection(cfg.target_db)

    start_date, end_date = _resolve_bounds(con, params.start_date, params.end_date)
    dates = _load_dates(con, start_date, end_date)

    imoex = _moex("IMOEX", "stock", "index", start_date, end_date).rename(columns={"close": "imoex"})
    rgbi = _moex("RGBI", "stock", "index", start_date, end_date).rename(columns={"close": "rgbi"})
    usd = _usd_rub(start_date, end_date)
    gold = _gold(start_date, end_date)
    brent = _brent(start_date, end_date)
    rate = _key_rate(start_date, end_date)
    mm = _money_market(start_date, end_date, params.money_market_code, params.money_market_field)

    df = dates.copy()

    for part in [imoex, rgbi, usd, gold, brent, rate, mm]:
        if not part.empty:
            df = df.merge(part, on="date", how="left")

    df = df.sort_values("date").ffill()

    # returns
    for col in ["usd_rub", "brent", "imoex", "rgbi", "gold"]:
        df[f"{col}_ret_1p"] = df[col].pct_change()

    if params.overwrite:
        con.execute("DELETE FROM macro_factors")

    con.register("tmp_df", df)

    con.execute("""
        INSERT INTO macro_factors
        SELECT
            date,
            usd_rub,
            brent,
            imoex,
            rgbi,
            gold,
            cbr_key_rate,
            money_market_rate,
            NULL,
            usd_rub_ret_1p,
            brent_ret_1p,
            imoex_ret_1p,
            rgbi_ret_1p,
            gold_ret_1p
        FROM tmp_df
    """)

    print("macro_factors loaded:", len(df))

    con.close()


if __name__ == "__main__":
    run_macro_factors()
