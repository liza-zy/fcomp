ASSET_CLASS_QUERIES = {
    # Акции: stock/shares (потом можно сузить до TQBR)
    "equity": [
        {"engine": "stock", "market": "shares"},
    ],

    # Фонды/ETF: тоже stock/shares, но фильтруем по ETF board'ам
    "fund": [
        {"engine": "stock", "market": "shares", "boardids": ["TQTF", "TQTE", "TQTD"]},
    ],

    # Валюта: currency/selt
    "fx": [
        {"engine": "currency", "market": "selt"},
    ],

    # Металлы: пока оставим как есть (если 0 — подберём другой engine/market)
    "metal": [
        {"engine": "commodity", "market": "metals"},
    ],
}