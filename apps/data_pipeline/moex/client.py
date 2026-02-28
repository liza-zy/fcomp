from __future__ import annotations

from typing import Any, Dict, Optional, Iterator, Tuple, List

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from apps.data_pipeline.runtime.settings import MOEX_BASE_URL


class MoexISSClient:
    def __init__(self, base_url: str = MOEX_BASE_URL):
        self.base_url = base_url.rstrip("/")

        # Важно: отдельные таймауты, чтобы не зависать на connect/read
        self.timeout = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)

        # Ограничим пул соединений (чтобы не застревало)
        self.limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.8, min=1, max=10))
    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        with httpx.Client(timeout=self.timeout, limits=self.limits) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            return r.json()

    def paged_table(
        self,
        path: str,
        table_name: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 100,
        start: int = 0,
        max_pages: int = 10_000,
    ) -> Iterator[Tuple[List[str], List[list]]]:
        params = dict(params or {})
        params.setdefault("iss.meta", "off")
        params.setdefault("iss.only", table_name)
        params.setdefault("limit", page_size)

        cur = start
        pages = 0
        
        seen_first_keys = set()

        while pages < max_pages:
            params["start"] = cur
            data = self.get_json(path, params=params)

            if table_name not in data:
                break

            columns = data[table_name].get("columns", [])
            rows = data[table_name].get("data", [])

            if not rows:
                break

            # анти-луп: если первая строка страницы такая же, как уже была — выходим
            first_key = tuple(rows[0]) if rows else None
            if first_key in seen_first_keys:
                break
            if first_key is not None:
                seen_first_keys.add(first_key)

            yield columns, rows

            # анти-луп: если len(rows)=0 или cur не двигается — выходим
            step = len(rows)
            if step <= 0:
                break

            cur += step
            pages += 1