const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || '';

export async function fetchMarket(q = '') {
  const url = `${API_BASE_URL}/api/market?q=${encodeURIComponent(q)}`;
  const response = await fetch(url, { cache: 'no-store' });

  if (!response.ok) {
    throw new Error(`Market request failed: ${response.status}`);
  }

  return response.json();
}

export async function fetchMarketAsset(secid: string) {
  const url = `${API_BASE_URL}/api/market/${encodeURIComponent(secid)}`;
  const response = await fetch(url, { cache: 'no-store' });

  if (!response.ok) {
    throw new Error(`Market details request failed: ${response.status}`);
  }

  return response.json();
}

export async function fetchMarketChart(secid: string, period = '1M') {
  const response = await fetch(
    `${API_BASE_URL}/api/market/${encodeURIComponent(secid)}/chart?period=${encodeURIComponent(period)}`,
    { cache: 'no-store' }
  );

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Market chart request failed: ${response.status} ${text}`);
  }

  return response.json();
}

export async function fetchMarketMetrics(secid: string) {
  const response = await fetch(
    `${API_BASE_URL}/api/market/${encodeURIComponent(secid)}/metrics`,
    { cache: 'no-store' }
  );

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Market metrics request failed: ${response.status} ${text}`);
  }

  return response.json();
}
