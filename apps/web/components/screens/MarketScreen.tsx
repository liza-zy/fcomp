'use client';

import Link from 'next/link';
import { useEffect, useState } from 'react';
import BottomTabBar from '@/components/navigation/BottomTabBar';
import { fetchMarket } from '@/lib/marketApi';
import { getUserPlan, isPremiumUser } from '@/lib/userSession';

type MarketItem = {
  instrument_uid: string;
  secid: string;
  title: string;
  subtitle?: string | null;
  sector?: string | null;
  asset_class?: string | null;
  currency?: string | null;
  boardid?: string | null;
  price?: number | null;
  change_percent?: number | null;
  risk_profile?: string | null;
  risk_score?: number | null;
  ann_vol_pct?: number | null;
};

export default function MarketScreen() {
  const premium = isPremiumUser();
  const plan = getUserPlan();

  const [query, setQuery] = useState('');
  const [items, setItems] = useState<MarketItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const timer = setTimeout(async () => {
      try {
        setLoading(true);
        setError(null);
        const data = await fetchMarket(query.trim());
        setItems(data.items ?? []);
      } catch {
        setError('Не удалось загрузить данные рынка');
      } finally {
        setLoading(false);
      }
    }, 250);

    return () => clearTimeout(timer);
  }, [query]);

  return (
    <main className="min-h-screen bg-background p-6 pb-24">
      <div className="max-w-xl mx-auto">
        <div className="mb-6">
          <h1 className="text-2xl font-semibold mb-2">Рынок</h1>
          <p className="text-sm text-muted-foreground">
            Поиск по тикеру, названию или сектору. План: {plan === 'premium' ? 'Premium' : 'Бесплатный'}
          </p>
        </div>

        <div className="mb-6">
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Поиск: SBER, Сбербанк, Финансы..."
            className="w-full rounded-2xl border border-neutral-200 bg-white px-4 py-3 outline-none"
          />
        </div>

        {loading && (
          <div className="rounded-2xl border border-neutral-200 bg-white p-4 text-sm text-neutral-500">
            Загружаю данные рынка...
          </div>
        )}

        {error && (
          <div className="rounded-2xl border border-red-200 bg-white p-4 text-sm text-red-600">
            {error}
          </div>
        )}

        {!loading && !error && (
          <div className="flex flex-col gap-4">
            {items.map((asset) => (
              <Link
                key={asset.instrument_uid}
                href={`/market/${asset.secid}`}
                className="block rounded-2xl border border-neutral-200 bg-white p-4"
              >
                <div className="flex items-start justify-between gap-4 mb-3">
                  <div>
                    <div className="text-lg font-semibold">{asset.secid}</div>
                    <div className="text-sm text-neutral-500">{asset.title}</div>
                  </div>

                  <div className="text-right">
                    <div className="font-semibold">
                      {asset.price ?? '—'} {asset.currency ?? ''}
                    </div>
                    {asset.change_percent != null && (
                      <div
                        className={`text-sm ${
                          asset.change_percent >= 0 ? 'text-green-600' : 'text-red-600'
                        }`}
                      >
                        {asset.change_percent >= 0 ? '+' : ''}
                        {asset.change_percent}%
                      </div>
                    )}
                  </div>
                </div>

                <div className="text-sm text-neutral-600 mb-2">
                  Сектор: {asset.sector ?? '—'}
                </div>

                {asset.subtitle && (
                  <div className="text-sm text-neutral-700">
                    {asset.subtitle}
                  </div>
                )}

                {premium && asset.risk_profile && (
                  <div className="mt-3 rounded-xl bg-neutral-50 px-3 py-2 text-sm text-neutral-700">
                    <div>Риск-профиль: {asset.risk_profile}</div>
                    {asset.risk_score != null && (
                      <div>Risk score: {asset.risk_score}</div>
                    )}
                    {asset.ann_vol_pct != null && (
                      <div>Годовая волатильность: {asset.ann_vol_pct.toFixed(2)}%</div>
                    )}
                  </div>
                )}
              </Link>
            ))}

            {items.length === 0 && (
              <div className="rounded-2xl border border-neutral-200 bg-white p-4 text-sm text-neutral-500">
                Ничего не найдено.
              </div>
            )}
          </div>
        )}
      </div>

      <BottomTabBar isPremium={premium} />
    </main>
  );
}
