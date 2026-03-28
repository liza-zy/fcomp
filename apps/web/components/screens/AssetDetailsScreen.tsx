'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import BottomTabBar from '@/components/navigation/BottomTabBar';
import {
  fetchMarketAsset,
  fetchMarketChart,
  fetchMarketMetrics,
} from '@/lib/marketApi';
import { isPremiumUser } from '@/lib/userSession';

type AssetDetails = {
  instrument_uid: string;
  secid: string;
  title: string;
  full_name?: string | null;
  sector?: string | null;
  asset_class?: string | null;
  currency?: string | null;
  boardid?: string | null;
  isin?: string | null;
  lot?: number | null;
  price?: number | null;
  prev_close?: number | null;
  change_percent?: number | null;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  volume?: number | null;
  value?: number | null;
  last_dt?: string | null;
  risk_profile?: string | null;
  risk_score?: number | null;
  ann_vol_pct?: number | null;
};

type ChartPoint = {
  label: string;
  value: number;
};

type MetricItem = {
  key: string;
  label: string;
  value: string;
  help: string;
};

function translateAssetClass(value?: string | null) {
  if (!value) return '—';

  const map: Record<string, string> = {
    equity: 'Акция',
    bond: 'Облигация',
    etf: 'Фонд',
    currency: 'Валюта',
    futures: 'Фьючерс',
  };

  return map[value] ?? value;
}

function formatMetricValue(item: MetricItem) {
  if (item.key === 'asset_class') {
    return translateAssetClass(item.value);
  }
  return item.value;
}

function LineChart({
  points,
}: {
  points: ChartPoint[];
}) {
  const width = 720;
  const height = 260;
  const paddingLeft = 24;
  const paddingRight = 16;
  const paddingTop = 16;
  const paddingBottom = 36;

  const values = points.map((p) => p.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const chartWidth = width - paddingLeft - paddingRight;
  const chartHeight = height - paddingTop - paddingBottom;

  const coords = points.map((point, index) => {
    const x =
      paddingLeft +
      (index * chartWidth) / Math.max(points.length - 1, 1);

    const y =
      paddingTop +
      ((max - point.value) / range) * chartHeight;

    return {
      ...point,
      x,
      y,
    };
  });

  const path = coords
    .map((p, index) => `${index === 0 ? 'M' : 'L'} ${p.x} ${p.y}`)
    .join(' ');

  return (
    <div className="overflow-x-auto">
      <div className="min-w-[720px]">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          className="w-full h-[260px]"
        >
          <line
            x1={paddingLeft}
            y1={height - paddingBottom}
            x2={width - paddingRight}
            y2={height - paddingBottom}
            stroke="#E5E7EB"
            strokeWidth="1"
          />

          <path
            d={path}
            fill="none"
            stroke="#10B981"
            strokeWidth="3"
            strokeLinejoin="round"
            strokeLinecap="round"
          />

          {coords.map((p) => (
            <g key={`${p.label}-${p.x}`}>
              <circle cx={p.x} cy={p.y} r="5" fill="#10B981" />
              <circle cx={p.x} cy={p.y} r="2.5" fill="#111827" />
            </g>
          ))}

          {coords.map((p) => (
            <text
              key={`label-${p.label}-${p.x}`}
              x={p.x}
              y={height - 10}
              textAnchor="middle"
              fontSize="11"
              fill="#6B7280"
            >
              {p.label}
            </text>
          ))}
        </svg>
      </div>
    </div>
  );
}

export default function AssetDetailsScreen({ ticker }: { ticker: string }) {
  const router = useRouter();
  const premium = isPremiumUser();

  const [asset, setAsset] = useState<AssetDetails | null>(null);
  const [chartPoints, setChartPoints] = useState<ChartPoint[]>([]);
  const [metrics, setMetrics] = useState<MetricItem[]>([]);
  const [period, setPeriod] = useState('1M');
  const [loading, setLoading] = useState(true);
  const [openHelp, setOpenHelp] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    const run = async () => {
      try {
        setLoading(true);

        const [assetData, chartData, metricsData] = await Promise.all([
          fetchMarketAsset(ticker),
          fetchMarketChart(ticker, period),
          fetchMarketMetrics(ticker),
        ]);

        if (!cancelled) {
          setAsset(assetData);
          setChartPoints(chartData.points ?? []);
          setMetrics(metricsData.items ?? []);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    run();
    return () => {
      cancelled = true;
    };
  }, [ticker, period]);

  const periodButtons = useMemo(() => ['1M', '3M', '1Y', 'ALL'], []);

  if (loading) {
    return (
      <main className="min-h-screen bg-background p-6 pb-24">
        <div className="max-w-xl mx-auto">Загрузка актива...</div>
        <BottomTabBar isPremium={premium} />
      </main>
    );
  }

  if (!asset) {
    return (
      <main className="min-h-screen bg-background p-6 pb-24">
        <div className="max-w-xl mx-auto">
          <button
            onClick={() => router.push('/market')}
            className="text-sm text-muted-foreground mb-6"
          >
            ← Назад
          </button>

          <div className="rounded-3xl border border-neutral-200 bg-white p-5">
            Актив не найден.
          </div>
        </div>

        <BottomTabBar isPremium={premium} />
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-background p-6 pb-24">
      <div className="max-w-xl mx-auto">
        <button
          onClick={() => router.push('/market')}
          className="text-sm text-muted-foreground mb-6"
        >
          ← Назад
        </button>

        <div className="mb-6">
          <div className="flex items-center gap-3 mb-2">
            <h1 className="text-3xl font-semibold">{asset.secid}</h1>
            {premium && asset.risk_profile && (
              <span className="rounded-full bg-emerald-100 px-3 py-1 text-sm text-emerald-700">
                {asset.risk_profile}
              </span>
            )}
          </div>

          <div className="text-neutral-500 mb-4">{asset.title}</div>

          <div className="flex items-center gap-3 mb-2">
            <div className="text-4xl font-bold">₽{asset.price ?? '—'}</div>
            {asset.change_percent != null && (
              <div
                className={
                  asset.change_percent >= 0
                    ? 'text-emerald-600 text-2xl'
                    : 'text-red-600 text-2xl'
                }
              >
                {asset.change_percent >= 0 ? '+' : ''}
                {asset.change_percent}%
              </div>
            )}
          </div>

          <div className="text-neutral-500">
            Последняя дата: {asset.last_dt ?? '—'}
          </div>
        </div>

        <div className="rounded-3xl border border-neutral-200 bg-white p-5 mb-6">
          {chartPoints.length > 0 ? (
            <LineChart points={chartPoints} />
          ) : (
            <div className="h-[260px] flex items-center justify-center text-neutral-400">
              Нет данных для графика
            </div>
          )}

          <div className="flex gap-2 flex-wrap mt-4">
            {periodButtons.map((p) => (
              <button
                key={p}
                onClick={() => setPeriod(p)}
                className={`rounded-2xl px-4 py-2 ${
                  period === p
                    ? 'bg-blue-600 text-white'
                    : 'bg-neutral-100 text-black'
                }`}
              >
                {p}
              </button>
            ))}
          </div>
        </div>

        <div className="rounded-3xl border border-neutral-200 bg-white p-5 mb-6">
          <h2 className="text-2xl font-semibold mb-5">Ключевые показатели</h2>

          <div className="space-y-4">
            {metrics.map((item) => (
              <div
                key={item.key}
                className="border-b border-neutral-100 pb-4 last:border-b-0 last:pb-0"
              >
                <div className="flex items-center justify-between gap-4">
                  <div className="flex items-center gap-2">
                    <span className="text-neutral-500">{item.label}</span>
                    <button
                      onClick={() =>
                        setOpenHelp(openHelp === item.key ? null : item.key)
                      }
                      className="h-5 w-5 rounded-full border border-neutral-300 text-xs text-neutral-500"
                    >
                      ?
                    </button>
                  </div>

                  <div className="font-medium">
                    {formatMetricValue(item)}
                  </div>
                </div>

                {openHelp === item.key && (
                  <div className="mt-2 text-sm text-neutral-500">
                    {item.help}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      <BottomTabBar isPremium={premium} />
    </main>
  );
}
