'use client';

import Link from 'next/link';
import type { TrackedPortfolio } from './types';

type Props = {
  portfolio: TrackedPortfolio;
};

function formatRiskProfile(value: string | null) {
  if (!value) return 'Без профиля';

  const map: Record<string, string> = {
    'ultra-conservative': 'Ultra-Conservative',
    conservative: 'Conservative',
    balanced: 'Balanced',
    growth: 'Growth',
    aggressive: 'Aggressive',
  };

  return map[value] ?? value;
}

export default function PortfolioHeader({ portfolio }: Props) {
  const title = portfolio.name?.trim() || `Портфель #${portfolio.id}`;

  return (
    <div className="rounded-3xl bg-gradient-to-br from-[#d9ead9] to-[#edf4ed] p-4">
      <Link
        href="/portfolios"
        className="mb-4 inline-flex items-center rounded-full bg-white px-3 py-2 text-sm text-gray-700 shadow-sm"
      >
        ← Назад
      </Link>

      <div className="text-2xl font-semibold text-gray-900">{title}</div>
      <div className="mt-1 text-sm text-gray-600">
        {formatRiskProfile(portfolio.risk_profile)} · {portfolio.method}
      </div>

      <div className="mt-4 flex flex-wrap gap-2 text-xs text-gray-600">
        <span className="rounded-full bg-white px-3 py-1">
          ID: {portfolio.id}
        </span>
        <span className="rounded-full bg-white px-3 py-1">
          lookback: {portfolio.lookback}
        </span>
        <span className="rounded-full bg-white px-3 py-1">
          status: {portfolio.status || 'active'}
        </span>
      </div>
    </div>
  );
}

