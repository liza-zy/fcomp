'use client';

import Link from 'next/link';
import PortfolioMenu from './PortfolioMenu';
import type { TrackedPortfolio } from './types';

type Props = {
  portfolio: TrackedPortfolio;
  isLocked: boolean;
  onRename: (id: number, nextName: string) => Promise<void> | void;
  onDelete: (id: number) => Promise<void> | void;
};

function prettyRiskProfile(value: string | null) {
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

export default function PortfolioListItem({
  portfolio,
  isLocked,
  onRename,
  onDelete,
}: Props) {
  const title = portfolio.name?.trim() || `Портфель #${portfolio.id}`;

  return (
    <div
      className={[
        'rounded-3xl border border-gray-200 bg-white p-4 shadow-sm transition',
        isLocked ? 'opacity-45' : 'opacity-100',
      ].join(' ')}
    >
      <div className="flex items-start justify-between gap-3">
        <Link
          href={`/portfolios/${portfolio.id}`}
          className="min-w-0 flex-1"
        >
          <div className="text-base font-semibold text-gray-900">{title}</div>
          <div className="mt-1 text-sm text-gray-500">
            {prettyRiskProfile(portfolio.risk_profile)} · {portfolio.method}
          </div>
          <div className="mt-3 flex flex-wrap gap-2 text-xs text-gray-500">
            <span className="rounded-full bg-gray-100 px-2 py-1">
              lookback: {portfolio.lookback}
            </span>
            <span className="rounded-full bg-gray-100 px-2 py-1">
              status: {portfolio.status || 'active'}
            </span>
          </div>
        </Link>

        <PortfolioMenu
          portfolioName={title}
          onRename={(nextName) => onRename(portfolio.id, nextName)}
          onDelete={() => onDelete(portfolio.id)}
        />
      </div>
    </div>
  );
}
