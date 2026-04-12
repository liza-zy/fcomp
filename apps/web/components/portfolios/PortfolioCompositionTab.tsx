import type { PortfolioWeight } from './types';

type Props = {
  weights: PortfolioWeight[];
};

export default function PortfolioCompositionTab({ weights }: Props) {
  const sortedWeights = [...weights].sort((a, b) => b.weight - a.weight);

  return (
    <div className="space-y-3">
      {sortedWeights.map((item) => (
        <div
          key={`${item.portfolio_id}-${item.instrument_uid}`}
          className="rounded-2xl border border-gray-200 bg-white p-4 shadow-sm"
        >
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="text-base font-semibold text-gray-900">
                {item.secid || 'Без тикера'}
              </div>
              <div className="mt-1 text-xs text-gray-500">
                board: {item.boardid || '—'}
              </div>
              <div className="mt-2 break-all text-xs text-gray-500">
                {item.instrument_uid}
              </div>
            </div>

            <div className="rounded-full bg-[#edf4ed] px-3 py-1 text-sm font-semibold text-[#1f3b2d]">
              {(item.weight * 100).toFixed(0)}%
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
