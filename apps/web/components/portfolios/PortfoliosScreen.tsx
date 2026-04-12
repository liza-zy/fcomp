'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import BottomTabBar from '@/components/navigation/BottomTabBar';
import { deletePortfolio, getTrackedPortfolios, renamePortfolio } from '@/lib/api';
import PortfolioListItem from './PortfolioListItem';
import type {
  PortfoliosListResponse,
  TrackedPortfolio,
  UserPortfolioLimits,
} from './types';

export default function PortfoliosScreen() {
  const [portfolios, setPortfolios] = useState<TrackedPortfolio[]>([]);
  const [limits, setLimits] = useState<UserPortfolioLimits>({
    portfolio_limit: 0,
    portfolio_count: 0,
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadPortfolios = async () => {
    setLoading(true);
    setError(null);

    try {
      const data: PortfoliosListResponse = await getTrackedPortfolios();
      setPortfolios(data.portfolios ?? []);
      setLimits(
        data.limits ?? {
          portfolio_limit: 0,
          portfolio_count: 0,
        }
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Не удалось загрузить портфели');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadPortfolios();
  }, []);

  const sortedPortfolios = useMemo(() => {
    return [...portfolios].sort((a, b) => {
      const aPos = a.position ?? Number.MAX_SAFE_INTEGER;
      const bPos = b.position ?? Number.MAX_SAFE_INTEGER;
      if (aPos !== bPos) return aPos - bPos;
      return a.id - b.id;
    });
  }, [portfolios]);

  const overLimitIds = useMemo(() => {
    const limit = limits.portfolio_limit;
    if (!limit || sortedPortfolios.length <= limit) {
      return new Set<number>();
    }
    return new Set(sortedPortfolios.slice(limit).map((item) => item.id));
  }, [limits.portfolio_limit, sortedPortfolios]);

  const handleRename = async (id: number, nextName: string) => {
    await renamePortfolio(id, { name: nextName });
    await loadPortfolios();
  };

  const handleDelete = async (id: number) => {
    await deletePortfolio(id);
    await loadPortfolios();
  };

  return (
    <main className="mx-auto flex min-h-screen w-full max-w-md flex-col bg-[#fafaf8] px-4 pb-24 pt-6">
      <div className="mb-5">
        <h1 className="text-2xl font-semibold text-gray-900">Портфели</h1>
        <p className="mt-1 text-sm text-gray-500">
          Список отслеживаемых портфелей и переход в детали.
        </p>
      </div>

      <div className="mb-4 rounded-3xl bg-gradient-to-br from-[#d9ead9] to-[#edf4ed] p-4">
        <div className="text-sm text-gray-700">
          Лимит портфелей:{' '}
          <span className="font-semibold">{limits.portfolio_limit}</span>
        </div>
        <div className="mt-1 text-sm text-gray-700">
          Текущее количество:{' '}
          <span className="font-semibold">{limits.portfolio_count}</span>
        </div>

        {limits.portfolio_count > limits.portfolio_limit && (
          <div className="mt-3 rounded-2xl bg-white/75 px-3 py-2 text-sm text-gray-700">
            Портфели сверх лимита отображаются полупрозрачно.
          </div>
        )}
      </div>

      <div className="mb-4 flex gap-3">
        <Link
          href="/risk-survey"
          className="flex-1 rounded-2xl bg-[#5B8F7B] hover:bg-[#4E7D6C] px-4 py-4 text-center text-white shadow-sm"
        >
          <div className="text-base font-semibold">Собрать портфель</div>
          <div className="text-xs opacity-80">На основе риск-профиля</div>
        </Link>

        <button
          type="button"
          className="flex-1 rounded-2xl border border-gray-300 bg-white px-4 py-3 text-sm font-medium text-gray-800"
        >
          Добавить существующий
        </button>
      </div>

      {loading && <div className="text-sm text-gray-500">Загрузка...</div>}

      {error && (
        <div className="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {!loading && !error && sortedPortfolios.length === 0 && (
        <div className="rounded-3xl border border-dashed border-gray-300 bg-white px-4 py-8 text-center text-sm text-gray-500">
          У вас пока нет портфелей.
        </div>
      )}

      {!loading && !error && sortedPortfolios.length > 0 && (
        <div className="space-y-3">
          {sortedPortfolios.map((portfolio) => (
            <PortfolioListItem
              key={portfolio.id}
              portfolio={portfolio}
              isLocked={overLimitIds.has(portfolio.id)}
              onRename={handleRename}
              onDelete={handleDelete}
            />
          ))}
        </div>
      )}

      <BottomTabBar currentTab="portfolios" />
    </main>
  );
}
