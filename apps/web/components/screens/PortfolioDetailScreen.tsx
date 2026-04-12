'use client';

import { useEffect, useState } from 'react';
import BottomTabBar from '@/components/navigation/BottomTabBar';
import { getPortfolioDetails } from '@/lib/api';
import type { PortfolioDetails } from '@/components/portfolios/types';
import PortfolioHeader from '@/components/portfolios/PortfolioHeader';
import PortfolioCompositionTab from '@/components/portfolios/PortfolioCompositionTab';
import PortfolioHistoryTab from '@/components/portfolios/PortfolioHistoryTab';
import PortfolioAnalysisTab from '@/components/portfolios/PortfolioAnalysisTab';

type TabKey = 'actions' | 'history' | 'analysis';

const tabs: Array<{ key: TabKey; label: string }> = [
  { key: 'actions', label: 'Действия' },
  { key: 'history', label: 'История' },
  { key: 'analysis', label: 'Анализ' },
];

type Props = {
  portfolioId: number;
};

export default function PortfolioDetailScreen({ portfolioId }: Props) {
  const [activeTab, setActiveTab] = useState<TabKey>('actions');
  const [data, setData] = useState<PortfolioDetails | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadPortfolio = async () => {
    setLoading(true);
    setError(null);

    try {
      const response: PortfolioDetails = await getPortfolioDetails(portfolioId);
      setData(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Не удалось загрузить портфель');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadPortfolio();
  }, [portfolioId]);

  return (
    <main className="mx-auto flex min-h-screen w-full max-w-md flex-col bg-[#fafaf8] px-4 pb-24 pt-6">
      {loading && <div className="text-sm text-gray-500">Загрузка портфеля...</div>}

      {error && (
        <div className="rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {!loading && !error && data && (
        <>
          <PortfolioHeader portfolio={data.portfolio} />

          <div className="mt-5 grid grid-cols-3 gap-2 rounded-2xl bg-[#eef1ec] p-1">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                type="button"
                onClick={() => setActiveTab(tab.key)}
                className={[
                  'rounded-2xl px-3 py-2 text-sm font-medium transition',
                  activeTab === tab.key
                    ? 'bg-white text-gray-900 shadow-sm'
                    : 'text-gray-500',
                ].join(' ')}
              >
                {tab.label}
              </button>
            ))}
          </div>

          <div className="mt-4">
            {activeTab === 'actions' && (
              <PortfolioCompositionTab weights={data.weights} />
            )}

            {activeTab === 'history' && <PortfolioHistoryTab />}

            {activeTab === 'analysis' && <PortfolioAnalysisTab />}
          </div>
        </>
      )}

      <BottomTabBar />
    </main>
  );
}
