'use client';

import { useMemo, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import {
  getNeighborProfile,
  getProfileByScore,
  getProfileLabel,
  RISK_PROFILES,
  calculateQuizScore,
} from '@/lib/riskQuiz';
import {
  previewPortfolio,
  replacePortfolio,
  savePreviewPortfolio,
} from '@/lib/api';
import type { TrackedPortfolio } from '@/components/portfolios/types';
import {
  LineChart,
  Line,
  ResponsiveContainer,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from 'recharts';

type OptimizationMethod = 'max_sharpe' | 'equal_weight_top_assets';

type PreviewHolding = {
  secid: string;
  instrument_uid?: string | null;
  boardid?: string | null;
  weight: number;
};

type PreviewChartPoint = {
  dt: string;
  value: number;
};

type PreviewStats = {
  max_drawdown_6m: number;
  volatility_min_6m: number;
  volatility_max_6m: number;
  sharpe_6m: number;
};

type PreviewResponse = {
  portfolio_name: string;
  risk_profile: string;
  method: OptimizationMethod;
  apply_ai: boolean;
  lookback: number;
  holdings: PreviewHolding[];
  chart_6m: PreviewChartPoint[];
  stats: PreviewStats;
  as_of_date?: string;
};

type SaveResponse = {
  ok: boolean;
  saved_portfolio_id?: number | null;
  limit_exceeded: boolean;
  existing_portfolios: Array<{
    id: number;
    name?: string | null;
    position?: number | null;
    status?: string | null;
  }>;
};

type ReplaceResponse = {
  ok: boolean;
  saved_portfolio_id: number;
  deleted_portfolio_id: number;
};

type UiStep = 'form' | 'loading' | 'preview' | 'limit_resolve';

function formatMethodLabel(method: OptimizationMethod) {
  if (method === 'equal_weight_top_assets') {
    return 'Равные доли лучших активов';
  }
  return 'Максимальный Sharpe';
}

function formatProfileLabel(profileCode: string) {
  const profile = RISK_PROFILES.find((p) => p.code === profileCode);
  return profile ? getProfileLabel(profile.code) : profileCode;
}

function shortDateLabel(value: string) {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleDateString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
  });
}

export default function RiskResultClient() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const answersRaw = searchParams.get('answers');

  const parsedAnswers = useMemo(() => {
    if (!answersRaw) return {};
    try {
      return JSON.parse(answersRaw);
    } catch {
      return {};
    }
  }, [answersRaw]);

  const score = calculateQuizScore(parsedAnswers);
  const profile = getProfileByScore(score);
  const neighbor = getNeighborProfile(profile);

  const [step, setStep] = useState<UiStep>('form');
  const [portfolioName, setPortfolioName] = useState('');
  const [selectedRiskProfile, setSelectedRiskProfile] = useState(profile.code);
  const [selectedMethod, setSelectedMethod] =
    useState<OptimizationMethod>('max_sharpe');
  const [applyAi, setApplyAi] = useState(false);
  const [budgetRub, setBudgetRub] = useState('100000');
  const [isQualifiedInvestor, setIsQualifiedInvestor] = useState(false);


  const [preview, setPreview] = useState<PreviewResponse | null>(null);
  const [existingPortfolios, setExistingPortfolios] = useState<
    SaveResponse['existing_portfolios']
  >([]);
  const [selectedDeletePortfolioId, setSelectedDeletePortfolioId] = useState<
    number | null
  >(null);

  const [formError, setFormError] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const canBuild = portfolioName.trim().length > 0 && !isSubmitting;

  const handleBuildPreview = async () => {
    if (!portfolioName.trim()) {
      setFormError('Укажите название портфеля.');
      return;
    }

    const parsedBudget = Number(budgetRub);

    if (!Number.isFinite(parsedBudget) || parsedBudget <= 0) {
      setFormError('Укажите корректную сумму портфеля.');
      return;
    }

    setFormError(null);
    setSaveError(null);
    setStep('loading');
    setIsSubmitting(true);

    try {
      const response = (await previewPortfolio({
        portfolio_name: portfolioName.trim(),
        risk_profile: selectedRiskProfile,
        method: selectedMethod,
        apply_ai: applyAi,
        cov_method: 'ledoit',
        lookback: 252,
        constraints: {
          sectors_include: [],
          currencies_include: [],
          exclude_secids: [],
          max_weight: 0.15,
          min_weight: 0.01,
          max_assets: 10,
          budget_rub: 100000,
          is_qualified_investor: isQualifiedInvestor,
        },
      })) as PreviewResponse;

      setPreview(response);
      setStep('preview');
    } catch (err) {
      setFormError(
        err instanceof Error ? err.message : 'Не удалось собрать портфель'
      );
      setStep('form');
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleSavePortfolio = async () => {
    if (!preview) return;

    setSaveError(null);
    setIsSubmitting(true);

    const parsedBudget = Number(budgetRub);

    try {
      const response = (await savePreviewPortfolio({
        portfolio_name: preview.portfolio_name,
        risk_profile: preview.risk_profile,
        method: preview.method,
        apply_ai: preview.apply_ai,
        lookback: preview.lookback,
        holdings: preview.holdings,
        chart_6m: preview.chart_6m,
        stats: preview.stats,
        budget_rub: parsedBudget,
        as_of_date: preview.as_of_date ?? '2026-02-28',
        is_qualified_investor: isQualifiedInvestor,
      })) as SaveResponse;

      if (response.limit_exceeded) {
        setExistingPortfolios(response.existing_portfolios || []);
        setSelectedDeletePortfolioId(
          response.existing_portfolios?.[0]?.id ?? null
        );
        setStep('limit_resolve');
        return;
      }

      if (response.ok) {
        router.push('/portfolios');
        return;
      }

      setSaveError('Не удалось сохранить портфель.');
    } catch (err) {
      setSaveError(
        err instanceof Error ? err.message : 'Не удалось сохранить портфель'
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleReplacePortfolio = async () => {
    if (!preview) return;
    if (!selectedDeletePortfolioId) {
      setSaveError('Выберите портфель для удаления.');
      return;
    }

    setSaveError(null);
    setIsSubmitting(true);
    const parsedBudget = Number(budgetRub);

    try {
      const response = (await replacePortfolio({
        delete_portfolio_id: selectedDeletePortfolioId,
        portfolio_name: preview.portfolio_name,
        risk_profile: preview.risk_profile,
        method: preview.method,
        apply_ai: preview.apply_ai,
        lookback: preview.lookback,
        holdings: preview.holdings,
        chart_6m: preview.chart_6m,
        stats: preview.stats,
        budget_rub: parsedBudget,
        as_of_date: preview.as_of_date ?? '2026-02-28',
        is_qualified_investor: isQualifiedInvestor,
      })) as ReplaceResponse;

      if (response.ok) {
        router.push('/portfolios');
        return;
      }

      setSaveError('Не удалось заменить портфель.');
    } catch (err) {
      setSaveError(
        err instanceof Error ? err.message : 'Не удалось заменить портфель'
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  if (step === 'loading') {
    return (
      <main className="min-h-screen bg-background p-6">
        <div className="mx-auto max-w-xl">
          <div className="rounded-3xl border border-neutral-200 bg-white p-8 text-center">
            <div className="mb-4 text-sm text-muted-foreground">
              Идет формирование портфеля
            </div>
            <h1 className="mb-3 text-2xl font-semibold">
              Подбираем состав и считаем метрики
            </h1>
            <p className="text-sm text-muted-foreground">
              Это может занять несколько секунд.
            </p>

            <div className="mt-8 overflow-hidden rounded-full bg-neutral-200">
              <div className="h-2 w-2/3 animate-pulse rounded-full bg-black" />
            </div>
          </div>
        </div>
      </main>
    );
  }

  if (step === 'preview' && preview) {
    return (
      <main className="min-h-screen bg-background p-6">
        <div className="mx-auto max-w-xl">
          <button
            onClick={() => setStep('form')}
            className="mb-6 text-sm text-muted-foreground"
          >
            Назад
          </button>

          <h1 className="mb-2 text-3xl font-semibold">Предлагаемый портфель</h1>
          <p className="mb-6 text-sm text-muted-foreground">
            {preview.portfolio_name} · {formatProfileLabel(preview.risk_profile)} ·{' '}
            {formatMethodLabel(preview.method)}
          </p>
          <p className="mb-2 text-sm text-muted-foreground">
            Бюджет: {Number(budgetRub).toLocaleString('ru-RU')} ₽
          </p>
          <div className="mb-6 rounded-2xl border border-neutral-200 bg-white p-5">
            <h2 className="mb-4 text-xl font-semibold">Инструменты и веса</h2>
            <div className="space-y-3">
              {preview.holdings.map((holding) => (
                <div
                  key={`${holding.secid}-${holding.instrument_uid ?? 'x'}`}
                  className="flex items-center justify-between rounded-xl border border-neutral-100 px-4 py-3"
                >
                  <div>
                    <div className="font-medium">{holding.secid}</div>
                    <div className="text-xs text-muted-foreground">
                      {holding.instrument_uid || 'instrument_uid отсутствует'}
                    </div>
                  </div>
                  <div className="font-semibold">
                    {(holding.weight * 100).toFixed(1)}%
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="mb-6 rounded-2xl border border-neutral-200 bg-white p-5">
            <h2 className="mb-4 text-xl font-semibold">
              Стоимость за последние 6 месяцев
            </h2>

            <div className="h-[260px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={preview.chart_6m}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis
                    dataKey="dt"
                    tickFormatter={shortDateLabel}
                    minTickGap={24}
                  />
                  <YAxis />
                  <Tooltip
                    labelFormatter={(value) => shortDateLabel(String(value))}
                  />
                  <Line
                    type="monotone"
                    dataKey="value"
                    stroke="#2E5E4E"
                    strokeWidth={2}
                    dot={false}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="mb-6 rounded-2xl border border-neutral-200 bg-white p-5">
            <h2 className="mb-4 text-xl font-semibold">Статистика</h2>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div className="rounded-xl border border-neutral-100 p-4">
                <div className="text-sm text-muted-foreground">
                  Максимальное отклонение за 6 месяцев
                </div>
                <div className="mt-1 text-lg font-semibold">
                  {preview.stats.max_drawdown_6m.toFixed(2)}%
                </div>
              </div>

              <div className="rounded-xl border border-neutral-100 p-4">
                <div className="text-sm text-muted-foreground">
                  Диапазон волатильности
                </div>
                <div className="mt-1 text-lg font-semibold">
                  {preview.stats.volatility_min_6m.toFixed(2)}% –{' '}
                  {preview.stats.volatility_max_6m.toFixed(2)}%
                </div>
              </div>

              <div className="rounded-xl border border-neutral-100 p-4 sm:col-span-2">
                <div className="text-sm text-muted-foreground">
                  Коэффициент Шарпа
                </div>
                <div className="mt-1 text-lg font-semibold">
                  {preview.stats.sharpe_6m.toFixed(2)}
                </div>
              </div>
            </div>
          </div>

          {saveError && (
            <div className="mb-4 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {saveError}
            </div>
          )}

          <button
            onClick={handleSavePortfolio}
            disabled={isSubmitting}
            className="w-full rounded-2xl bg-[#2E5E4E] px-5 py-4 font-medium text-white disabled:opacity-50"
          >
            Сохранить портфель
          </button>
        </div>
      </main>
    );
  }

  if (step === 'limit_resolve' && preview) {
    return (
      <main className="min-h-screen bg-background p-6">
        <div className="mx-auto max-w-xl">
          <button
            onClick={() => setStep('preview')}
            className="mb-6 text-sm text-muted-foreground"
          >
            Назад
          </button>

          <h1 className="mb-3 text-3xl font-semibold">
            Лимит портфелей достигнут
          </h1>

          <p className="mb-6 text-muted-foreground">
            Чтобы сохранить новый портфель, нужно удалить один из существующих.
          </p>

          <div className="mb-6 rounded-2xl border border-neutral-200 bg-white p-5">
            <h2 className="mb-4 text-xl font-semibold">Выберите портфель</h2>

            <div className="space-y-3">
              {existingPortfolios.map((portfolio) => {
                const active = selectedDeletePortfolioId === portfolio.id;

                return (
                  <button
                    key={portfolio.id}
                    type="button"
                    onClick={() => setSelectedDeletePortfolioId(portfolio.id)}
                    className={`w-full rounded-2xl border px-4 py-4 text-left transition ${
                      active
                        ? 'border-black bg-black text-white'
                        : 'border-neutral-200 bg-white text-black'
                    }`}
                  >
                    <div className="font-medium">
                      {portfolio.name?.trim() || `Портфель #${portfolio.id}`}
                    </div>
                    <div
                      className={`mt-1 text-xs ${
                        active ? 'text-white/70' : 'text-muted-foreground'
                      }`}
                    >
                      position: {portfolio.position ?? '—'} · status:{' '}
                      {portfolio.status ?? 'active'}
                    </div>
                  </button>
                );
              })}
            </div>
          </div>

          {saveError && (
            <div className="mb-4 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {saveError}
            </div>
          )}

          <button
            onClick={handleReplacePortfolio}
            disabled={isSubmitting || !selectedDeletePortfolioId}
            className="w-full rounded-2xl bg-black px-5 py-4 font-medium text-white disabled:opacity-50"
          >
            Удалить выбранный и сохранить новый
          </button>
        </div>
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-background p-6">
      <div className="mx-auto max-w-xl">
        <button
          onClick={() => router.push('/risk-survey')}
          className="mb-6 text-sm text-muted-foreground"
        >
          Назад
        </button>

        <h1 className="mb-3 text-3xl font-semibold">
          Ваш риск-профиль: {getProfileLabel(profile.code)}
        </h1>

        <p className="mb-6 text-muted-foreground">Набрано баллов: {score}</p>

        <div className="mb-6 rounded-2xl border border-neutral-200 bg-white p-5">
          <div className="mb-4">
            <div className="mb-2 text-sm text-muted-foreground">
              Шкала профилей
            </div>

            <div className="relative py-6">
              <div className="absolute left-0 right-0 top-1/2 h-1 -translate-y-1/2 rounded-full bg-neutral-200" />
              <div className="relative flex justify-between gap-2">
                {RISK_PROFILES.map((p) => {
                  const active = p.code === profile.code;
                  const secondary = neighbor?.code === p.code;

                  return (
                    <div
                      key={p.code}
                      className="flex flex-1 flex-col items-center text-center"
                    >
                      <div
                        className={`h-4 w-4 rounded-full border-2 ${
                          active
                            ? 'border-black bg-black'
                            : secondary
                            ? 'border-neutral-400 bg-neutral-400'
                            : 'border-neutral-300 bg-white'
                        }`}
                      />
                      <div className="mt-3 text-[10px] sm:text-xs">
                        {getProfileLabel(p.code)}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>

          {neighbor && (
            <p className="text-sm text-muted-foreground">
              Соседний профиль по шкале:{' '}
              <span className="font-medium text-black">
                {getProfileLabel(neighbor.code)}
              </span>
            </p>
          )}
        </div>

        <div className="mb-6 rounded-2xl border border-neutral-200 bg-white p-5">
          <h2 className="mb-3 text-xl font-semibold">Описание профиля</h2>
          <p className="whitespace-pre-line text-muted-foreground">
            {profile.text}
          </p>
        </div>

        <div className="mb-6 rounded-2xl border border-neutral-200 bg-white p-5">
          <h2 className="mb-4 text-xl font-semibold">Параметры портфеля</h2>

          <div className="mb-4">
            <label className="mb-2 block text-sm font-medium">
              Название портфеля (обязательно)
            </label>
            <input
              value={portfolioName}
              onChange={(e) => setPortfolioName(e.target.value)}
              placeholder="Например: Мой сбалансированный портфель"
              className="w-full rounded-2xl border border-neutral-200 px-4 py-3 outline-none focus:border-black"
            />
          </div>

          <div className="mb-4">
            <label className="mb-2 block text-sm font-medium text-gray-700">
              Сумма портфеля, ₽
            </label>
            <input
              type="number"
              min="1"
              step="1000"
              value={budgetRub}
              onChange={(e) => setBudgetRub(e.target.value)}
              className="w-full rounded-2xl border border-neutral-200 bg-white px-4 py-3 text-sm"
              placeholder="Например, 100000"
            />
          </div>

          <label className="mb-6 flex items-start gap-3 rounded-2xl border border-neutral-200 bg-white px-4 py-3">
            <input
              type="checkbox"
              checked={isQualifiedInvestor}
              onChange={(e) => setIsQualifiedInvestor(e.target.checked)}
              className="mt-1"
            />
            <div>
              <div className="text-sm font-medium text-gray-900">
                Я квалифицированный инвестор
              </div>
              <div className="text-xs text-muted-foreground">
                Использовать расширенный набор доступных инструментов.
              </div>
            </div>
          </label>

          <div className="mb-4">
            <label className="mb-2 block text-sm font-medium">
              Риск-профиль
            </label>
            <select
              value={selectedRiskProfile}
              onChange={(e) => setSelectedRiskProfile(e.target.value)}
              className="w-full rounded-2xl border border-neutral-200 bg-white px-4 py-3 outline-none focus:border-black"
            >
              {RISK_PROFILES.map((p) => (
                <option key={p.code} value={p.code}>
                  {getProfileLabel(p.code)}
                </option>
              ))}
            </select>
          </div>

          <div className="mb-4">
            <label className="mb-2 block text-sm font-medium">
              Метод оптимизации
            </label>
            <select
              value={selectedMethod}
              onChange={(e) =>
                setSelectedMethod(e.target.value as OptimizationMethod)
              }
              className="w-full rounded-2xl border border-neutral-200 bg-white px-4 py-3 outline-none focus:border-black"
            >
              <option value="max_sharpe">Максимальный Sharpe</option>
              <option value="equal_weight_top_assets">
                Равные доли лучших активов
              </option>
            </select>
          </div>

          <label className="flex items-center gap-3 rounded-2xl border border-neutral-200 px-4 py-3">
            <input
              type="checkbox"
              checked={applyAi}
              onChange={(e) => setApplyAi(e.target.checked)}
            />
            <span className="text-sm">
              Применить ИИ для пересчета параметров
            </span>
          </label>
        </div>

        {formError && (
          <div className="mb-4 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {formError}
          </div>
        )}

        <button
          onClick={handleBuildPreview}
          disabled={!canBuild}
          className="w-full rounded-2xl bg-black px-5 py-4 font-medium text-white disabled:opacity-50"
        >
          Собрать портфель для заданных параметров
        </button>
      </div>
    </main>
  );
}
