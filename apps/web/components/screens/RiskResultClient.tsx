'use client';

import { useMemo } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import {
  calculateQuizScore,
  getNeighborProfile,
  getProfileByScore,
  getProfileLabel,
  RISK_PROFILES,
} from '@/lib/riskQuiz';

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

  return (
    <main className="min-h-screen bg-background p-6">
      <div className="max-w-xl mx-auto">
        <button
          onClick={() => router.push('/risk-survey')}
          className="text-sm text-muted-foreground mb-6"
        >
          Назад
        </button>

        <h1 className="text-3xl font-semibold mb-3">
          Ваш риск-профиль: {getProfileLabel(profile.code)}
        </h1>

        <p className="text-muted-foreground mb-6">
          Набрано баллов: {score}
        </p>

        <div className="rounded-2xl border border-neutral-200 bg-white p-5 mb-6">
          <div className="mb-4">
            <div className="text-sm text-muted-foreground mb-2">Шкала профилей</div>
            <div className="relative py-6">
              <div className="absolute left-0 right-0 top-1/2 -translate-y-1/2 h-1 bg-neutral-200 rounded-full" />
              <div className="relative flex justify-between gap-2">
                {RISK_PROFILES.map((p) => {
                  const active = p.code === profile.code;
                  const secondary = neighbor?.code === p.code;

                  return (
                    <div key={p.code} className="flex flex-col items-center flex-1 text-center">
                      <div
                        className={`h-4 w-4 rounded-full border-2 ${
                          active
                            ? 'bg-black border-black'
                            : secondary
                            ? 'bg-neutral-400 border-neutral-400'
                            : 'bg-white border-neutral-300'
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
              <span className="text-black font-medium">
                {getProfileLabel(neighbor.code)}
              </span>
            </p>
          )}
        </div>

        <div className="rounded-2xl border border-neutral-200 bg-white p-5 mb-6">
          <h2 className="text-xl font-semibold mb-3">Описание профиля</h2>
          <p className="text-muted-foreground whitespace-pre-line">
            {profile.text}
          </p>
        </div>

        <button
          onClick={() => router.push('/portfolios')}
          className="w-full rounded-2xl px-5 py-4 bg-black text-white font-medium"
        >
          Продолжить
        </button>
      </div>
    </main>
  );
}
