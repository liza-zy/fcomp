'use client';

import { useMemo, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { QUIZ_QUESTIONS } from '@/lib/riskQuiz';

export default function RiskSurveyClient() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const backHref = searchParams.get('back') || '/welcome';

  const [step, setStep] = useState(0);
  const [answers, setAnswers] = useState<Record<string, number>>({});

  const question = QUIZ_QUESTIONS[step];
  const selected = answers[question.id];

  const progress = useMemo(() => {
    return Math.round(((step + 1) / QUIZ_QUESTIONS.length) * 100);
  }, [step]);

  const goBack = () => {
    if (step === 0) {
      router.push(backHref);
      return;
    }
    setStep((prev) => prev - 1);
  };

  const chooseOption = (score: number) => {
    setAnswers((prev) => ({
      ...prev,
      [question.id]: score,
    }));
  };

  const goNext = () => {
    if (selected === undefined) return;

    if (step === QUIZ_QUESTIONS.length - 1) {
      const payload = encodeURIComponent(
        JSON.stringify({
          ...answers,
          [question.id]: selected,
        })
      );
      router.push(`/risk-result?answers=${payload}`);
      return;
    }

    setStep((prev) => prev + 1);
  };

  return (
    <main className="min-h-screen bg-background p-6">
      <div className="max-w-xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <button
            onClick={goBack}
            className="text-sm text-muted-foreground"
          >
            Назад
          </button>
          <div className="text-sm text-muted-foreground">
            {step + 1} / {QUIZ_QUESTIONS.length}
          </div>
        </div>

        <div className="w-full h-2 bg-muted rounded-full mb-8 overflow-hidden">
          <div
            className="h-full bg-black rounded-full transition-all"
            style={{ width: `${progress}%` }}
          />
        </div>

        <h1 className="text-2xl font-semibold mb-8">{question.text}</h1>

        <div className="flex flex-col gap-3 mb-8">
          {question.options.map((option) => {
            const isActive = selected === option.score;

            return (
              <button
                key={option.code}
                onClick={() => chooseOption(option.score)}
                className={`text-left rounded-2xl border px-4 py-4 transition ${
                  isActive
                    ? 'border-black bg-black text-white'
                    : 'border-neutral-200 bg-white text-black'
                }`}
              >
                {option.text}
              </button>
            );
          })}
        </div>

        <button
          onClick={goNext}
          disabled={selected === undefined}
          className="w-full rounded-2xl px-5 py-4 bg-black text-white font-medium disabled:opacity-50"
        >
          {step === QUIZ_QUESTIONS.length - 1 ? 'Показать результат' : 'Далее'}
        </button>
      </div>
    </main>
  );
}
