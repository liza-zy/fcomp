'use client';

import { useRouter } from 'next/navigation';

export default function WelcomeScreen() {
  const router = useRouter();

  return (
    <main className="min-h-screen bg-background flex flex-col justify-center p-6">
      <div className="max-w-md mx-auto w-full">
        <h1 className="text-3xl font-semibold mb-3 text-center">Добро пожаловать в FinCompass</h1>
        <p className="text-muted-foreground text-center mb-10">
          Выберите, как хотите начать работу с портфелем.
        </p>

        <div className="flex flex-col gap-4">
          <button
            onClick={() => router.push('/risk-survey?back=/welcome')}
            className="w-full rounded-2xl px-5 py-4 bg-black text-white text-left"
          >
            <div className="font-medium">Собрать портфель с нуля</div>
            <div className="text-sm text-white/80 mt-1">
              Пройдите короткий сценарий и получите стартовую структуру портфеля.
            </div>
          </button>

          <button
            onClick={() => router.push('/portfolio-input')}
            className="w-full rounded-2xl px-5 py-4 border border-border bg-white text-left"
          >
            <div className="font-medium">Добавить существующий</div>
            <div className="text-sm text-muted-foreground mt-1">
              Загрузите или введите текущий портфель для анализа.
            </div>
          </button>
        </div>
      </div>
    </main>
  );
}
