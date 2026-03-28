'use client';

import { useRouter } from 'next/navigation';

export default function PortfoliosScreen() {
  const router = useRouter();

  return (
    <main className="min-h-screen bg-background p-6">
      <div className="max-w-md mx-auto min-h-[80vh] flex flex-col items-center justify-center text-center">
        <h1 className="text-2xl font-semibold mb-3">Мои портфели</h1>

        <p className="text-muted-foreground mb-8">
          У вас пока нет собранных портфелей.
        </p>

        <button
          onClick={() => router.push('/risk-survey?back=/portfolios')}
          className="w-full rounded-2xl px-5 py-4 bg-black text-white font-medium"
        >
          Собрать портфель с нуля
        </button>
      </div>
    </main>
  );
}
