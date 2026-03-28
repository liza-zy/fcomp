'use client';

import { useRouter } from 'next/navigation';

export default function Page() {
  const router = useRouter();

  return (
    <main className="min-h-screen bg-background p-6">
      <div className="max-w-xl mx-auto">
        <button
          onClick={() => router.back()}
          className="text-sm text-muted-foreground mb-6"
        >
          ← Назад
        </button>

        <div className="rounded-3xl border border-neutral-200 bg-white p-6 text-center">
          <h1 className="text-2xl font-semibold mb-3">Добавить существующий портфель</h1>
          <p className="text-muted-foreground">
            Эта функция появится позднее.
          </p>
        </div>
      </div>
    </main>
  );
}
