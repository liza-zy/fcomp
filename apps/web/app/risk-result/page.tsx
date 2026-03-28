import { Suspense } from 'react';
import RiskResultClient from '@/components/screens/RiskResultClient';

export default function Page() {
  return (
    <Suspense
      fallback={
        <main className="min-h-screen bg-background p-6">
          <div className="max-w-xl mx-auto">Загрузка результата...</div>
        </main>
      }
    >
      <RiskResultClient />
    </Suspense>
  );
}
