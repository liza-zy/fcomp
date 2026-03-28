import { Suspense } from 'react';
import RiskSurveyClient from '@/components/screens/RiskSurveyClient';

export default function Page() {
  return (
    <Suspense
      fallback={
        <main className="min-h-screen bg-background p-6">
          <div className="max-w-xl mx-auto">Загрузка опроса...</div>
        </main>
      }
    >
      <RiskSurveyClient />
    </Suspense>
  );
}
