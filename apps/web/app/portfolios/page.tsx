import BottomTabBar from '@/components/navigation/BottomTabBar';
import Link from 'next/link';

export default function Page() {
  return (
    <main className="min-h-screen bg-background p-6 pb-24">
      <div className="max-w-xl mx-auto">
        <h1 className="text-2xl font-semibold mb-3">Портфели</h1>
        <p className="text-muted-foreground mb-6">
          У вас пока нет собранных портфелей.
        </p>

        <div className="flex flex-col gap-3">
          <Link
            href="/risk-survey?back=/portfolios"
            className="w-full rounded-2xl px-5 py-4 bg-black text-white font-medium text-center"
          >
            Собрать портфель с нуля
          </Link>

          <Link
            href="/portfolio-input"
            className="w-full rounded-2xl px-5 py-4 border border-neutral-200 bg-white text-center"
          >
            Добавить существующий
          </Link>
        </div>
      </div>

      <BottomTabBar />
    </main>
  );
}
