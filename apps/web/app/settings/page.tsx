import BottomTabBar from '@/components/navigation/BottomTabBar';

export default function Page() {
  return (
    <main className="min-h-screen bg-background p-6 pb-24">
      <div className="max-w-xl mx-auto">
        <h1 className="text-2xl font-semibold mb-3">Настройки</h1>
        <div className="rounded-2xl border border-neutral-200 bg-white p-5 space-y-3">
          <div>
            <div className="text-sm text-neutral-500">Тариф</div>
            <div className="font-medium">Бесплатный</div>
          </div>
          <div>
            <div className="text-sm text-neutral-500">Профиль</div>
            <div className="font-medium">Пока не определен</div>
          </div>
        </div>
      </div>

      <BottomTabBar />
    </main>
  );
}
