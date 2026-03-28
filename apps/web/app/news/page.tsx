import BottomTabBar from '@/components/navigation/BottomTabBar';
import Link from 'next/link';

export default function Page() {
  const isPremium = false;

  return (
    <main className="min-h-screen bg-background p-6 pb-24">
      <div className="max-w-xl mx-auto">
        <h1 className="text-2xl font-semibold mb-3">Новости</h1>

        {!isPremium ? (
          <div className="rounded-2xl border border-neutral-200 bg-white p-5">
            <p className="text-muted-foreground mb-4">
              Раздел новостей доступен в Premium.
            </p>
            <Link
              href="/settings"
              className="inline-block rounded-xl px-4 py-3 bg-black text-white"
            >
              Посмотреть тариф
            </Link>
          </div>
        ) : (
          <p className="text-muted-foreground">
            Здесь будет новостная лента и фильтры.
          </p>
        )}
      </div>

      <BottomTabBar isPremium={isPremium} />
    </main>
  );
}
