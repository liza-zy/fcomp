'use client';

import { usePathname, useRouter } from 'next/navigation';

type Tab = {
  path: string;
  label: string;
  icon: string;
  premiumOnly?: boolean;
};

const tabs: Tab[] = [
  { path: '/portfolios', label: 'Портфель', icon: '🏠' },
  { path: '/market', label: 'Рынок', icon: '📈' },
  { path: '/news', label: 'Новости', icon: '📰', premiumOnly: true },
  { path: '/settings', label: 'Настройки', icon: '⚙️' },
];

export default function BottomTabBar({ isPremium = false }: { isPremium?: boolean }) {
  const router = useRouter();
  const pathname = usePathname();

  return (
    <div className="fixed bottom-0 left-0 right-0 border-t bg-white">
      <div className="max-w-xl mx-auto flex items-center justify-around py-2">
        {tabs.map((tab) => {
          const isActive = pathname === tab.path;
          const isLocked = tab.premiumOnly && !isPremium;

          return (
            <button
              key={tab.path}
              onClick={() => router.push(tab.path)}
              className="flex flex-col items-center gap-1 px-3 py-2 min-w-[68px] relative"
            >
              <div className="relative text-xl">
                <span>{tab.icon}</span>
                {isLocked && (
                  <span className="absolute -top-1 -right-2 text-[10px]">🔒</span>
                )}
              </div>
              <span
                className={`text-xs ${
                  isActive ? 'text-black font-medium' : 'text-neutral-500'
                }`}
              >
                {tab.label}
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}
