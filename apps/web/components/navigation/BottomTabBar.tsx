'use client';

import { usePathname, useRouter } from 'next/navigation';
import { getUserPlan } from '@/lib/userSession';

type Tab = {
  path: string;
  label: string;
  icon: string;
  premiumOnly?: boolean;
};

const tabs: Tab[] = [
  { path: '/portfolios', label: 'Портфели', icon: '📁' },
  { path: '/market', label: 'Рынок', icon: '📈' },
  { path: '/news', label: 'Новости', icon: '📰', premiumOnly: true },
  { path: '/settings', label: 'Настройки', icon: '⚙️' },
];

export default function BottomTabBar() {
  const router = useRouter();
  const pathname = usePathname();
  const isPremium = getUserPlan() === 'premium';

  return (
    <div className="fixed bottom-0 left-0 right-0 z-30 border-t border-gray-200 bg-white/95 backdrop-blur">
      <div className="mx-auto flex w-full max-w-md items-center justify-around px-2 py-2">
        {tabs.map((tab) => {
          const isActive =
            pathname === tab.path || pathname.startsWith(`${tab.path}/`);
          const isLocked = !!tab.premiumOnly && !isPremium;

          return (
            <button
              key={tab.path}
              type="button"
              onClick={() => {
                if (isLocked) return;
                router.push(tab.path);
              }}
              className="relative flex min-w-[68px] flex-col items-center gap-1 px-3 py-2"
            >
              <span className="text-base">{tab.icon}</span>

              <span
                className={[
                  'text-xs',
                  isActive ? 'font-semibold text-gray-900' : 'text-gray-500',
                  isLocked ? 'opacity-50' : '',
                ].join(' ')}
              >
                {tab.label}
              </span>

              {isLocked && (
                <span className="absolute right-2 top-1 text-[10px] text-amber-600">
                  PRO
                </span>
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
}
