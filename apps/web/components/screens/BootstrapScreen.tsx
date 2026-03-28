'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { bootstrapUser } from '@/lib/api';
import { getTelegramInitData } from '@/lib/telegram';
import { saveUserSession } from '@/lib/userSession';

export default function BootstrapScreen() {
  const router = useRouter();

  const [status, setStatus] = useState('mount');
  const [details, setDetails] = useState('');

  useEffect(() => {
    let cancelled = false;

    const run = async () => {
      try {
        setStatus('reading initData');

        const initData = getTelegramInitData();
        setDetails(`initData length: ${initData?.length ?? 0}`);

        if (!initData) {
          setStatus('no initData');
          return;
        }

        setStatus('calling bootstrap');

        const result = await bootstrapUser(initData);

        if (cancelled) return;

        setStatus('bootstrap ok');
        setDetails(JSON.stringify(result));

        saveUserSession(result.user);

        if (result.is_new_user) {
          router.replace('/onboarding');
        } else {
          router.replace('/portfolios');
        }
      } catch (error) {
        if (cancelled) return;

        setStatus('bootstrap failed');
        setDetails(error instanceof Error ? error.message : String(error));
      }
    };

    run();

    return () => {
      cancelled = true;
    };
  }, [router]);

  return (
    <main className="min-h-screen flex items-center justify-center bg-background p-6">
      <div className="max-w-md w-full rounded-2xl border border-neutral-200 bg-white p-5">
        <div className="text-lg font-semibold mb-3">Bootstrap debug</div>
        <div className="text-sm mb-2">Status: {status}</div>
        <div className="text-xs text-neutral-500 break-all whitespace-pre-wrap">
          {details}
        </div>
      </div>
    </main>
  );
}
