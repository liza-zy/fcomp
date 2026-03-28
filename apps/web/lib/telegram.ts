export function getTelegramInitData(): string {
  if (typeof window === 'undefined') return '';
  return (window as any)?.Telegram?.WebApp?.initData ?? '';
}
