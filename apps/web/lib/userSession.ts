export type UserPlan = 'free' | 'premium';

export type UserSession = {
  id: number;
  telegram_id: number;
  username?: string | null;
  first_name?: string | null;
  last_name?: string | null;
  plan: UserPlan;
  plan_expires_at?: string | null;
};

const SESSION_KEY = 'fincompass_user_session';

export function saveUserSession(session: UserSession) {
  if (typeof window === 'undefined') return;
  localStorage.setItem(SESSION_KEY, JSON.stringify(session));
}

export function getUserSession(): UserSession | null {
  if (typeof window === 'undefined') return null;

  const raw = localStorage.getItem(SESSION_KEY);
  if (!raw) return null;

  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function getUserPlan(): UserPlan {
  const session = getUserSession();
  return session?.plan === 'premium' ? 'premium' : 'free';
}

export function isPremiumUser(): boolean {
  return getUserPlan() === 'premium';
}
