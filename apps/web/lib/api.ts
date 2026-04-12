import { getUserSession } from '@/lib/userSession';
import type {
  PortfolioDetails,
  PortfoliosListResponse,
  TrackedPortfolio,
  PreviewPortfolioRequest,
  PreviewPortfolioResponse,
  SavePreviewPortfolioRequest,
  SavePreviewPortfolioResponse,
  ReplacePortfolioRequest,
  ReplacePortfolioResponse,
} from '@/components/portfolios/types';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || '';

function buildUrl(path: string) {
  return `${API_BASE_URL}${path}`;
}

async function readJsonOrThrow(response: Response) {
  const text = await response.text();

  if (!response.ok) {
    throw new Error(text || `Request failed with status ${response.status}`);
  }

  return text ? JSON.parse(text) : null;
}

function getTelegramIdOrThrow(): number {
  const session = getUserSession();

  if (!session?.telegram_id) {
    throw new Error('User session not found');
  }

  return session.telegram_id;
}

export async function bootstrapUser(initData: string) {
  const response = await fetch(buildUrl('/me/bootstrap'), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      init_data: initData,
    }),
  });

  return readJsonOrThrow(response);
}

export async function getTrackedPortfolios(): Promise<PortfoliosListResponse> {
  const telegramId = getTelegramIdOrThrow();

  const response = await fetch(
    buildUrl(`/portfolio?telegram_id=${telegramId}`),
    {
      method: 'GET',
      cache: 'no-store',
    }
  );

  return readJsonOrThrow(response);
}

export async function getPortfolioDetails(
  portfolioId: number | string
): Promise<PortfolioDetails> {
  const telegramId = getTelegramIdOrThrow();

  const response = await fetch(
    buildUrl(`/portfolio/${portfolioId}?telegram_id=${telegramId}`),
    {
      method: 'GET',
      cache: 'no-store',
    }
  );

  return readJsonOrThrow(response);
}

export async function renamePortfolio(
  portfolioId: number | string,
  payload: { name: string }
): Promise<TrackedPortfolio> {
  const telegramId = getTelegramIdOrThrow();

  const response = await fetch(
    buildUrl(`/portfolio/${portfolioId}`),
    {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        telegram_id: telegramId,
        name: payload.name,
      }),
    }
  );

  return readJsonOrThrow(response);
}

export async function deletePortfolio(portfolioId: number | string) {
  const telegramId = getTelegramIdOrThrow();

  const response = await fetch(
    buildUrl(`/portfolio/${portfolioId}?telegram_id=${telegramId}`),
    {
      method: 'DELETE',
    }
  );

  return readJsonOrThrow(response);
}

export async function previewPortfolio(
  payload: Omit<PreviewPortfolioRequest, 'telegram_id'>
): Promise<PreviewPortfolioResponse> {
  const telegramId = getTelegramIdOrThrow();

  const response = await fetch(buildUrl('/portfolio/preview'), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      telegram_id: telegramId,
      ...payload,
    }),
  });

  return readJsonOrThrow(response);
}

export async function savePreviewPortfolio(
  payload: Omit<SavePreviewPortfolioRequest, 'telegram_id'>
): Promise<SavePreviewPortfolioResponse> {
  const telegramId = getTelegramIdOrThrow();

  const response = await fetch(buildUrl('/portfolio/save'), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      telegram_id: telegramId,
      ...payload,
    }),
  });

  return readJsonOrThrow(response);
}

export async function replacePortfolio(
  payload: Omit<ReplacePortfolioRequest, 'telegram_id'>
): Promise<ReplacePortfolioResponse> {
  const telegramId = getTelegramIdOrThrow();

  const response = await fetch(buildUrl('/portfolio/replace'), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      telegram_id: telegramId,
      ...payload,
    }),
  });

  return readJsonOrThrow(response);
}
