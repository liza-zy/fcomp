export type PortfolioStatus = 'active' | 'archived' | 'draft';

export type TrackedPortfolio = {
  id: number;
  name: string | null;
  risk_profile: string | null;
  method: string;
  lookback: number;
  position: number | null;
  status: PortfolioStatus | string | null;
  created_at: string;
  updated_at: string | null;
};

export type PortfolioWeight = {
  portfolio_id: number;
  instrument_uid: string;
  secid: string | null;
  boardid: string | null;
  weight: number;
};

export type PortfolioDetails = {
  portfolio: TrackedPortfolio;
  weights: PortfolioWeight[];
};

export type UserPortfolioLimits = {
  portfolio_limit: number;
  portfolio_count: number;
};

export type PortfoliosListResponse = {
  portfolios: TrackedPortfolio[];
  limits: UserPortfolioLimits;
};

export type RenamePortfolioPayload = {
  name: string;
};

export type PortfolioMethod = 'max_sharpe' | 'equal_weight_top_assets';

export type PreviewHolding = {
  secid: string;
  instrument_uid?: string | null;
  boardid?: string | null;
  weight: number;
};

export type PreviewChartPoint = {
  dt: string;
  value: number;
};

export type PreviewStats = {
  max_drawdown_6m: number;
  volatility_min_6m: number;
  volatility_max_6m: number;
  sharpe_6m: number;
};

export type PreviewPortfolioRequest = {
  telegram_id: number;
  portfolio_name: string;
  risk_profile: string;
  method: PortfolioMethod;
  apply_ai: boolean;
  cov_method: 'ledoit' | 'ewma';
  lookback: number;
  constraints: {
    sectors_include: string[];
    currencies_include: string[];
    exclude_secids: string[];
    max_weight: number;
    max_assets: number;
    budget_rub: number;
  };
};

export type PreviewPortfolioResponse = {
  portfolio_name: string;
  risk_profile: string;
  method: PortfolioMethod;
  apply_ai: boolean;
  lookback: number;
  holdings: PreviewHolding[];
  chart_6m: PreviewChartPoint[];
  stats: PreviewStats;
};

export type ExistingPortfolioOption = {
  id: number;
  name?: string | null;
  position?: number | null;
  status?: string | null;
};

export type SavePreviewPortfolioRequest = {
  telegram_id: number;
  portfolio_name: string;
  risk_profile: string;
  method: PortfolioMethod;
  apply_ai: boolean;
  lookback: number;
  holdings: PreviewHolding[];
  stats: PreviewStats;
};

export type SavePreviewPortfolioResponse = {
  ok: boolean;
  saved_portfolio_id?: number | null;
  limit_exceeded: boolean;
  existing_portfolios: ExistingPortfolioOption[];
};

export type ReplacePortfolioRequest = {
  telegram_id: number;
  delete_portfolio_id: number;
  portfolio_name: string;
  risk_profile: string;
  method: PortfolioMethod;
  apply_ai: boolean;
  lookback: number;
  holdings: PreviewHolding[];
  stats: PreviewStats;
};

export type ReplacePortfolioResponse = {
  ok: boolean;
  saved_portfolio_id: number;
  deleted_portfolio_id: number;
};
