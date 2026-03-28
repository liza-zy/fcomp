export type ProfileCode =
  | 'Ultra-Conservative'
  | 'Conservative'
  | 'Balanced'
  | 'Growth'
  | 'Aggressive';

export type QuizOption = {
  code: string;
  text: string;
  score: number;
};

export type QuizQuestion = {
  id: string;
  dimension: string;
  text: string;
  weight: number;
  options: QuizOption[];
};

export type RiskProfile = {
  code: ProfileCode;
  index: number;
  maxDrawdown: number;
  volatilityMin: number;
  volatilityMax: number;
  equityShareMin: number;
  equityShareMax: number;
  expectedReturnMin: number;
  expectedReturnMax: number;
  text: string;
};

export const RISK_PROFILES: RiskProfile[] = [
  {
    code: 'Ultra-Conservative',
    index: 0,
    maxDrawdown: 5,
    volatilityMin: 0,
    volatilityMax: 4,
    equityShareMin: 0,
    equityShareMax: 10,
    expectedReturnMin: 2,
    expectedReturnMax: 4,
    text:
      'Профиль для инвесторов, которые стремятся максимально сохранить капитал и почти не готовы к колебаниям стоимости. Портфель допускает очень ограниченную просадку — до 5% — и имеет минимальную волатильность (0–4%). Доля рискованных активов обычно крайне низкая (0–10%), что помогает поддерживать стабильность. Потенциальная доходность находится на уровне 2–4% при благоприятных рыночных условиях.',
  },
  {
    code: 'Conservative',
    index: 1,
    maxDrawdown: 10,
    volatilityMin: 5,
    volatilityMax: 8,
    equityShareMin: 10,
    equityShareMax: 30,
    expectedReturnMin: 4,
    expectedReturnMax: 6,
    text:
      'Профиль подходит инвесторам, которые готовы к небольшим колебаниям портфеля ради умеренного роста. Допустимая просадка — до 10%, волатильность находится в диапазоне 5–8%. В портфеле обычно присутствует ограниченная доля рискованных активов (10–30%), чтобы повысить доходность при контролируемом уровне риска. Долгосрочная ориентировочная доходность — 4–6% при устойчивой рыночной динамике.',
  },
  {
    code: 'Balanced',
    index: 2,
    maxDrawdown: 20,
    volatilityMin: 8,
    volatilityMax: 12,
    equityShareMin: 30,
    equityShareMax: 55,
    expectedReturnMin: 6,
    expectedReturnMax: 9,
    text:
      'Сбалансированный профиль сочетает стремление к доходности и готовность переносить заметные рыночные колебания. Портфель может временно снижаться на 10–20%, а волатильность находится в диапазоне 8–12%. Доля рискованных активов обычно составляет 30–55%, что обеспечивает потенциал роста при разумном уровне риска. Долгосрочная ожидаемая доходность — 6–9%.',
  },
  {
    code: 'Growth',
    index: 3,
    maxDrawdown: 30,
    volatilityMin: 12,
    volatilityMax: 18,
    equityShareMin: 60,
    equityShareMax: 80,
    expectedReturnMin: 9,
    expectedReturnMax: 13,
    text:
      'Профиль для инвесторов, ориентированных на долгосрочный рост капитала. Портфель допускает значительные просадки — до 30% — и характеризуется повышенной волатильностью (12–18%). Доля рискованных активов, как правило, высокая (60–80%), что увеличивает потенциальную доходность, но требует готовности переносить существенные колебания. Долгосрочная ориентировочная доходность — 9–13%.',
  },
  {
    code: 'Aggressive',
    index: 4,
    maxDrawdown: 50,
    volatilityMin: 18,
    volatilityMax: 25,
    equityShareMin: 80,
    equityShareMax: 100,
    expectedReturnMin: 12,
    expectedReturnMax: 18,
    text:
      'Агрессивный профиль предполагает стремление к максимальной доходности при готовности переносить резкие и глубокие рыночные просадки — вплоть до 50%. Волатильность портфеля повышенная (18–25%), а доля рискованных активов обычно составляет 80–100%. Такой профиль подходит тем, кто способен выдерживать длительные фазы снижения стоимости ради потенциально высокой долгосрочной доходности на уровне 12–18% и выше.',
  },
];

export const SCORE_RANGES: Array<{
  code: ProfileCode;
  min: number;
  max: number;
}> = [
  { code: 'Ultra-Conservative', min: 0, max: 6 },
  { code: 'Conservative', min: 7, max: 13 },
  { code: 'Balanced', min: 14, max: 20 },
  { code: 'Growth', min: 21, max: 26 },
  { code: 'Aggressive', min: 27, max: 32 },
];

export const QUIZ_QUESTIONS: QuizQuestion[] = [
  {
    id: 'horizon',
    dimension: 'horizon',
    text: 'На какой срок вы планируете инвестировать большую часть средств?',
    weight: 1,
    options: [
      { code: 'horizon_0', text: 'До 1 года', score: 0 },
      { code: 'horizon_1', text: 'От 1 до 3 лет', score: 1 },
      { code: 'horizon_2', text: 'От 3 до 5 лет', score: 2 },
      { code: 'horizon_3', text: 'От 5 до 10 лет', score: 3 },
      { code: 'horizon_4', text: 'Более 10 лет', score: 4 },
    ],
  },
  {
    id: 'income_stability',
    dimension: 'income',
    text: 'Насколько стабилен ваш доход и есть ли у вас финансовая подушка?',
    weight: 1,
    options: [
      { code: 'income_0', text: 'Доход нестабилен, подушки почти нет (хватает < 3 месяцев)', score: 0 },
      { code: 'income_1', text: 'Доход нестабилен, но есть подушка на 3–6 месяцев', score: 1 },
      { code: 'income_2', text: 'Доход стабильный, подушка на 3–6 месяцев', score: 2 },
      { code: 'income_3', text: 'Доход стабильный, подушка на 6–12 месяцев', score: 3 },
      { code: 'income_4', text: 'Высокий/стабильный доход, подушка ≥ 12 месяцев', score: 4 },
    ],
  },
  {
    id: 'capital_dependency',
    dimension: 'dependency',
    text: 'Насколько вы зависите от этих инвестируемых средств в ближайшие 3–5 лет?',
    weight: 1,
    options: [
      { code: 'depend_0', text: 'Без этих денег будет сложно оплачивать базовые расходы', score: 0 },
      { code: 'depend_1', text: 'Важно для крупных целей (жильё, образование) в ближайшие 3–5 лет', score: 1 },
      { code: 'depend_2', text: 'Деньги важны, но не критичны — основная подушка отдельно', score: 2 },
      { code: 'depend_3', text: 'Скорее деньги «для роста», базовые потребности обеспечены', score: 3 },
      { code: 'depend_4', text: 'Излишек капитала, готов(а) рискнуть ради высокой доходности', score: 4 },
    ],
  },
  {
    id: 'drawdown_behaviour',
    dimension: 'drawdown',
    text: 'Ваш портфель упал на 20%. Что вы скорее всего сделаете?',
    weight: 1,
    options: [
      { code: 'drawdown_0', text: 'Зафиксирую убыток, чтобы не потерять ещё больше', score: 0 },
      { code: 'drawdown_1', text: 'Сокращу рисковые активы, чтобы успокоить портфель', score: 1 },
      { code: 'drawdown_2', text: 'Ничего не буду делать, подожду', score: 2 },
      { code: 'drawdown_3', text: 'Частично докуплю активы на просадке', score: 3 },
      { code: 'drawdown_4', text: 'Активно докуплю, если фундаментал не изменился', score: 4 },
    ],
  },
  {
    id: 'comfort_volatility',
    dimension: 'comfort',
    text: 'Насколько вам комфортно видеть колебания портфеля?',
    weight: 1,
    options: [
      { code: 'comfort_0', text: 'Падение на 5–10% вызывает сильный стресс', score: 0 },
      { code: 'comfort_1', text: 'До 10% неприятно, но терпимо, выше — тяжело', score: 1 },
      { code: 'comfort_2', text: 'До 20% неприятно, но могу сохранять спокойствие', score: 2 },
      { code: 'comfort_3', text: 'До 30% не выбивает из долгосрочного плана', score: 3 },
      { code: 'comfort_4', text: 'Спокойно отношусь к большим колебаниям при долгом горизонте', score: 4 },
    ],
  },
  {
    id: 'goal',
    dimension: 'goal',
    text: 'Какая цель для вас сейчас наиболее важна?',
    weight: 1,
    options: [
      { code: 'goal_0', text: 'Сохранить покупательную способность денег, не потерять капитал', score: 0 },
      { code: 'goal_1', text: 'Немного обогнать инфляцию при низком риске', score: 1 },
      { code: 'goal_2', text: 'Создать накопления быстрее, чем через вклад/депозит', score: 2 },
      { code: 'goal_3', text: 'Существенно нарастить капитал, принимая заметный риск', score: 3 },
      { code: 'goal_4', text: 'Максимизировать прибыль, даже если путь будет волатильным', score: 4 },
    ],
  },
  {
    id: 'payoff_expectations',
    dimension: 'payoff',
    text: 'Через какой срок вы ожидаете увидеть результат, который вас удовлетворит?',
    weight: 1,
    options: [
      { code: 'payoff_0', text: 'Хочу видеть положительный результат уже через 1 год', score: 0 },
      { code: 'payoff_1', text: 'Готов(а) ждать 1–3 года, но хочу время от времени видеть плюс', score: 1 },
      { code: 'payoff_2', text: 'Готов(а) ждать 3–5 лет, понимаю, что не каждый год будет плюс', score: 2 },
      { code: 'payoff_3', text: 'Готов(а) ждать 5–7 лет, прибыль важнее стабильности по пути', score: 3 },
      { code: 'payoff_4', text: 'Готов(а) ждать 7–10+ лет, главное — итоговый результат', score: 4 },
    ],
  },
  {
    id: 'fears',
    dimension: 'fear',
    text: 'Что вас пугает больше всего при инвестировании?',
    weight: 1,
    options: [
      { code: 'fear_0', text: 'Потерять даже небольшую часть капитала', score: 0 },
      { code: 'fear_1', text: 'Долго видеть «красный» портфель и не понимать, что делать', score: 1 },
      { code: 'fear_2', text: 'Сделать ошибку из-за недостатка знаний и не заметить её вовремя', score: 2 },
      { code: 'fear_3', text: 'Упустить возможности роста, слишком осторожничая', score: 3 },
      { code: 'fear_4', text: 'Боюсь скорее ничего не делать и потерять время, чем переживать просадки', score: 4 },
    ],
  },
];

export function calculateQuizScore(answers: Record<string, number>): number {
  return QUIZ_QUESTIONS.reduce((sum, q) => {
    const raw = answers[q.id] ?? 0;
    return sum + raw * q.weight;
  }, 0);
}

export function getProfileByScore(score: number): RiskProfile {
  const range = SCORE_RANGES.find((r) => score >= r.min && score <= r.max);
  const code = range?.code ?? 'Balanced';
  return RISK_PROFILES.find((p) => p.code === code)!;
}

export function getNeighborProfile(profile: RiskProfile): RiskProfile | null {
  const higher = RISK_PROFILES.find((p) => p.index === profile.index + 1);
  const lower = RISK_PROFILES.find((p) => p.index === profile.index - 1);

  return higher ?? lower ?? null;
}

export function getProfileLabel(code: ProfileCode): string {
  const map: Record<ProfileCode, string> = {
    'Ultra-Conservative': 'Ультра-консервативный',
    Conservative: 'Консервативный',
    Balanced: 'Сбалансированный',
    Growth: 'Ростовой',
    Aggressive: 'Агрессивный',
  };
  return map[code];
}
