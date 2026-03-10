"use client";

import { useEffect, useMemo, useState } from "react";

type TgUser = { id: number } | null;

type BuildReq = {
  telegram_id: number;
  risk_profile?: string | null;
  cov_method?: "ledoit" | "ewma";
  lookback?: number;
  constraints?: {
    sectors_include?: string[];
    currencies_include?: string[];
    exclude_secids?: string[];
    max_weight?: number;
    budget_rub?: number;
    // max_assets (заглушка, бек пока не использует)
    max_assets?: number;
  };
};

type PortfolioResult = {
  method: "max_sharpe" | "max_return";
  as_of: string;
  risk_profile: string;
  weights: Record<string, number>;
  metrics: Record<string, number>;
  chart_png_b64?: string | null;
};

type BuildResp = { portfolios: PortfolioResult[] };

const DEFAULT_RISK = "Balanced";

const RISK_OPTIONS = [
  "Ultra-Conservative",
  "Conservative",
  "Balanced",
  "Growth",
  "Aggressive",
];

export default function Page() {
  const [tgId, setTgId] = useState<number | null>(null);
  const [risk, setRisk] = useState<string>(DEFAULT_RISK);

  // заглушки/поля
  const [budget, setBudget] = useState<number>(100000);
  const [maxAssets, setMaxAssets] = useState<number>(10); // заглушка: позже на бек
  const [sectors, setSectors] = useState<string[]>([]); // заглушка: пока пусто = "все"
  const [exclude, setExclude] = useState<string>(""); // заглушка строкой, позже UI

  // результат
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [resp, setResp] = useState<BuildResp | null>(null);

  // Telegram init
  useEffect(() => {
    const tg = (window as any)?.Telegram?.WebApp;
    tg?.ready?.();
    tg?.expand?.();

    const user: TgUser = tg?.initDataUnsafe?.user ?? null;
    if (user?.id) setTgId(user.id);
  }, []);

  const payload: BuildReq | null = useMemo(() => {
    if (!tgId) return null;
    return {
      telegram_id: tgId ?? 0,
      risk_profile: risk, // если хочешь “авто” — передавай null и пусть бек решает
      cov_method: "ledoit",
      lookback: 252,
      constraints: {
        sectors_include: sectors, // пусто = все (договоримся так)
        exclude_secids: exclude
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
        budget_rub: budget,
        max_assets: maxAssets, // пока бек игнорит
        max_weight: 0.15,
      },
    };
  }, [tgId, risk, sectors, exclude, budget, maxAssets]);

  async function build() {
    setErr(null);
    setResp(null);

    const effectiveTgId = tgId ?? 0;
    

    setLoading(true);
    try {
      const r = await fetch("/api/portfolio/build", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!r.ok) {
        const t = await r.text();
        throw new Error(`HTTP ${r.status}: ${t.slice(0, 800)}`);
      }

      const data = (await r.json()) as BuildResp;
      setResp(data);
    } catch (e: any) {
      setErr(e?.message ?? String(e));
    } finally {
      setLoading(false);
    }
  }

  const portfolios = resp?.portfolios ?? [];
  const sharpe = portfolios.find((p) => p.method === "max_sharpe") ?? null;
  const maxRet = portfolios.find((p) => p.method === "max_return") ?? null;

  return (
    <main style={{ padding: 16, maxWidth: 900, margin: "0 auto" }}>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 12 }}>
        FinCompass — портфель
      </h1>

{/*      <div style={{ marginBottom: 16, opacity: 0.8 }}>
        telegram_id: <b>{tgId ?? "—"}</b>
      </div>*/}

      {/* Экран параметров */}
      <section style={{ border: "1px solid #eee", borderRadius: 12, padding: 12, marginBottom: 16 }}>
        <h2 style={{ fontSize: 16, fontWeight: 600, marginBottom: 12 }}>Параметры</h2>

        <div style={{ display: "grid", gap: 12 }}>
          <label style={{ display: "grid", gap: 6 }}>
            Risk profile
            <select
              value={risk}
              onChange={(e) => setRisk(e.target.value)}
              style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }}
            >
              {RISK_OPTIONS.map((x) => (
                <option key={x} value={x}>{x}</option>
              ))}
            </select>
          </label>

          <label style={{ display: "grid", gap: 6 }}>
            Budget (RUB)
            <input
              type="number"
              value={budget}
              onChange={(e) => setBudget(Number(e.target.value || 0))}
              style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }}
            />
          </label>

          <label style={{ display: "grid", gap: 6 }}>
            Максимальное число активов (заглушка)
            <input
              type="range"
              min={5}
              max={50}
              value={maxAssets}
              onChange={(e) => setMaxAssets(Number(e.target.value))}
            />
            <div style={{ opacity: 0.8 }}>{maxAssets}</div>
          </label>

          <label style={{ display: "grid", gap: 6 }}>
            Сектора (заглушка: пусто = все)
            <input
              placeholder="позже будет мультиселект"
              value={sectors.join(",")}
              onChange={(e) =>
                setSectors(
                  e.target.value
                    .split(",")
                    .map((s) => s.trim())
                    .filter(Boolean)
                )
              }
              style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }}
            />
          </label>

          <label style={{ display: "grid", gap: 6 }}>
            Исключить инструменты (secid через запятую) — заглушка
            <input
              placeholder="SBER,GAZP,..."
              value={exclude}
              onChange={(e) => setExclude(e.target.value)}
              style={{ padding: 10, borderRadius: 10, border: "1px solid #ddd" }}
            />
          </label>

          <button
            onClick={build}
            disabled={loading}
            style={{
              padding: 12,
              borderRadius: 12,
              border: "1px solid #111",
              background: loading ? "#ddd" : "#111",
              color: "#fff",
              fontWeight: 600,
              cursor: loading ? "default" : "pointer",
            }}
          >
            {loading ? "Собираю…" : "Собрать портфель"}
          </button>

          {err && (
            <pre style={{ whiteSpace: "pre-wrap", color: "#b00020" }}>
              {err}
            </pre>
          )}
        </div>
      </section>

      {/* Экран результата */}
      {resp && (
        <section style={{ display: "grid", gap: 12 }}>
          <h2 style={{ fontSize: 16, fontWeight: 600 }}>Результат</h2>

          <PortfolioCard title="max_sharpe" p={sharpe} />
          <PortfolioCard title="max_return" p={maxRet} />

          <button
            onClick={build}
            disabled={loading}
            style={{
              padding: 12,
              borderRadius: 12,
              border: "1px solid #111",
              background: "#fff",
              color: "#111",
              fontWeight: 600,
            }}
          >
            Ребалансировка
          </button>
        </section>
      )}
    </main>
  );
}

function PortfolioCard({ title, p }: { title: string; p: PortfolioResult | null }) {
  if (!p) {
    return (
      <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
        <b>{title}</b>
        <div style={{ opacity: 0.8 }}>нет данных</div>
      </div>
    );
  }

  const top = Object.entries(p.weights || {})
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15);

  return (
    <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
        <b>{p.method}</b>
        <span style={{ opacity: 0.7 }}>
          as_of: {p.as_of} • risk: {p.risk_profile}
        </span>
      </div>

      {p.chart_png_b64 ? (
        <img
          src={`data:image/png;base64,${p.chart_png_b64}`}
          alt={p.method}
          style={{ width: "100%", marginTop: 10, borderRadius: 10 }}
        />
      ) : null}

      <div style={{ marginTop: 10 }}>
        <div style={{ fontWeight: 600, marginBottom: 6 }}>Top weights</div>
        <div style={{ display: "grid", gap: 4 }}>
          {top.map(([k, v]) => (
            <div key={k} style={{ display: "flex", justifyContent: "space-between" }}>
              <span>{k}</span>
              <span>{(v * 100).toFixed(2)}%</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
