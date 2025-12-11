# tests/unit/test_risk_quiz_scoring.py

from services.risk_quiz.runtime.service import score


def test_ultra_conservative_profile():
    """
    Все максимально консервативные ответы → Ultra-Conservative.
    total_score примерно 0–6.
    """
    answers = {
        "horizon": "horizon_0",
        "income_stability": "income_0",
        "capital_dependency": "depend_0",
        "drawdown_behaviour": "drawdown_0",
        "comfort_volatility": "comfort_0",
        "goal": "goal_0",
        "payoff_expectations": "payoff_0",
        "fears": "fear_0",
    }

    result = score(answers)
    assert result["risk_class"] == "Ultra-Conservative"
    assert 0 <= result["score"] <= 6
    assert 0.55 <= result["confidence"] <= 0.95


def test_aggressive_profile():
    """
    Все максимально агрессивные ответы → Aggressive.
    total_score близко к максимуму (27–32).
    """
    answers = {
        "horizon": "horizon_4",
        "income_stability": "income_4",
        "capital_dependency": "depend_4",
        "drawdown_behaviour": "drawdown_4",
        "comfort_volatility": "comfort_4",
        "goal": "goal_4",
        "payoff_expectations": "payoff_4",
        "fears": "fear_4",
    }

    result = score(answers)
    assert result["risk_class"] == "Aggressive"
    assert 27 <= result["score"] <= 32
    assert 0.55 <= result["confidence"] <= 0.95


def test_balanced_middle_case():
    """
    Смешанный профиль должен попадать в Balanced.
    """
    answers = {
        "horizon": "horizon_2",            # 2
        "income_stability": "income_2",    # 2
        "capital_dependency": "depend_2",  # 2
        "drawdown_behaviour": "drawdown_2",# 2
        "comfort_volatility": "comfort_2", # 2
        "goal": "goal_2",                  # 2
        "payoff_expectations": "payoff_2", # 2
        "fears": "fear_2",                 # 2
    }
    # total_score ≈ 16 → Balanced (14–20)
    result = score(answers)
    assert result["risk_class"] == "Balanced"
    assert 14 <= result["score"] <= 20


def test_boundary_between_conservative_and_balanced():
    """
    Пороговый кейс на границе Conservative / Balanced.
    Например, score ≈ 13 → Conservative, 14 → Balanced.
    """
    # соберём ответы ближе к верхней границе Conservative
    answers_conservative = {
        "horizon": "horizon_2",            # 2
        "income_stability": "income_2",    # 2
        "capital_dependency": "depend_1",  # 1
        "drawdown_behaviour": "drawdown_1",# 1
        "comfort_volatility": "comfort_2", # 2
        "goal": "goal_2",                  # 2
        "payoff_expectations": "payoff_2", # 2
        "fears": "fear_1",                 # 1
    }
    res_cons = score(answers_conservative)
    assert res_cons["risk_class"] in ("Conservative", "Balanced")
    assert 7 <= res_cons["score"] <= 20

    # чуть более "смелый" набор
    answers_balanced = {
        "horizon": "horizon_3",            # 3
        "income_stability": "income_3",    # 3
        "capital_dependency": "depend_2",  # 2
        "drawdown_behaviour": "drawdown_2",# 2
        "comfort_volatility": "comfort_2", # 2
        "goal": "goal_2",                  # 2
        "payoff_expectations": "payoff_2", # 2
        "fears": "fear_2",                 # 2
    }
    res_bal = score(answers_balanced)
    assert res_bal["risk_class"] in ("Balanced", "Growth")