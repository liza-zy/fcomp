# services/risk_quiz/runtime/service.py

from pathlib import Path
from typing import Dict, Any, Tuple

import yaml


CONFIG_PATH = Path(__file__).resolve().parent.parent / "domain" / "questions.yaml"


with CONFIG_PATH.open("r", encoding="utf-8") as f:
    _CONFIG = yaml.safe_load(f)

_PROFILES: Dict[str, Dict[str, Any]] = _CONFIG["profiles"]
_SCORE_RANGES: Dict[str, Dict[str, float]] = _CONFIG["score_ranges"]
_QUESTIONS = _CONFIG["questions"]


# Подготовим порядок профилей по index
_PROFILE_ORDER = sorted(
    [(name, data) for name, data in _PROFILES.items()],
    key=lambda item: item[1]["index"],
)
_PROFILE_NAMES_IN_ORDER = [name for name, _ in _PROFILE_ORDER]
_PROFILE_INDEX_BY_NAME = {name: data["index"] for name, data in _PROFILE_ORDER}


def _calc_total_score(answers: Dict[str, str]) -> float:
    """
    answers: словарь вида {"horizon": "horizon_2", "goal": "goal_3", ...}
    Если ответ по вопросу отсутствует или неизвестен — используем минимальный score
    по этому вопросу (консервативный fallback).
    """
    total = 0.0

    for q in _QUESTIONS:
        q_id = q["id"]
        weight = float(q.get("weight", 1.0))
        options = q["options"]
        options_by_code = {opt["code"]: opt for opt in options}

        answer_code = answers.get(q_id)

        if answer_code in options_by_code:
            score_value = float(options_by_code[answer_code].get("score", 0.0))
        else:
            # Не ответил или неизвестный код — минимальный риск по вопросу
            score_value = min(float(opt.get("score", 0.0)) for opt in options)

        total += score_value * weight

    return total


def _find_main_class(total_score: float) -> Tuple[str, float, float]:
    """
    Возвращает (имя класса, min, max) для основного профиля.
    Если score выходит за диапазоны — прижимаем к краям.
    """
    chosen_name = None
    chosen_min = None
    chosen_max = None

    # Пытаемся найти подходящий диапазон
    for name, bounds in _SCORE_RANGES.items():
        b_min = float(bounds["min"])
        b_max = float(bounds["max"])
        if b_min <= total_score <= b_max:
            chosen_name = name
            chosen_min = b_min
            chosen_max = b_max
            break

    if chosen_name is not None:
        return chosen_name, chosen_min, chosen_max

    # Если не попали ни в один диапазон — прижимаем к ближайшему
    items = sorted(_SCORE_RANGES.items(), key=lambda item: item[1]["min"])
    first_name, first_bounds = items[0]
    last_name, last_bounds = items[-1]

    if total_score < float(first_bounds["min"]):
        return first_name, float(first_bounds["min"]), float(first_bounds["max"])
    else:
        return last_name, float(last_bounds["min"]), float(last_bounds["max"])


def _compute_confidences(
    main_class: str,
    segment_min: float,
    segment_max: float,
    total_score: float,
) -> Tuple[float, str, float]:
    """
    Возвращает:
      main_confidence, neighbor_class (или None), neighbor_confidence
    main_confidence в [0.55, 0.95], neighbor = 1 - main.
    """
    center = (segment_min + segment_max) / 2.0
    half_width = max((segment_max - segment_min) / 2.0, 1e-6)
    distance_to_center = abs(total_score - center)

    # 1) сырая уверенность: 1 в центре, 0 на границах
    raw = 1.0 - distance_to_center / half_width
    raw = max(0.0, min(raw, 1.0))

    # 2) сжимаем в диапазон [0.55, 0.95]
    main_confidence = 0.55 + 0.4 * raw
    main_confidence = max(0.55, min(main_confidence, 0.95))

    # 3) соседний класс
    idx = _PROFILE_INDEX_BY_NAME[main_class]
    neighbor_name = None

    if total_score < center and idx > 0:
        neighbor_name = _PROFILE_NAMES_IN_ORDER[idx - 1]
    elif total_score > center and idx < len(_PROFILE_NAMES_IN_ORDER) - 1:
        neighbor_name = _PROFILE_NAMES_IN_ORDER[idx + 1]

    neighbor_confidence = 0.0
    if neighbor_name is not None:
        neighbor_confidence = max(0.0, 1.0 - main_confidence)

    return (
        round(main_confidence, 2),
        neighbor_name,
        round(neighbor_confidence, 2),
    )


def score(answers: Dict[str, str]) -> Dict[str, Any]:
    """
    Stateless-функция скоринга.

    answers:
      {
        "horizon": "horizon_2",
        "income_stability": "income_3",
        ...
      }
    """
    total_score = _calc_total_score(answers)
    main_class, seg_min, seg_max = _find_main_class(total_score)
    main_conf, neighbor_name, neighbor_conf = _compute_confidences(
        main_class, seg_min, seg_max, total_score
    )

    profile_info = _PROFILES.get(main_class, {})
    profile_text = profile_info.get("text")

    return {
        "risk_class": main_class,
        "confidence": main_conf,
        "neighbor_class": neighbor_name,
        "neighbor_confidence": neighbor_conf,
        "score": round(total_score, 2),
        "profile_text": profile_text,
    }