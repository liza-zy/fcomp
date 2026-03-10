from __future__ import annotations

import base64
import io
import matplotlib.pyplot as plt


def plot_weights_bar_b64(
    weights: dict[str, float],
    title: str,
    max_assets: int = 10,
) -> str:
    """
    Горизонтальный bar chart (читабельный).
    Возвращает base64 PNG.
    """

    items = sorted(weights.items(), key=lambda x: x[1], reverse=True)[:max_assets]
    if not items:
        return ""

    labels = [k for k, _ in items][::-1]
    values = [v for _, v in items][::-1]

    fig, ax = plt.subplots(figsize=(9, 6), dpi=160)

    cmap = plt.get_cmap("tab10")
    colors = [cmap(i % 10) for i in range(len(values))]

    ax.barh(labels, [v * 100 for v in values], color=colors)

    ax.set_xlabel("Weight, %")
    ax.set_title(title)

    ax.grid(True, axis="x", alpha=0.25)
    ax.set_xlim(0, max(5, max([v * 100 for v in values]) * 1.15))

    for i, v in enumerate(values):
        ax.text(v * 100 + 0.3, i, f"{v*100:.1f}%", va="center", fontsize=9)

    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=160)
    plt.close(fig)

    return base64.b64encode(buf.getvalue()).decode("ascii")


def weights_pie_b64(
    weights: dict[str, float],
    title: str,
    max_assets: int = 10,
) -> str:
    """
    Pie chart (если нужен альтернативный вариант)
    """

    items = sorted(weights.items(), key=lambda x: x[1], reverse=True)[:max_assets]
    if not items:
        return ""

    labels = [k for k, _ in items]
    values = [v for _, v in items]

    fig = plt.figure(figsize=(6, 6))
    cmap = plt.get_cmap("tab10")
    colors = [cmap(i % 10) for i in range(len(values))]

    plt.pie(
        values,
        labels=None,
        autopct="%1.1f%%",
        colors=colors,
    )
    plt.title(title)
    plt.legend(labels, loc="center left", bbox_to_anchor=(1.0, 0.5))

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)

    return base64.b64encode(buf.getvalue()).decode("ascii")
