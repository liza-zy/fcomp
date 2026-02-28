from __future__ import annotations
import base64
import io
import matplotlib.pyplot as plt

def weights_pie_b64(weights: dict[str, float], title: str) -> str:
    labels = list(weights.keys())
    values = list(weights.values())

    fig = plt.figure(figsize=(6, 6))
    plt.pie(values, labels=None, autopct="%1.1f%%")
    plt.title(title)
    plt.legend(labels, loc="center left", bbox_to_anchor=(1.0, 0.5))

    buf = io.BytesIO()
    plt.tight_layout()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)

    return base64.b64encode(buf.getvalue()).decode("ascii")