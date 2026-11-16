"""Toy Macaulay duration model with optional MLflow tracking."""

from __future__ import annotations

import random


def macaulay_duration(coupon: float, yld: float, tenor_years: float, freq: int = 2) -> float:
    """Annual Macaulay duration for a bullet bond; coupon and yld are decimals."""
    n = int(tenor_years * freq)
    if n <= 0:
        return 0.0
    c = coupon / freq
    r = yld / freq
    price = 0.0
    weighted = 0.0
    for t in range(1, n + 1):
        cf = c * 100.0
        if t == n:
            cf += 100.0
        df = (1.0 + r) ** t
        pv = cf / df
        price += pv
        weighted += (t / freq) * pv
    return weighted / price if price else 0.0


def synthetic_training_rows(n: int = 40, seed: int = 42) -> list[dict[str, float]]:
    rng = random.Random(seed)
    rows: list[dict[str, float]] = []
    for _ in range(n):
        coupon = rng.uniform(0.01, 0.06)
        yld = rng.uniform(0.02, 0.055)
        tenor = rng.choice([2.0, 5.0, 10.0, 30.0])
        dur = macaulay_duration(coupon, yld, tenor)
        rows.append({"coupon": coupon, "yield": yld, "tenor": tenor, "duration": dur})
    return rows


def fit_linear(rows: list[dict[str, float]]) -> dict[str, float]:
    """Duration ~ a + b*coupon + c*yield + d*tenor (normal equations, 4 params)."""
    n = len(rows)
    xtx = [[0.0] * 4 for _ in range(4)]
    xty = [0.0] * 4
    for row in rows:
        x = [1.0, row["coupon"], row["yield"], row["tenor"]]
        y = row["duration"]
        for i in range(4):
            xty[i] += x[i] * y
            for j in range(4):
                xtx[i][j] += x[i] * x[j]
    # Gaussian elimination
    a = [row[:] + [rhs] for row, rhs in zip(xtx, xty)]
    for i in range(4):
        pivot = a[i][i] if a[i][i] else 1e-12
        for j in range(i, 5):
            a[i][j] /= pivot
        for k in range(4):
            if k == i:
                continue
            factor = a[k][i]
            for j in range(i, 5):
                a[k][j] -= factor * a[i][j]
    coef = [a[i][4] for i in range(4)]
    sse = 0.0
    sst = 0.0
    mean_y = sum(r["duration"] for r in rows) / n
    for row in rows:
        pred = coef[0] + coef[1] * row["coupon"] + coef[2] * row["yield"] + coef[3] * row["tenor"]
        sse += (pred - row["duration"]) ** 2
        sst += (row["duration"] - mean_y) ** 2
    r2 = 1.0 - sse / sst if sst else 0.0
    return {"intercept": coef[0], "b_coupon": coef[1], "b_yield": coef[2], "b_tenor": coef[3], "r2": r2, "n": float(n)}


def log_mlflow(metrics: dict[str, float]) -> None:
    try:
        import mlflow
    except ImportError:
        print("mlflow not installed; metrics=", metrics)
        return
    with mlflow.start_run(run_name="fi-duration-toy"):
        mlflow.log_params({"model": "linear_macaulay_features", "n": int(metrics["n"])})
        mlflow.log_metrics({k: v for k, v in metrics.items() if k != "n"})


def main() -> dict[str, float]:
    metrics = fit_linear(synthetic_training_rows())
    log_mlflow(metrics)
    print(metrics)
    return metrics


if __name__ == "__main__":
    main()
