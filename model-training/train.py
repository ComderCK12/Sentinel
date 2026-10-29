"""Train an XGBoost classifier on the synthetic labeled dataset.

The dataset comes from `stream-processor/cmd/synth-dataset` (phase 3
task 1), not from real decision history — see that command's package
doc for why. `is_fraud` is a label we control, deliberately driven by
velocity/amount-deviation/geo-distance patterns, not by amount alone.

`fraud_pattern` is excluded from the feature set on purpose: it names
which fraud pattern produced a row, which only exists because we
generated the row — including it would hand the model the answer
instead of the behavioral signal that's supposed to reveal it.
"""

import argparse
import json

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

FEATURE_COLUMNS = [
    "amount",
    "velocity_count",
    "amount_ewma",
    "has_amount_baseline",
    "amount_deviation_ratio",
    "time_since_last_seconds",
    "has_time_since_last",
    "distance_from_last_km",
    "has_distance",
]
TARGET_COLUMN = "is_fraud"

BOOL_COLUMNS = [
    "has_amount_baseline",
    "has_time_since_last",
    "has_distance",
    "is_fraud",
]


def load_dataset(path: str) -> pd.DataFrame:
    # pandas' CSV reader already infers true/false columns as real bool
    # dtype on its own — just cast those to int for XGBoost.
    df = pd.read_csv(path)
    for col in BOOL_COLUMNS:
        df[col] = df[col].astype(int)
    return df


def train(args: argparse.Namespace) -> None:
    df = load_dataset(args.data)
    print(f"loaded {len(df)} rows ({df[TARGET_COLUMN].sum()} fraud, "
          f"{len(df) - df[TARGET_COLUMN].sum()} normal)")

    x = df[FEATURE_COLUMNS]
    y = df[TARGET_COLUMN]

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=args.test_size, random_state=args.seed, stratify=y
    )
    print(f"train: {len(x_train)} rows ({y_train.sum()} fraud) | "
          f"test: {len(x_test)} rows ({y_test.sum()} fraud)")

    # Fraud is ~1-2% of rows here — scale_pos_weight tells XGBoost to
    # weight a missed fraud row roughly as costly as (negatives/positives)
    # missed normal rows, instead of optimizing for an accuracy number
    # that "always predict normal" would already satisfy.
    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.1,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",
        random_state=args.seed,
    )
    model.fit(x_train, y_train)

    y_pred = model.predict(x_test)
    y_proba = model.predict_proba(x_test)[:, 1]

    print("\n=== classification report ===")
    print(classification_report(y_test, y_pred, target_names=["normal", "fraud"]))

    print("=== confusion matrix (rows=actual, cols=predicted) ===")
    print(confusion_matrix(y_test, y_pred))

    print(f"\nROC-AUC:        {roc_auc_score(y_test, y_proba):.4f}")
    print(f"PR-AUC (avg P): {average_precision_score(y_test, y_proba):.4f}")

    print("\n=== feature importances ===")
    importances = sorted(
        zip(FEATURE_COLUMNS, model.feature_importances_), key=lambda kv: -kv[1]
    )
    for name, score in importances:
        print(f"  {name:<28} {score:.4f}")

    model.save_model(args.model_out)
    print(f"\nsaved model to {args.model_out}")

    metrics = {
        "roc_auc": float(roc_auc_score(y_test, y_proba)),
        "pr_auc": float(average_precision_score(y_test, y_proba)),
        "feature_importances": {name: float(score) for name, score in importances},
        "train_rows": int(len(x_train)),
        "test_rows": int(len(x_test)),
        "test_fraud_rows": int(y_test.sum()),
    }
    with open(args.metrics_out, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"saved metrics to {args.metrics_out}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default="data/synthetic_dataset.csv")
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--model-out", default="artifacts/model.json")
    parser.add_argument("--metrics-out", default="artifacts/metrics.json")
    args = parser.parse_args()

    np.random.seed(args.seed)
    train(args)


if __name__ == "__main__":
    main()
