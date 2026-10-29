"""Export the trained XGBoost model (phase 3 task 2) to ONNX.

Converting isn't enough on its own to trust the result — ONNX converters
can silently diverge from the source model on edge cases (output
structure, probability rounding, categorical handling). This script runs
a parity check: the same sampled rows through both the native XGBoost
model and the exported ONNX model via onnxruntime, comparing predicted
labels and probabilities before writing the export off as done.
"""

import argparse

import numpy as np
import onnxruntime as ort
import pandas as pd
import xgboost as xgb
from onnxmltools.convert import convert_xgboost
from onnxmltools.convert.common.data_types import FloatTensorType

from train import FEATURE_COLUMNS, load_dataset


def convert_to_onnx(model_path: str, onnx_out: str) -> xgb.XGBClassifier:
    model = xgb.XGBClassifier()
    model.load_model(model_path)

    # train.py fits on a pandas DataFrame, so the booster's tree dump has
    # the real column names (e.g. "time_since_last_seconds") baked in.
    # onnxmltools' converter only understands XGBoost's default "f0, f1,
    # ..." naming and fails outright on anything else — reset to that
    # before converting. FEATURE_COLUMNS' order is what both train.py and
    # this conversion agree on, so f0..fN here still means the same
    # feature it meant during training.
    model.get_booster().feature_names = [f"f{i}" for i in range(len(FEATURE_COLUMNS))]

    initial_types = [("input", FloatTensorType([None, len(FEATURE_COLUMNS)]))]
    onnx_model = convert_xgboost(model, initial_types=initial_types)

    with open(onnx_out, "wb") as f:
        f.write(onnx_model.SerializeToString())

    return model


def check_parity(model: xgb.XGBClassifier, onnx_path: str, x_sample: np.ndarray):
    sess = ort.InferenceSession(onnx_path)
    input_name = sess.get_inputs()[0].name
    output_names = [o.name for o in sess.get_outputs()]

    native_proba = model.predict_proba(x_sample)[:, 1]
    native_pred = model.predict(x_sample)

    onnx_outputs = sess.run(None, {input_name: x_sample.astype(np.float32)})
    onnx_pred = np.asarray(onnx_outputs[0])

    # The probability output's exact shape depends on onnxmltools' ZipMap
    # setting — handle both a plain (N, 2) array and a list of {label:
    # prob} dicts rather than assuming one.
    proba_out = onnx_outputs[1]
    if isinstance(proba_out, list) and isinstance(proba_out[0], dict):
        onnx_proba = np.array([row[1] for row in proba_out])
    else:
        onnx_proba = np.asarray(proba_out)[:, 1]

    print(f"onnx outputs: {output_names}")
    label_match = bool((native_pred == onnx_pred).all())
    max_proba_diff = float(np.abs(native_proba - onnx_proba).max())
    return label_match, max_proba_diff


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default="artifacts/model.json")
    parser.add_argument("--data", default="data/synthetic_dataset.csv")
    parser.add_argument("--onnx-out", default="artifacts/model.onnx")
    parser.add_argument("--sample-size", type=int, default=500)
    parser.add_argument("--max-proba-diff", type=float, default=1e-3)
    args = parser.parse_args()

    model = convert_to_onnx(args.model, args.onnx_out)
    print(f"exported ONNX model to {args.onnx_out}")

    df = load_dataset(args.data)
    # Fraud is ~1.7% of this dataset — a plain random sample could easily
    # undersample it, and fraud rows are exactly where a subtle tree
    # conversion discrepancy (deep paths, edge-case splits) would most
    # likely show up. Take every fraud row plus a random normal sample,
    # rather than leaving fraud coverage to chance.
    fraud_rows = df[df["is_fraud"] == 1]
    normal_sample = df[df["is_fraud"] == 0].sample(
        n=min(args.sample_size, len(df)), random_state=42
    )
    sample = pd.concat([fraud_rows, normal_sample])
    x_sample = sample[FEATURE_COLUMNS].to_numpy(dtype=np.float32)

    label_match, max_proba_diff = check_parity(model, args.onnx_out, x_sample)
    print(f"parity check on {len(x_sample)} rows "
          f"({len(fraud_rows)} fraud, all of them, plus {len(normal_sample)} random normal):")
    print(f"  predicted labels match: {label_match}")
    print(f"  max probability difference: {max_proba_diff:.8f}")

    if not label_match or max_proba_diff > args.max_proba_diff:
        raise SystemExit(
            "parity check FAILED - ONNX export does not match the native "
            "model closely enough to trust"
        )
    print("parity check passed")


if __name__ == "__main__":
    main()
