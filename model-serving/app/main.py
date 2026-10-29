import os
from contextlib import asynccontextmanager

import numpy as np
import onnxruntime as ort
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SERVICE_NAME = "model-serving"

# Must match train.py's FEATURE_COLUMNS exactly, in exactly this order —
# that's what the ONNX model was trained on. This can't be a shared
# import the way Go's internal packages are, since model-training and
# model-serving are separate Python processes with no package
# relationship, so this list exists purely by convention. Changing the
# feature set on the training side requires changing this list the same
# way, in the same order, or every prediction silently uses the wrong
# feature for the wrong input.
FEATURE_ORDER = [
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


class FeatureVector(BaseModel):
    amount: float
    velocity_count: int
    amount_ewma: float
    has_amount_baseline: bool
    amount_deviation_ratio: float
    time_since_last_seconds: float
    has_time_since_last: bool
    distance_from_last_km: float
    has_distance: bool

    def to_array(self) -> np.ndarray:
        values = {
            "amount": self.amount,
            "velocity_count": self.velocity_count,
            "amount_ewma": self.amount_ewma,
            "has_amount_baseline": float(self.has_amount_baseline),
            "amount_deviation_ratio": self.amount_deviation_ratio,
            "time_since_last_seconds": self.time_since_last_seconds,
            "has_time_since_last": float(self.has_time_since_last),
            "distance_from_last_km": self.distance_from_last_km,
            "has_distance": float(self.has_distance),
        }
        return np.array([[values[name] for name in FEATURE_ORDER]], dtype=np.float32)


class ScoreResponse(BaseModel):
    risk_score: float
    is_fraud: bool


class ModelNotLoadedError(RuntimeError):
    pass


_session: ort.InferenceSession | None = None
_input_name: str | None = None


def load_model() -> None:
    global _session, _input_name
    model_path = os.getenv("MODEL_PATH", "../model-training/artifacts/model.onnx")
    if not os.path.exists(model_path):
        raise ModelNotLoadedError(
            f"MODEL_PATH={model_path!r} does not exist — train and export a "
            "model first (see model-training/train.py and convert.py)"
        )
    _session = ort.InferenceSession(model_path)
    _input_name = _session.get_inputs()[0].name


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Fail fast: a model-serving instance that can't load its model can't
    # do the one thing it exists for, same reasoning decision-service
    # uses for failing to start without a Postgres connection.
    load_model()
    yield


app = FastAPI(title="Sentinel Model Serving", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": SERVICE_NAME}


@app.post("/score", response_model=ScoreResponse)
def score(features: FeatureVector) -> ScoreResponse:
    if _session is None or _input_name is None:
        raise HTTPException(status_code=503, detail="model not loaded")

    x = features.to_array()
    outputs = _session.run(None, {_input_name: x})

    # onnxmltools' XGBoost classifier export returns [label, probabilities]
    # — probabilities is either a (1, 2) array or a list of one {label:
    # prob} dict, depending on onnxmltools' ZipMap setting (see
    # model-training/convert.py's check_parity for the same handling).
    proba_out = outputs[1]
    if isinstance(proba_out, list) and isinstance(proba_out[0], dict):
        risk_score = float(proba_out[0][1])
    else:
        risk_score = float(np.asarray(proba_out)[0][1])

    return ScoreResponse(risk_score=risk_score, is_fraud=risk_score >= 0.5)
