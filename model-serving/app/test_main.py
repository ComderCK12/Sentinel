"""Tests for the /score endpoint, run against the real exported ONNX
model (model-training/artifacts/model.onnx) — not a mock. The point of
these specific cases is catching a silent feature-ordering bug: if
FEATURE_ORDER in main.py ever drifts from train.py's FEATURE_COLUMNS,
predictions would still run without error, just on the wrong feature for
the wrong input — these assertions would be the thing that actually
catches that, not a passing build.
"""

import os

import pytest
from fastapi.testclient import TestClient

from app.main import app

MODEL_PATH = os.getenv("MODEL_PATH", "../model-training/artifacts/model.onnx")


@pytest.fixture
def client():
    # The app's startup event loads the model and fails hard if it's
    # missing (see main.py's load_model) — correct behavior for a real
    # deployment, but it means every test using this fixture needs a real
    # model present, not just the ones that call /score. Skip the whole
    # fixture, not individual tests, so that's consistent.
    if not os.path.exists(MODEL_PATH):
        pytest.skip(f"no trained model at {MODEL_PATH} — run train.py and convert.py first")
    with TestClient(app) as c:
        yield c


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "service": "model-serving"}


def test_score_normal_shaped_event_is_low_risk(client):
    resp = client.post(
        "/score",
        json={
            "amount": 100,
            "velocity_count": 1,
            "amount_ewma": 100,
            "has_amount_baseline": True,
            "amount_deviation_ratio": 0.0,
            "time_since_last_seconds": 21600,  # 6h — ordinary gap
            "has_time_since_last": True,
            "distance_from_last_km": 2.0,
            "has_distance": True,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_fraud"] is False
    assert body["risk_score"] < 0.1


def test_score_geo_jump_shaped_event_is_high_risk(client):
    resp = client.post(
        "/score",
        json={
            "amount": 100,
            "velocity_count": 1,
            "amount_ewma": 100,
            "has_amount_baseline": True,
            "amount_deviation_ratio": 0.0,
            "time_since_last_seconds": 300,  # 5 minutes
            "has_time_since_last": True,
            "distance_from_last_km": 5000.0,  # implausible for 5 minutes
            "has_distance": True,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_fraud"] is True
    assert body["risk_score"] > 0.9


def test_score_amount_spike_shaped_event_is_high_risk(client):
    resp = client.post(
        "/score",
        json={
            "amount": 2000,
            "velocity_count": 1,
            "amount_ewma": 100,
            "has_amount_baseline": True,
            "amount_deviation_ratio": 19.0,  # 2000 vs a baseline of 100
            "time_since_last_seconds": 21600,
            "has_time_since_last": True,
            "distance_from_last_km": 2.0,
            "has_distance": True,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_fraud"] is True
    assert body["risk_score"] > 0.9


def test_score_velocity_burst_shaped_event_is_high_risk(client):
    resp = client.post(
        "/score",
        json={
            "amount": 100,
            "velocity_count": 6,
            "amount_ewma": 100,
            "has_amount_baseline": True,
            "amount_deviation_ratio": 0.0,
            "time_since_last_seconds": 15,  # seconds, not hours
            "has_time_since_last": True,
            "distance_from_last_km": 0.1,
            "has_distance": True,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_fraud"] is True
    assert body["risk_score"] > 0.9


def test_score_missing_field_is_rejected(client):
    resp = client.post("/score", json={"amount": 100})
    assert resp.status_code == 422
