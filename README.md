# Sentinel

Real-time risk-decisioning platform. Events stream in, get scored for risk by an ML model in tens of milliseconds, and a decision service returns allow / flag / block — with a full audit trail of how every decision was made.

Built as a systems-design portfolio project: the point isn't just "a model that predicts fraud," it's the infrastructure around it — high-throughput ingestion, stream processing, low-latency ML serving, and the Kubernetes/Terraform/observability stack needed to run it reliably under load and failure.

## Why this exists

Most fraud/anomaly checks are either static rules (fast, but rigid) or batch-scored models (accurate, but too slow to matter). Sentinel scores every event in real time using computed behavioral features, while keeping deterministic rule overrides for cases that need guaranteed behavior. The interesting engineering problems here are the same ones real-time risk platforms deal with at scale: partitioning and ordering, online/offline feature consistency, graceful degradation when a dependency fails, and autoscaling on the metric that actually reflects load.

## Architecture

Three layers: **ingestion** (Go + Kafka) → **stream processing + ML serving** (feature computation, ONNX model) → **decision + storage** (Go, Postgres, Redis). Full breakdown in `docs/design-doc.md`.

## Tech stack

| Layer | Tech |
|---|---|
| Ingestion, stream processing, decisioning | Go |
| Model training & serving | Python, XGBoost/LightGBM, ONNX, FastAPI |
| Messaging | Kafka (Redpanda for local dev) |
| Storage | Postgres, Redis |
| Orchestration | Kubernetes |
| Infrastructure as code | Terraform |
| Observability | Prometheus, Grafana |

## Project status

- [x] Phase 0 — foundations & local dev environment
- [x] Phase 1 — core pipeline (ingestion → Kafka → decision, dummy rule)
- [ ] Phase 2 — real-time feature computation
- [ ] Phase 3 — ML model training & serving
- [ ] Phase 4 — model integration with fallback logic
- [ ] Phase 5 — Kubernetes deployment
- [ ] Phase 6 — infrastructure as code
- [ ] Phase 7 — observability
- [ ] Phase 8 — load & chaos testing
- [ ] Phase 9 — documentation & demo polish

## Getting started

\`\`\`bash
git clone https://github.com/<your-username>/sentinel.git
cd sentinel
docker compose up -d
make build
make test
\`\`\`

## Project structure

\`\`\`
sentinel/
├── ingestion/
├── stream-processor/
├── decision-service/
├── model-serving/
├── model-training/
├── loadgen/
├── infra/
│   ├── terraform/
│   └── k8s/
└── docs/
├── design-doc.md
└── build-plan.md
\`\`\`
