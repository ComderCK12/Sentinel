# Build plan

Phase-by-phase plan for Sentinel. Each phase-level checkbox in the README
rolls up the tasks listed here. Status markers: ✅ done, 🔵 in progress,
⬜ not started.

---

## Phase 0 — Foundations ✅ Done
- Monorepo folders, `go.work`
- Docker Compose (Redpanda, Postgres, Redis)
- Health-check endpoint per service
- CI skeleton (GitHub Actions, per-module build/lint)

## Phase 1 — Core pipeline plumbing ✅ Done
- Ingestion endpoint: validate schema, publish to Kafka partitioned by `user_id`
- Decision service: consume from Kafka, apply dummy rule, write to Postgres (idempotent via `ON CONFLICT`)
- Idempotency at ingestion: Redis `SETNX` dedup before publish, with fail-open on Redis outage

## Phase 2 — Feature engineering & data layer 🔵 In progress
- **Task 1** *(current)*: rolling per-user features — velocity, deviation from historical average, time-since-last, geo-distance
- **Task 2**: store features in Redis, keyed by user ID, with TTL — the online feature store
- **Task 3**: offline batch job recomputing the same features from Postgres history — the training data source
- **Task 4**: document where online vs offline features diverge

## Phase 3 — ML model training & serving ⬜ Not started
- Source/generate labeled dataset
- Train gradient-boosted model (XGBoost/LightGBM) on Phase 2 features
- Export to ONNX
- Stand up model-serving endpoint (FastAPI)

## Phase 4 — Wire model into decision service ⬜ Not started
- Decision service calls model-serving with the feature vector
- Combine model score with hard rule overrides
- Deliberate fail-open/fail-closed behavior on model-serving failure
- Audit log: score, rules applied, verdict, per-stage latency

## Phase 5 — Containerize & deploy to Kubernetes ⬜ Not started
- Dockerfiles per service, multi-stage builds
- K8s manifests/Helm: deployment, service, resource limits, probes
- HPA on Kafka consumer lag, not just CPU
- Verify locally with kind/minikube first

## Phase 6 — Infrastructure as code ⬜ Not started
- Terraform: managed K8s, Postgres, Redis, Kafka/MSK
- Remote state backend
- Full destroy/rebuild reproducibility

## Phase 7 — Observability ⬜ Not started
- Prometheus metrics: latency, Kafka lag, inference latency, throughput, error rates
- Grafana dashboards
- Structured logs with correlation ID across an event's full path
- Alerting rules

## Phase 8 — Load testing & chaos ⬜ Not started
- Load generator, increasing throughput until bottleneck found
- Chaos: kill a pod, kill a broker, inject latency into model-serving
- Document recovery time and affected decisions

## Phase 9 — Documentation & polish ⬜ Not started
- Design doc: problem statement, alternatives, trade-offs, capacity estimate
- Architecture diagram
- README with a working quickstart
- Demo video/GIF
