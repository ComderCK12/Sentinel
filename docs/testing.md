# Testing log

Manual verification steps for each feature, as they're built. This is a running
log, not automated test code — every entry below was actually run against a
live local stack (`docker compose up -d` + the relevant services built from
source) and the results recorded are real, not expected/assumed.

**Convention for new entries:** one section per phase/task. Each test gets a
short title (what's being proven), the exact steps to reproduce it, and the
expected result. Append new sections at the bottom as the project progresses —
don't rewrite history above.

---

## Phase 0 — Foundations

### Test: services come up and report healthy

**Steps:**
```bash
docker compose up -d
curl -i localhost:8080/health   # ingestion
curl -i localhost:8083/health   # decision-service
```

**Expected:** `docker compose ps` shows all containers `healthy`; both health
endpoints return `200 OK` with a JSON body naming the service.

---

## Phase 1 Task 1 — Ingestion endpoint

### Test: valid event is accepted and published to Kafka

**Steps:**
```bash
curl -i -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"evt_100","user_id":"usr_100","amount":249.99,"currency":"USD"}'
```

**Expected:** `202 Accepted`, body `{"event_id":"evt_100","status":"accepted"}`.

### Test: invalid events are rejected, not silently dropped

**Steps:**
```bash
curl -i -X POST localhost:8080/v1/events -d '{}'                # missing event_id
curl -i -X POST localhost:8080/v1/events -d '{"event_id":"x"}'  # missing user_id
curl -i -X POST localhost:8080/v1/events -X GET                 # wrong method
```

**Expected:** first two return `400` with a field-specific error message; the
third returns `405`.

---

## Phase 1 Task 2 — Decision service

### Test: dummy rule — allow path

**Steps:** send an event under the $10,000 threshold (see Task 1 above), then:
```bash
docker exec -it sentinel-postgres psql -U sentinel -d sentinel \
  -c "select event_id, amount, decision, reason from decisions where event_id='evt_100';"
```

**Expected:** one row, `decision = 'allow'`, `reason = 'within dummy threshold'`.

### Test: dummy rule — block path

**Steps:**
```bash
curl -i -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"evt_101","user_id":"usr_100","amount":15000,"currency":"USD"}'

docker exec -it sentinel-postgres psql -U sentinel -d sentinel \
  -c "select decision, reason from decisions where event_id='evt_101';"
```

**Expected:** `decision = 'block'`, `reason = 'amount exceeds dummy threshold'`.

---

## Phase 1 Task 3 — Idempotency

### Test: single-event dedup (ingestion fast path)

**Steps:**
```bash
curl -i -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"verify_1","user_id":"u1","amount":100,"currency":"USD"}'
# resend the exact same body
curl -i -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"verify_1","user_id":"u1","amount":100,"currency":"USD"}'

docker exec -it sentinel-postgres psql -U sentinel -d sentinel \
  -c "select count(*) from decisions where event_id='verify_1';"
```

**Expected:** first call → `{"status":"accepted"}`; second call →
`{"status":"duplicate"}` (caught before Kafka, no re-publish); row count `1`.

### Test: idempotency proof at scale (1,000 events, ~10% duplicates)

**Steps:**
```bash
docker exec -it sentinel-postgres psql -U sentinel -d sentinel -c "select count(*) from decisions;"
# note this as baseline_N

go run ./loadgen -n 1000 -dup-rate 0.1 -concurrency 50
```

**Expected:** `loadgen` summary shows `accepted=900, duplicate=100, failed=0`;
Postgres count afterward is exactly `baseline_N + 900`.

**Actually run:** baseline 905 → after run, 1805 (delta 900, exact match).

### Test: fail-open when Redis is unreachable

Redis is a fast-path optimization here, not the source of truth — a slow or
down Redis should degrade latency by a bounded amount and still accept the
event, not hang or reject it.

> Note: this machine has two Redis processes claiming port 6379 — Docker
> compose's `redis` service, and a separate Homebrew `redis-server` already
> running locally. `localhost:6379` resolves to the **Homebrew one**; pause
> that process (find its PID via `lsof -nP -iTCP:6379 -sTCP:LISTEN`), not the
> Docker container.

**Steps:**
```bash
kill -STOP <redis-server-pid>
ps -o pid,state -p <redis-server-pid>   # confirm STAT is "T"

time curl -i -m 10 -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"verify_fail_open","user_id":"u1","amount":50,"currency":"USD"}'

grep verify_fail_open /tmp/ingestion.log
```

**Expected:** still `202 accepted`, returning in ~1-1.5s (not hanging for the
full 10s client timeout); log shows
`"idempotency check failed, publishing anyway" ... "error":"...i/o timeout"`.

**History:** first attempt at this test accidentally paused the *Docker*
redis container and found nothing wrong (false pass — wrong process). Once
correctly targeting the Homebrew process, the request took 6s to fail open,
bounded only by how long the caller was willing to wait — a
`context.WithTimeout` at the call site doesn't bound go-redis's underlying
socket I/O, which is governed by the client's own `DialTimeout`/`ReadTimeout`
(multi-second defaults) plus its retry count. Fixed by setting
`DialTimeout`/`ReadTimeout`/`WriteTimeout: 300ms` and `MaxRetries: -1` on the
Redis client itself (`ingestion/cmd/server/main.go`). Re-tested: now fails in
~300ms regardless of client patience.

### Test: Postgres backstop when the fast path can't dedupe

**Steps:** with Redis still paused from the previous test:
```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"verify_backstop","user_id":"u1","amount":75,"currency":"USD"}'
curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"verify_backstop","user_id":"u1","amount":75,"currency":"USD"}'

kill -CONT <redis-server-pid>   # resume redis — don't skip this
docker exec -it sentinel-postgres psql -U sentinel -d sentinel \
  -c "select count(*) from decisions where event_id='verify_backstop';"
```

**Expected:** both sends return `202` (Redis can't catch the duplicate this
time, both reach Kafka) — but the row count is still `1`. This is the actual
correctness guarantee: `decisions.event_id` is `PRIMARY KEY`, and
`store.SaveDecision` writes with `INSERT ... ON CONFLICT (event_id) DO
NOTHING`, so even total loss of the fast path can't produce a duplicate
decision.

### Test: a failed Kafka publish doesn't permanently swallow the event

Found by code review, not by the manual pass above — the original
`MarkIfNew` call claimed the Redis key *before* the Kafka publish was
confirmed, with nothing to undo that claim if the publish then failed. A
client retrying the same `event_id` (the standard response to a `500`) would
find the key already claimed and get told `"duplicate"` — silently dropping
an event that was never actually processed, for up to the 24h TTL. The
Postgres backstop above doesn't help here: it only prevents a duplicate
*decision* once an event reaches Kafka twice, and does nothing when an event
never reaches Kafka at all.

**Steps:**
```bash
docker stop sentinel-redpanda   # simulate Kafka being unreachable

curl -i -m 10 -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"release_test_1","user_id":"u1","amount":50,"currency":"USD"}'
# expect: 500 {"error":"failed to accept event"}

docker start sentinel-redpanda
# decision-service's Kafka connection doesn't survive the broker restart —
# restart it too before continuing:
#   pkill -f bin/decision-service && go build -o bin/decision-service ./decision-service/cmd/server
#   POSTGRES_DSN=... KAFKA_BROKERS=localhost:19092 ./bin/decision-service &

# retry the exact same event_id
curl -i -m 10 -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"release_test_1","user_id":"u1","amount":50,"currency":"USD"}'

docker exec -it sentinel-postgres psql -U sentinel -d sentinel \
  -c "select event_id, decision from decisions where event_id='release_test_1';"
```

**Expected:** the retry returns `202 {"status":"accepted"}` — **not**
`"duplicate"` — and a row for `release_test_1` shows up in Postgres.

**Fix:** added `idempotency.Checker.Release` (a plain `DEL` on the claimed
key) and wired it into the handler: if `producer.Publish` fails after a
successful claim, the claim is released before returning the error, so a
retry gets a real second attempt instead of a false "duplicate". Best-effort
— if the release call itself fails, the claim just lives out its TTL, which
costs a spurious duplicate response on retry (an availability hit), not
silent event loss (a correctness bug) like before the fix.

**Actually run:** confirmed `500` on the first send while Redpanda was
stopped, `202 accepted` (not `duplicate`) on the retry after it came back,
and the row present in Postgres with `decision = 'allow'`. Also re-ran the
full 1,000-event loadgen proof afterward to confirm no regression: baseline
2707 → 3607 after the run (delta 900, exact match), zero errors.

---

## Phase 2 Task 2 — Online feature store

### Note: two Redis processes on this machine

Carried over from the Task 1 fail-open test — this machine has both Docker
compose's `redis` service *and* a separate Homebrew `redis-server` bound to
port 6379. `localhost:6379` (what every service here connects to) resolves
to the **Homebrew one**, not the Docker container. So `docker exec
sentinel-redis redis-cli ...` inspects the wrong instance — use a plain
`redis-cli -p 6379 ...` from the host instead when checking state written
by these services.

### Test: feature state accumulates correctly across a user's events

**Steps:** with `stream-processor` running (`PORT=8084` — see port-conflict
note below) alongside `ingestion`, send a sequence of events for one user:

```bash
for i in 1 2 3; do
  curl -s -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
    -d "{\"event_id\":\"feat_verify_$i\",\"user_id\":\"u_feat_test\",\"amount\":100,\"currency\":\"USD\",\"location\":{\"lat\":12.34,\"lon\":56.78}}"
done

redis-cli -p 6379 GET sentinel:features:state:u_feat_test
redis-cli -p 6379 TTL sentinel:features:state:u_feat_test
```

**Expected:** each `stream-processor` log line shows `velocity_count`
incrementing (1, 2, 3); the Redis value's `RecentTimestamps` has 3 entries;
`TTL` is set (~24h).

**Actually run:** log lines showed `velocity_count: 1, 2, 3` across the
three events, `amount_deviation_ratio: 0` and `distance_from_last_km: 0`
throughout (same amount and location every time — correctly no deviation).
Redis held `{"RecentTimestamps":[...3 entries...],"MeanAmount":100,
"HasMean":true,"LastTimestamp":"...","HasLast":true,"LastLocation":
{"lat":12.34,"lon":56.78}}`, `TTL` = 86377s (~24h).

### Test: amount deviation, time-since-last, and geo-distance on a real change

**Steps:** send a 4th event for the same user with a different amount and
location:

```bash
curl -s -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"feat_verify_4","user_id":"u_feat_test","amount":500,"currency":"USD","location":{"lat":51.5074,"lon":-0.1278}}'

redis-cli -p 6379 GET sentinel:features:state:u_feat_test
redis-cli -p 6379 TTL sentinel:features:state:u_feat_test
```

**Expected:** `amount_deviation_ratio` reflects a 400% spike against the
prior 100 baseline (`(500-100)/100 = 4`); `MeanAmount` updates via EWMA to
`0.3*500 + 0.7*100 = 220`; `distance_from_last_km` is the real geodesic
distance between the two points; TTL refreshes back up to ~24h (sliding).

**Actually run:** log line showed `velocity_count: 4, amount_deviation_
ratio: 4, distance_from_last_km: 6677.216614226916` (arbitrary point →
London, real-world distance). Redis's `MeanAmount` updated to exactly
`220`, `LastLocation` updated to the London coordinates, `TTL` refreshed
to 86398s.

### Bug found while running this: default port collision

`stream-processor`'s `defaultPort` was `8081` — same port Redpanda's
schema registry is mapped to in `docker-compose.yml`
(`--schema-registry-addr=0.0.0.0:8081` + `"8081:8081"`). Running
`stream-processor` against the full compose stack failed immediately with
`listen tcp :8081: bind: address already in use`. This predates Task 2 —
it would have broken from Phase 0 onward the first time anyone actually
ran it locally, since nothing had exercised that path until now.
`8080`/`8083`/`8081`/`8082`/`8090` are all already taken (ingestion,
decision-service, and Redpanda's three ports respectively), so
`defaultPort` was changed to `8084`.

---

## Phase 2 Task 3 — Offline feature replay

### Test: replay reproduces live feature math exactly, against real history

**Steps:** with `ingestion` and `decision-service` running, send a short
sequence for two users — one with a flat run of same-amount events, one
with an amount spike that should trip the dummy block rule:

```bash
curl -s -X POST localhost:8080/v1/events -H "Content-Type: application/json" \
  -d '{"event_id":"r_u1_1","user_id":"replay_u1","amount":100,"currency":"USD"}'
# ... r_u1_2 (100), r_u1_3 (900) for replay_u1, ~2s apart
# ... r_u2_1 (50), r_u2_2 (20000) for replay_u2, ~2s apart

POSTGRES_DSN="postgres://sentinel:sentinel@localhost:5435/sentinel" \
  OUTPUT_PATH=/tmp/features_dataset.csv ./bin/offline-replay
```

**Expected:** the CSV's computed columns for these rows match
`features.Compute`'s formulas exactly, hand-verified against the known
input sequence — the whole point of reusing `features.Compute` instead of
reimplementing the math for the batch path.

**Actually run:**
```
r_u1_2: velocity_count=2, amount_ewma=100, deviation=0, time_since_last≈2.03s
r_u1_3: velocity_count=3, amount_ewma=100, deviation=8   (= (900-100)/100)
r_u2_2: velocity_count=2, amount_ewma=50,  deviation=399 (= (20000-50)/50)
```
All exact matches — confirms the offline path reproduces the online path's
math bit-for-bit given the same inputs.

### Test: full historical replay — no rows lost or duplicated

**Steps:** run `offline-replay` against the full `decisions` table as it
stood after months of prior testing (loadgen runs, idempotency tests,
etc. — not a fresh database), then cross-check counts:

```bash
wc -l /tmp/features_dataset.csv
docker exec -i sentinel-postgres psql -U sentinel -d sentinel -t -c "select count(*) from decisions;"
tail -n +2 /tmp/features_dataset.csv | cut -d, -f2 | sort -u | wc -l
docker exec -i sentinel-postgres psql -U sentinel -d sentinel -t -c "select count(distinct user_id) from decisions;"
```

**Expected:** CSV data-row count (file line count minus the header) equals
Postgres's row count exactly; distinct user count matches too.

**Actually run:** Postgres held 3,612 rows across 53 distinct users; the
CSV had 3,613 lines (3,612 data rows + 1 header) and 53 distinct users in
its `user_id` column — exact match on both counts.

### Known limitation: this is not a bit-for-bit reproduction of the online path

Two gaps are structural, not bugs, and are a direct consequence of what
`decisions` actually persists (`event_id, user_id, amount, currency,
decision, reason, created_at` — see `decision-service/migrations/schema.sql`):

- **Geo-distance can never be recomputed offline.** There is no `location`
  column in `decisions`, so every replayed `shared.Event` has `Location =
  nil`. `DistanceFromLastKM`/`HasDistance` are structurally always absent
  for this path — not a bug, just data that was never persisted past the
  moment `stream-processor` used it in memory on the live path. The CSV
  omits these columns entirely rather than emitting meaningless zeros.
- **Velocity and time-since-last use `created_at` (decision time), not the
  original event timestamp** — `decisions` doesn't store the latter. In
  practice these are close (seconds apart, as seen in the test above), but
  not guaranteed identical — retries or processing lag could widen that
  gap. This is the first concrete instance of exactly the kind of
  online/offline divergence Task 4 is meant to document formally.

---

## Phase 3 Task 4 — Model-serving endpoint

### Test: /score agrees with the model's own feature importances

**Steps:** start the service against the real trained model, then send
one request shaped like each fraud pattern plus one ordinary request:

```bash
cd model-serving
source .venv/bin/activate
MODEL_PATH=../model-training/artifacts/model.onnx uvicorn app.main:app --port 8085 &

curl -s localhost:8085/health

curl -s -X POST localhost:8085/score -H "Content-Type: application/json" -d '{
  "amount": 100, "velocity_count": 1, "amount_ewma": 100, "has_amount_baseline": true,
  "amount_deviation_ratio": 0.0, "time_since_last_seconds": 21600, "has_time_since_last": true,
  "distance_from_last_km": 2.0, "has_distance": true
}'
# ...and the same shape for geo_jump, amount_spike, and velocity_burst —
# see app/test_main.py for the exact request bodies.
```

**Expected:** the ordinary-shaped request scores low risk; each of the
three fraud-pattern-shaped requests scores high risk. This isn't just
"does it respond" — it's a check that `FEATURE_ORDER` in `main.py`
actually matches `train.py`'s `FEATURE_COLUMNS`, since a silent ordering
mismatch would still return a confident-looking score, just for the
wrong feature.

**Actually run:**
```
normal-shaped:         risk_score ≈ 0.000005   is_fraud: false
geo_jump-shaped:       risk_score ≈ 0.9996     is_fraud: true
amount_spike-shaped:   risk_score ≈ 0.9998     is_fraud: true
velocity_burst-shaped: risk_score ≈ 0.9999     is_fraud: true
```
All four matched expectations exactly. Malformed/incomplete requests
(`{"amount": 100}` alone, non-JSON body) correctly returned `422` via
FastAPI/pydantic validation, not a 500 or a silent wrong prediction.

These four cases are now locked in as `app/test_main.py`'s pytest suite —
6 tests, run via `python3 -m pytest -v` from `model-serving/`. Verified
from a completely clean virtualenv (`requirements.txt` plus `pytest`
and `httpx`), same reproducibility bar as tasks 2 and 3. Also verified
the tests fail *gracefully*, not loudly, when no trained model is present
(the `client` fixture calls `pytest.skip`, not an error) — exercised by
temporarily moving `model.onnx` aside and re-running; all 6 skipped
cleanly, exit code 0. That's the behavior CI actually sees, since CI
doesn't run the Go synth-dataset → train.py → convert.py pipeline that
would produce a model.

### Bug found while wiring this up

Nothing in the ONNX handling this time — the bug was stale FastAPI API
usage: `@app.on_event("startup")` is deprecated in favor of a `lifespan`
context manager (FastAPI's own deprecation warning, not a failure).
Fixed by switching to `@asynccontextmanager` + `lifespan=lifespan` on the
`FastAPI(...)` constructor, since this is new code and should use current
best practice rather than ship a known-deprecated pattern.
