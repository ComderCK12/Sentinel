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
