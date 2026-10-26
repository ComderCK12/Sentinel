# Online vs offline feature divergence

Phase 2 Task 4. Both paths call the same `features.Compute` function
(`stream-processor/internal/features`) — that was the whole point of
keeping it storage-agnostic in Task 1. But "same function" doesn't mean
"same output": the two paths feed it different *inputs*, because the
`decisions` table (the only source offline replay has) doesn't persist
everything the live path had available. This doc is the formal writeup of
where that leaves each feature, found concretely while building and
testing Task 3.

Why this matters: a model trained on offline-replayed features is trained
on an approximation of what it will see at serving time. Where that
approximation is wrong in a structural, not-random way, the model can
learn the wrong thing — e.g. learning to ignore a feature that's always
blank in training but populated in production.

## Root cause

`decision-service/migrations/schema.sql`'s `decisions` table is:

```sql
event_id, user_id, amount, currency, decision, reason, created_at
```

Two things the live path uses are missing:

- **No `location` column.** `shared.Event.Location` is never written anywhere past the moment `stream-processor` holds it in memory.
- **No original event timestamp.** `created_at` is `TIMESTAMPTZ NOT NULL DEFAULT now()` — stamped by Postgres when `decision-service` writes the row, not when the event actually happened at the client or at ingestion.

## Per-feature impact

### Velocity (trailing 60s count)

**Basis:** online, `shared.Event.Timestamp` (set by ingestion, defaulting to `time.Now().UTC()` at the moment the HTTP request is validated). Offline, `created_at` (set by Postgres at the moment `decision-service`'s write commits).

**Divergence:** `created_at` lags the true event timestamp by however long the event spent in flight — HTTP handling, the Kafka produce/consume round trip, and `decision-service`'s own processing. In the normal case this is milliseconds to low single-digit seconds (confirmed in Task 3's live test: ~2s between sends tracked almost exactly in both `TimeSinceLast` and the resulting `created_at` gaps). Against a 60-second window, that's usually noise. It stops being noise under the failure scenarios Task 1-3 specifically tested: a Kafka outage, a slow Redis, or consumer redelivery after a crash can each stretch that lag to seconds or more, which *can* move an event across the window boundary — present in the online count, absent (or vice versa) in the offline replay of the same history.

**Severity:** Low under normal operation, elevated specifically during the failure modes this project already built resilience for.

### Amount deviation (EWMA)

**Basis:** both paths use the same `Amount` field — not timestamp-sensitive on its own. But EWMA is sequence-sensitive: it depends on the *order* events are folded in, not just their timestamps.

**Divergence:** online order is guaranteed by Kafka partitioning on `user_id` (same partition, in-order delivery). Offline order is reconstructed by sorting `decisions` rows on `created_at` per user. These two orderings are the same in the common case — but if `decision-service` ever redelivers an event after a crash (see Phase 1 Task 3's idempotency testing), that event's `created_at` reflects its *second* attempt, which could land after a logically-later event that succeeded on its first try. The rows's `event_id` primary key plus `ON CONFLICT DO NOTHING` guarantees no duplicate row — but it doesn't guarantee `created_at` ordering matches true event ordering under redelivery. An EWMA replayed in the wrong order produces a different trajectory than what was actually computed live.

**Severity:** Low-probability (only triggered by the redelivery path), but silent — nothing fails loudly when it happens, the offline number is just quietly wrong for that user's subsequent rows.

### Time since last event

**Basis:** same as velocity — `shared.Event.Timestamp` online, `created_at` offline.

**Divergence:** identical reasoning and severity to velocity. Usually accurate to within the same small lag; degrades under the same failure scenarios.

### Geo-distance

**Basis:** online, `shared.Event.Location`. Offline: nothing — the column doesn't exist.

**Divergence:** total, not partial. `replay.Row` has no location field at all, so every replayed `shared.Event` has `Location: nil`, and `features.Compute` correctly reports `HasDistance: false` for every single offline row, regardless of what the live path actually saw. This is a structural gap, not an approximation — it cannot be narrowed by better timestamp precision or ordering, only by changing what gets persisted.

**Severity:** High. This is the one divergence that will bias model training outright if not handled deliberately (see Recommendations).

## Recommendations

**For Phase 3 (immediate):** exclude `DistanceFromLastKM`/`HasDistance` from the training feature set entirely, rather than imputing it as `0`/absent. Imputing a fake "no movement" value would actively teach the model that distance is always ~0, which is worse than not having the feature at all — the model would learn a false pattern instead of just lacking a true one. Velocity, amount deviation, and time-since-last can stay in the training set as-is; their divergence is bounded and occurs only under already-tested failure conditions, not structurally on every row.

**Medium-term:** extend `decisions` with nullable `event_timestamp TIMESTAMPTZ` and `location_lat`/`location_lon DOUBLE PRECISION` columns, populated by `decision-service` going forward. This doesn't fix historical rows (they stay null) but stops the gap from being permanent — future offline replays would have real location data for everything decided after the migration ships.

**Long-term:** the structural fix for training/serving skew isn't better replay — it's logging what was *actually* computed online. A feature-snapshot log (stream-processor writes its computed `Features`, not just raw inputs, to a durable store at decision time) would let offline training consume exactly what serving saw, instead of reconstructing an approximation of it after the fact. Out of scope for this project's current phase, but the honest long-term answer.
