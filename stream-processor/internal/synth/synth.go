// Package synth generates a synthetic, labeled transaction dataset for
// Phase 3 model training. It exists because there is no real fraud
// ground truth anywhere in this system — the only label-like data is
// decisions.decision, which comes from the dummy amount-threshold rule
// and carries no signal from velocity, amount deviation, or geo-distance.
// Training on that would just teach a model to re-derive "amount >
// $10,000" with extra steps.
//
// Like replay (Task 3), this reuses features.Compute directly rather than
// re-deriving feature math for training data — the same reasoning: two
// implementations of "what's a velocity feature" is how training/serving
// skew happens. Unlike replay, there's nothing to read from Postgres —
// Generate fabricates whole user histories itself, including a ground
// truth IsFraud label per event, then runs them through the same Compute
// function the live path uses.
package synth

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/ComderCK12/Sentinel/shared"
	"github.com/ComderCK12/Sentinel/stream-processor/internal/features"
)

// Tuning constants. None of these are fit to real data — there isn't
// any — they're chosen to produce a dataset where fraud is detectable
// from the computed features but not trivially separable on any single
// one, so training on it is a meaningful exercise rather than relearning
// a threshold.
const (
	// minNormalBeforeFraud is how many ordinary events a user has before
	// any fraud injection. Without this, an anomaly on a user's very
	// first event has no baseline to deviate from — HasAmountBaseline
	// would be false and the deviation signal would carry no information.
	minNormalBeforeFraud = 3

	normalAmountStddevFrac = 0.15 // normal amount = baseline * (1 ± this)
	normalGapMeanHours     = 6.0  // mean inter-arrival time for ordinary events

	// altLocationProb is the chance an ordinary event happens at a user's
	// secondary location (home vs. work/nearby city) instead of home.
	// altLocationOffsetDegMax bounds that location to a MODERATE,
	// regional distance from home (roughly up to a couple hundred km) —
	// deliberately not a global random point. Without that bound, normal
	// location switches would overlap the same multi-thousand-km range
	// geo_jump uses, and distance alone would stop being a usable signal.
	altLocationProb         = 0.15
	altLocationOffsetDegMax = 2.0
	altLocationMinGapHours  = 3.0 // a real regional trip takes real time

	// quickPairProb is the chance an ordinary event follows the previous
	// one unusually fast (an organic back-to-back purchase), so a
	// velocity_burst's much larger count is a distinguishably stronger
	// signal, not "any two close-together events."
	quickPairProb       = 0.05
	quickPairGapSeconds = 60.0

	velocityBurstMinEvents = 4
	velocityBurstMaxEvents = 8
	velocityBurstGapSecMin = 5.0
	velocityBurstGapSecMax = 30.0

	amountSpikeMultiplierMin = 5.0
	amountSpikeMultiplierMax = 20.0

	// geoJumpMinDistanceDeg is a crude but effective way to force a large
	// haversine distance without computing one up front: a location at
	// least this many degrees of latitude AND longitude away from home
	// will haversine out to a large number regardless of where home is —
	// comfortably clear of altLocationOffsetDegMax, so the two never
	// overlap in distance.
	//
	// geoJumpGapMinSeconds/Max bound the gap FROM the previous event TO
	// this one — not from this one to the next. That's what makes this
	// "impossible travel": a large distance covered in minutes, not
	// hours. (An earlier version of this generator got this backwards —
	// the short gap ended up governing the event AFTER the jump instead
	// of the jump itself, so the fraud-labeled row didn't reliably carry
	// the time/distance signal it was supposed to.)
	geoJumpMinDistanceDeg = 40.0
	geoJumpGapMinSeconds  = 30.0
	geoJumpGapMaxSeconds  = 600.0 // up to 10 minutes — far too fast for the distance below
)

var fraudPatterns = []string{"velocity_burst", "amount_spike", "geo_jump", "combined"}

// Config controls dataset generation.
type Config struct {
	NumUsers         int
	MinEventsPerUser int
	MaxEventsPerUser int
	FraudUserRate    float64 // fraction of users who get one injected fraud episode
	Seed             int64
}

// LabeledRow is one synthetic event's computed features plus its ground
// truth label — the training dataset's row shape.
type LabeledRow struct {
	EventID      string
	UserID       string
	Timestamp    time.Time
	Amount       float64
	IsFraud      bool
	FraudPattern string // "" for a normal event

	VelocityCount int

	AmountEWMA           float64
	HasAmountBaseline    bool
	AmountDeviationRatio float64

	TimeSinceLastSeconds float64
	HasTimeSinceLast     bool

	DistanceFromLastKM float64
	HasDistance        bool
}

type userProfile struct {
	userID         string
	home           shared.Location
	alt            shared.Location
	baselineAmount float64
}

type synthEvent struct {
	eventID      string
	timestamp    time.Time
	amount       float64
	location     shared.Location
	isFraud      bool
	fraudPattern string
}

// Generate produces a full synthetic dataset per cfg. Deterministic for a
// given Seed — the same cfg always produces byte-identical output, which
// matters for a training dataset (reproducible experiments) and for
// testing this package itself.
func Generate(cfg Config) []LabeledRow {
	rng := rand.New(rand.NewSource(cfg.Seed))

	var out []LabeledRow
	for u := 0; u < cfg.NumUsers; u++ {
		profile := newUserProfile(rng, u)
		events := generateUserEvents(rng, profile, cfg)

		var state features.State
		for _, se := range events {
			loc := se.location
			e := &shared.Event{
				EventID:   se.eventID,
				UserID:    profile.userID,
				Amount:    se.amount,
				Currency:  "USD",
				Timestamp: se.timestamp,
				Location:  &loc,
			}

			f, next := features.Compute(state, e)
			state = next

			out = append(out, LabeledRow{
				EventID:              se.eventID,
				UserID:               profile.userID,
				Timestamp:            se.timestamp,
				Amount:               se.amount,
				IsFraud:              se.isFraud,
				FraudPattern:         se.fraudPattern,
				VelocityCount:        f.VelocityCount,
				AmountEWMA:           f.AmountEWMA,
				HasAmountBaseline:    f.HasAmountBaseline,
				AmountDeviationRatio: f.AmountDeviationRatio,
				TimeSinceLastSeconds: f.TimeSinceLast.Seconds(),
				HasTimeSinceLast:     f.HasTimeSinceLast,
				DistanceFromLastKM:   f.DistanceFromLastKM,
				HasDistance:          f.HasDistance,
			})
		}
	}

	return out
}

func newUserProfile(rng *rand.Rand, idx int) userProfile {
	home := randomLocation(rng)
	return userProfile{
		userID:         fmt.Sprintf("synth_user_%d", idx),
		home:           home,
		alt:            nearbyLocation(rng, home, altLocationOffsetDegMax),
		baselineAmount: 20 + rng.Float64()*480, // 20..500
	}
}

func randomLocation(rng *rand.Rand) shared.Location {
	return shared.Location{
		Lat: rng.Float64()*140 - 70,  // -70..70
		Lon: rng.Float64()*360 - 180, // -180..180
	}
}

// nearbyLocation returns a point within maxDeg degrees of base in each
// axis — used for a user's secondary "regional" location, kept well
// short of geoJumpMinDistanceDeg so normal and fraudulent distance
// signals never overlap.
func nearbyLocation(rng *rand.Rand, base shared.Location, maxDeg float64) shared.Location {
	return clampLocation(shared.Location{
		Lat: base.Lat + (rng.Float64()*2-1)*maxDeg,
		Lon: base.Lon + (rng.Float64()*2-1)*maxDeg,
	})
}

func jitter(loc shared.Location, rng *rand.Rand, maxDeg float64) shared.Location {
	return shared.Location{
		Lat: loc.Lat + (rng.Float64()*2-1)*maxDeg,
		Lon: loc.Lon + (rng.Float64()*2-1)*maxDeg,
	}
}

func clampLocation(loc shared.Location) shared.Location {
	return shared.Location{
		Lat: math.Max(-90, math.Min(90, loc.Lat)),
		Lon: math.Max(-180, math.Min(180, loc.Lon)),
	}
}

// generateUserEvents builds one user's full chronological event sequence,
// with at most one fraud episode inserted after minNormalBeforeFraud
// ordinary events. Every generator here takes lastEventTime — the
// timestamp of the previously placed event — and computes its own gap
// forward from it, so the gap INTO a fraud event is always the one that
// pattern actually intends, never inherited from whatever the prior
// event happened to produce.
func generateUserEvents(rng *rand.Rand, profile userProfile, cfg Config) []synthEvent {
	numEvents := cfg.MinEventsPerUser
	if span := cfg.MaxEventsPerUser - cfg.MinEventsPerUser; span > 0 {
		numEvents += rng.Intn(span + 1)
	}

	fraudAt := -1
	if rng.Float64() < cfg.FraudUserRate && numEvents > minNormalBeforeFraud {
		fraudAt = minNormalBeforeFraud + rng.Intn(numEvents-minNormalBeforeFraud)
	}

	var events []synthEvent
	lastEventTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).
		Add(time.Duration(rng.Intn(365*24)) * time.Hour)
	eventSeq := 0

	nextID := func() string {
		id := fmt.Sprintf("%s_e%d", profile.userID, eventSeq)
		eventSeq++
		return id
	}

	for j := 0; j < numEvents; j++ {
		var batch []synthEvent
		if j == fraudAt {
			batch = injectFraud(rng, profile, lastEventTime, nextID)
		} else {
			batch = []synthEvent{generateNormalEvent(rng, profile, lastEventTime, nextID())}
		}
		events = append(events, batch...)
		lastEventTime = batch[len(batch)-1].timestamp
	}

	return events
}

func generateNormalEvent(rng *rand.Rand, profile userProfile, lastEventTime time.Time, id string) synthEvent {
	amount := profile.baselineAmount * (1 + rng.NormFloat64()*normalAmountStddevFrac)
	if amount <= 0 {
		amount = profile.baselineAmount * 0.1
	}

	loc := jitter(profile.home, rng, 0.05) // ~a few km of jitter around home
	gapHours := normalGapMeanHours

	switch {
	case rng.Float64() < altLocationProb:
		loc = jitter(profile.alt, rng, 0.05)
		if gapHours < altLocationMinGapHours {
			gapHours = altLocationMinGapHours
		}
	case rng.Float64() < quickPairProb:
		gapHours = quickPairGapSeconds / 3600.0
	}

	gap := time.Duration(rng.ExpFloat64() * gapHours * float64(time.Hour))
	eventTime := lastEventTime.Add(gap)

	return synthEvent{eventID: id, timestamp: eventTime, amount: amount, location: loc}
}

// injectFraud returns the event(s) for one fraud episode. velocity_burst
// returns several events; the other patterns return one. The caller
// treats the LAST returned event's timestamp as the new clock position.
func injectFraud(rng *rand.Rand, profile userProfile, lastEventTime time.Time, nextID func() string) []synthEvent {
	pattern := fraudPatterns[rng.Intn(len(fraudPatterns))]

	switch pattern {
	case "velocity_burst":
		n := velocityBurstMinEvents + rng.Intn(velocityBurstMaxEvents-velocityBurstMinEvents+1)
		events := make([]synthEvent, 0, n)
		cur := lastEventTime
		for range n {
			gapSec := velocityBurstGapSecMin + rng.Float64()*(velocityBurstGapSecMax-velocityBurstGapSecMin)
			cur = cur.Add(time.Duration(gapSec * float64(time.Second)))

			amount := profile.baselineAmount * (1 + rng.NormFloat64()*normalAmountStddevFrac)
			if amount <= 0 {
				amount = profile.baselineAmount * 0.1
			}
			events = append(events, synthEvent{
				eventID: nextID(), timestamp: cur, amount: amount,
				location: jitter(profile.home, rng, 0.05),
				isFraud:  true, fraudPattern: pattern,
			})
		}
		return events

	case "amount_spike":
		mult := amountSpikeMultiplierMin + rng.Float64()*(amountSpikeMultiplierMax-amountSpikeMultiplierMin)
		gap := time.Duration(rng.ExpFloat64() * normalGapMeanHours * float64(time.Hour))
		return []synthEvent{{
			eventID: nextID(), timestamp: lastEventTime.Add(gap), amount: profile.baselineAmount * mult,
			location: jitter(profile.home, rng, 0.05),
			isFraud:  true, fraudPattern: pattern,
		}}

	case "geo_jump":
		amount := profile.baselineAmount * (1 + rng.NormFloat64()*normalAmountStddevFrac)
		if amount <= 0 {
			amount = profile.baselineAmount * 0.1
		}
		return geoJumpAwayAndReturn(rng, profile, lastEventTime, nextID, amount, amount, pattern)

	default: // "combined": amount spike + geo jump together
		mult := amountSpikeMultiplierMin + rng.Float64()*(amountSpikeMultiplierMax-amountSpikeMultiplierMin)
		normalAmount := profile.baselineAmount * (1 + rng.NormFloat64()*normalAmountStddevFrac)
		if normalAmount <= 0 {
			normalAmount = profile.baselineAmount * 0.1
		}
		return geoJumpAwayAndReturn(rng, profile, lastEventTime, nextID, profile.baselineAmount*mult, normalAmount, "combined")
	}
}

// geoJumpAwayAndReturn generates the two-event shape "impossible travel"
// actually needs: a jump to a far location, then — just as fast — a jump
// back. Both ends are labeled fraud. Without the return leg, the next
// ordinary event (generated independently, always relative to home)
// would show the exact same huge distance over an implausible gap, but
// with IsFraud false — a mislabeled row carrying the same signal as the
// fraud it followed. Found via TestGenerate_NormalAndFraudDistancesDoNotOverlap.
func geoJumpAwayAndReturn(rng *rand.Rand, profile userProfile, lastEventTime time.Time, nextID func() string, awayAmount, returnAmount float64, pattern string) []synthEvent {
	awayGapSec := geoJumpGapMinSeconds + rng.Float64()*(geoJumpGapMaxSeconds-geoJumpGapMinSeconds)
	awayTime := lastEventTime.Add(time.Duration(awayGapSec * float64(time.Second)))
	away := synthEvent{
		eventID: nextID(), timestamp: awayTime, amount: awayAmount,
		location: farLocation(rng, profile.home), isFraud: true, fraudPattern: pattern,
	}

	returnGapSec := geoJumpGapMinSeconds + rng.Float64()*(geoJumpGapMaxSeconds-geoJumpGapMinSeconds)
	returnTime := awayTime.Add(time.Duration(returnGapSec * float64(time.Second)))
	ret := synthEvent{
		eventID: nextID(), timestamp: returnTime, amount: returnAmount,
		location: jitter(profile.home, rng, 0.05), isFraud: true, fraudPattern: pattern,
	}

	return []synthEvent{away, ret}
}

// farLocation returns a point at least geoJumpMinDistanceDeg of latitude
// and longitude away from home — crude, but reliably haversines out to a
// large distance regardless of where home happens to be, without needing
// to compute a haversine distance up front just to generate one.
func farLocation(rng *rand.Rand, home shared.Location) shared.Location {
	latSign := 1.0
	if rng.Float64() < 0.5 {
		latSign = -1
	}
	lonSign := 1.0
	if rng.Float64() < 0.5 {
		lonSign = -1
	}

	return clampLocation(shared.Location{
		Lat: home.Lat + latSign*geoJumpMinDistanceDeg,
		Lon: home.Lon + lonSign*geoJumpMinDistanceDeg,
	})
}
