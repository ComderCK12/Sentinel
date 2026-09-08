// Package features computes rolling per-user behavioral features from a
// stream of shared.Event. This package is deliberately storage-agnostic:
// Compute is a pure function of (prior State, new Event) -> (Features,
// next State). Where State actually lives (Redis, in-memory, whatever) is
// Task 2's concern, not this one — keeping the math here means it can be
// unit tested without a Redis instance, and reused later for the offline
// batch recomputation (Task 3) against Postgres history instead of a
// live stream.
package features

import (
	"math"
	"time"

	"github.com/ComderCK12/Sentinel/shared"
)

// velocityWindow is the fixed window used for the velocity feature. A
// fixed window (not EWMA) is deliberate here: velocity feeds a rule
// threshold ("block if >5 txns in 60s"), and a rule threshold wants an
// exact, interpretable count over a concrete window, not an approximated
// decayed rate.
const velocityWindow = 60 * time.Second

// emaAlpha controls how fast the running average of transaction amount
// adapts to new events. EWMA (not a fixed window) is deliberate for this
// feature: unlike velocity, "deviation from historical average" wants
// unbounded history collapsed into O(1) state — a fixed window here would
// mean either storing a user's full amount history forever or arbitrarily
// throwing away everything older than the window. A higher alpha weights
// recent events more; 0.3 is a starting point, not a tuned value.
const emaAlpha = 0.3

// State is a user's rolling feature state, carried event-to-event.
// Everything in it is intentionally small and fixed-size except
// RecentTimestamps, which is bounded by eviction in Compute (see below).
type State struct {
	// RecentTimestamps holds event times within the last velocityWindow,
	// oldest first. Evicted on every Compute call, so this never grows
	// past however many events a user can generate in one window.
	RecentTimestamps []time.Time

	// MeanAmount is the EWMA of transaction amount. Zero value (no prior
	// events) is handled explicitly in Compute, not treated as "average
	// of $0".
	MeanAmount float64
	HasMean    bool

	LastTimestamp time.Time
	HasLast       bool

	LastLocation *shared.Location // nil if the user has no known location yet
}

// Features is the computed feature vector for a single event, given the
// user's state immediately before it.
type Features struct {
	// VelocityCount is the number of transactions (including this one)
	// in the trailing velocityWindow.
	VelocityCount int

	// AmountEWMA is the EWMA of amount *before* this event was folded in
	// — i.e. "what this user's typical amount looked like going in",
	// which is what a deviation check should compare the current amount
	// against.
	AmountEWMA float64
	// AmountDeviationRatio is (amount - AmountEWMA) / AmountEWMA. Left at
	// its zero value (0) when AmountEWMA is 0 (no prior history) —
	// callers should check HasAmountBaseline before trusting this, since
	// a first-ever event isn't "0% deviation", it's "no baseline yet".
	AmountDeviationRatio float64
	HasAmountBaseline    bool

	// TimeSinceLast is time since this user's previous event. Zero value
	// with HasTimeSinceLast == false means this is the user's first
	// known event, not "zero seconds since last time".
	TimeSinceLast    time.Duration
	HasTimeSinceLast bool

	// DistanceFromLastKM is the great-circle distance from this user's
	// last known location to the current one. Only populated when both
	// the prior state and the current event carry a location.
	DistanceFromLastKM float64
	HasDistance        bool
}

// Compute derives Features for e given the user's prior state, and
// returns the state to carry forward to the next event for this user.
// e.Timestamp is assumed already set (shared.Event.Validate defaults it
// to time.Now().UTC() if the caller didn't supply one).
func Compute(prev State, e *shared.Event) (Features, State) {
	var f Features
	next := prev

	// --- velocity: fixed trailing window, evict-then-append ---
	cutoff := e.Timestamp.Add(-velocityWindow)
	kept := prev.RecentTimestamps[:0:0] // don't mutate prev's backing array
	for _, ts := range prev.RecentTimestamps {
		if ts.After(cutoff) {
			kept = append(kept, ts)
		}
	}
	kept = append(kept, e.Timestamp)
	next.RecentTimestamps = kept
	f.VelocityCount = len(kept)

	// --- amount deviation: EWMA updated after computing the deviation
	// against the *prior* mean, so this event's own amount doesn't
	// dilute the baseline it's being compared to ---
	if prev.HasMean {
		f.AmountEWMA = prev.MeanAmount
		f.HasAmountBaseline = true
		if prev.MeanAmount != 0 {
			f.AmountDeviationRatio = (e.Amount - prev.MeanAmount) / prev.MeanAmount
		}
		next.MeanAmount = emaAlpha*e.Amount + (1-emaAlpha)*prev.MeanAmount
	} else {
		// First event for this user: nothing to compare against yet.
		// Seed the EWMA with this event's amount rather than 0, so the
		// *next* event gets a sane baseline instead of comparing
		// against an artificial 0.
		next.MeanAmount = e.Amount
	}
	next.HasMean = true

	// --- time since last event ---
	if prev.HasLast {
		f.TimeSinceLast = e.Timestamp.Sub(prev.LastTimestamp)
		f.HasTimeSinceLast = true
	}
	next.LastTimestamp = e.Timestamp
	next.HasLast = true

	// --- geo-distance from last known location ---
	if prev.LastLocation != nil && e.Location != nil {
		f.DistanceFromLastKM = haversineKM(*prev.LastLocation, *e.Location)
		f.HasDistance = true
	}
	if e.Location != nil {
		next.LastLocation = e.Location
	}
	// If this event has no location, deliberately keep prev.LastLocation
	// rather than clearing it — a missing location on one event shouldn't
	// erase the last known one for the next comparison.

	return f, next
}

const earthRadiusKM = 6371.0

// haversineKM returns the great-circle distance between two lat/lon
// points in kilometers.
func haversineKM(a, b shared.Location) float64 {
	lat1, lon1 := degToRad(a.Lat), degToRad(a.Lon)
	lat2, lon2 := degToRad(b.Lat), degToRad(b.Lon)

	dLat := lat2 - lat1
	dLon := lon2 - lon1

	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(h), math.Sqrt(1-h))

	return earthRadiusKM * c
}

func degToRad(deg float64) float64 {
	return deg * math.Pi / 180
}
