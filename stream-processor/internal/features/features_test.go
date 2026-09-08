package features

import (
	"math"
	"testing"
	"time"

	"github.com/ComderCK12/Sentinel/shared"
)

func evt(ts time.Time, amount float64, loc *shared.Location) *shared.Event {
	return &shared.Event{
		EventID:   "e",
		UserID:    "u1",
		Amount:    amount,
		Currency:  "USD",
		Timestamp: ts,
		Location:  loc,
	}
}

func TestCompute_FirstEvent_NoBaselines(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	f, next := Compute(State{}, evt(now, 100, nil))

	if f.VelocityCount != 1 {
		t.Errorf("VelocityCount = %d, want 1", f.VelocityCount)
	}
	if f.HasAmountBaseline {
		t.Error("HasAmountBaseline should be false on first event")
	}
	if f.HasTimeSinceLast {
		t.Error("HasTimeSinceLast should be false on first event")
	}
	if f.HasDistance {
		t.Error("HasDistance should be false on first event")
	}
	if !next.HasMean || next.MeanAmount != 100 {
		t.Errorf("next state should seed MeanAmount with first amount, got %v (has=%v)", next.MeanAmount, next.HasMean)
	}
	if !next.HasLast || !next.LastTimestamp.Equal(now) {
		t.Errorf("next state should record LastTimestamp = %v, got %v (has=%v)", now, next.LastTimestamp, next.HasLast)
	}
}

func TestCompute_VelocityWindow_EvictsOldEvents(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	state := State{}

	// 3 events at t=0s, t=10s, t=50s: all within the 60s window.
	_, state = Compute(state, evt(base, 10, nil))
	_, state = Compute(state, evt(base.Add(10*time.Second), 10, nil))
	f, state := Compute(state, evt(base.Add(50*time.Second), 10, nil))
	if f.VelocityCount != 3 {
		t.Fatalf("VelocityCount = %d, want 3 (all within window)", f.VelocityCount)
	}

	// 4th event at t=90s: cutoff is t=30s, so the t=0s and t=10s events
	// have aged out, leaving only t=50s and this one within the window.
	f, state = Compute(state, evt(base.Add(90*time.Second), 10, nil))
	if f.VelocityCount != 2 {
		t.Fatalf("VelocityCount = %d, want 2 (first two events evicted)", f.VelocityCount)
	}
	if len(state.RecentTimestamps) != 2 {
		t.Errorf("state.RecentTimestamps len = %d, want 2", len(state.RecentTimestamps))
	}
}

func TestCompute_AmountDeviation_ComparesAgainstPriorMean(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	state := State{}

	// First event seeds the baseline at 100, no deviation reported yet.
	_, state = Compute(state, evt(base, 100, nil))

	// Second event of 200: deviation should be measured against the
	// *prior* mean (100), not diluted by this event's own amount.
	f, state := Compute(state, evt(base.Add(time.Second), 200, nil))
	if !f.HasAmountBaseline {
		t.Fatal("HasAmountBaseline should be true on second event")
	}
	if f.AmountEWMA != 100 {
		t.Errorf("AmountEWMA = %v, want 100 (prior mean, not diluted by this event)", f.AmountEWMA)
	}
	wantRatio := (200.0 - 100.0) / 100.0 // = 1.0, a 100% spike
	if f.AmountDeviationRatio != wantRatio {
		t.Errorf("AmountDeviationRatio = %v, want %v", f.AmountDeviationRatio, wantRatio)
	}

	wantNextMean := emaAlpha*200 + (1-emaAlpha)*100
	if math.Abs(state.MeanAmount-wantNextMean) > 1e-9 {
		t.Errorf("next MeanAmount = %v, want %v", state.MeanAmount, wantNextMean)
	}
}

func TestCompute_TimeSinceLast(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	state := State{}

	_, state = Compute(state, evt(base, 10, nil))
	f, _ := Compute(state, evt(base.Add(45*time.Second), 10, nil))

	if !f.HasTimeSinceLast {
		t.Fatal("HasTimeSinceLast should be true on second event")
	}
	if f.TimeSinceLast != 45*time.Second {
		t.Errorf("TimeSinceLast = %v, want 45s", f.TimeSinceLast)
	}
}

func TestCompute_GeoDistance(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	nyc := &shared.Location{Lat: 40.7128, Lon: -74.0060}
	london := &shared.Location{Lat: 51.5074, Lon: -0.1278}

	state := State{}
	_, state = Compute(state, evt(base, 10, nyc))

	// No location on this event: distance not computable, and the last
	// known location should carry forward rather than being cleared.
	f, state := Compute(state, evt(base.Add(time.Minute), 10, nil))
	if f.HasDistance {
		t.Error("HasDistance should be false when current event has no location")
	}
	if state.LastLocation != nyc {
		t.Error("LastLocation should carry forward when an event has no location")
	}

	// Jump to London: real-world distance is ~5570km, generous tolerance
	// since this is just checking the formula is wired up, not exactness.
	f, _ = Compute(state, evt(base.Add(2*time.Minute), 10, london))
	if !f.HasDistance {
		t.Fatal("HasDistance should be true when both prior and current location are known")
	}
	if f.DistanceFromLastKM < 5400 || f.DistanceFromLastKM > 5700 {
		t.Errorf("DistanceFromLastKM = %v, want ~5570km (NYC-London)", f.DistanceFromLastKM)
	}
}

func TestCompute_SameLocation_ZeroDistance(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	loc := &shared.Location{Lat: 12.34, Lon: 56.78}

	state := State{}
	_, state = Compute(state, evt(base, 10, loc))
	f, _ := Compute(state, evt(base.Add(time.Minute), 10, loc))

	if !f.HasDistance {
		t.Fatal("HasDistance should be true")
	}
	if math.Abs(f.DistanceFromLastKM) > 1e-6 {
		t.Errorf("DistanceFromLastKM = %v, want ~0 for identical coordinates", f.DistanceFromLastKM)
	}
}
