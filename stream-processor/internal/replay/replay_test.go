package replay

import (
	"testing"
	"time"
)

func TestRun_GroupsPerUser_IgnoresInputOrder(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Deliberately interleaved and out of chronological order within each
	// user — Run must sort per user by CreatedAt regardless of the order
	// rows arrive in, since a caller's query ordering isn't something this
	// package should have to trust.
	rows := []Row{
		{EventID: "b2", UserID: "userB", Amount: 50, CreatedAt: base.Add(10 * time.Second)},
		{EventID: "a1", UserID: "userA", Amount: 100, CreatedAt: base},
		{EventID: "b1", UserID: "userB", Amount: 20, CreatedAt: base},
		{EventID: "a2", UserID: "userA", Amount: 200, CreatedAt: base.Add(5 * time.Second)},
	}

	out := Run(rows)

	if len(out) != 4 {
		t.Fatalf("len(out) = %d, want 4", len(out))
	}

	// userA sorts before userB; within each user, chronological order.
	wantOrder := []string{"a1", "a2", "b1", "b2"}
	for i, want := range wantOrder {
		if out[i].EventID != want {
			t.Errorf("out[%d].EventID = %q, want %q", i, out[i].EventID, want)
		}
	}
}

func TestRun_PerUserStateDoesNotLeakAcrossUsers(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	rows := []Row{
		{EventID: "a1", UserID: "userA", Amount: 1000, CreatedAt: base},
		{EventID: "a2", UserID: "userA", Amount: 1000, CreatedAt: base.Add(time.Second)},
		// userB's first-ever event: must have no baseline, regardless of
		// what userA's amounts were.
		{EventID: "b1", UserID: "userB", Amount: 5, CreatedAt: base},
	}

	out := Run(rows)

	var b1 OutputRow
	for _, r := range out {
		if r.EventID == "b1" {
			b1 = r
		}
	}

	if b1.HasAmountBaseline {
		t.Error("userB's first event should have no amount baseline, regardless of userA's history")
	}
	if b1.VelocityCount != 1 {
		t.Errorf("userB's first event VelocityCount = %d, want 1", b1.VelocityCount)
	}
}

func TestRun_VelocityAndDeviation_MatchExpectedSequence(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	rows := []Row{
		{EventID: "e1", UserID: "u1", Amount: 100, CreatedAt: base},
		{EventID: "e2", UserID: "u1", Amount: 100, CreatedAt: base.Add(10 * time.Second)},
		{EventID: "e3", UserID: "u1", Amount: 500, CreatedAt: base.Add(20 * time.Second)},
	}

	out := Run(rows)
	if len(out) != 3 {
		t.Fatalf("len(out) = %d, want 3", len(out))
	}

	if out[0].VelocityCount != 1 || out[1].VelocityCount != 2 || out[2].VelocityCount != 3 {
		t.Errorf("velocity sequence = [%d, %d, %d], want [1, 2, 3]",
			out[0].VelocityCount, out[1].VelocityCount, out[2].VelocityCount)
	}

	// e3's deviation should be measured against the prior EWMA (100),
	// same contract as features.Compute itself.
	if !out[2].HasAmountBaseline {
		t.Fatal("e3 should have an amount baseline")
	}
	wantRatio := (500.0 - 100.0) / 100.0
	if out[2].AmountDeviationRatio != wantRatio {
		t.Errorf("e3 AmountDeviationRatio = %v, want %v", out[2].AmountDeviationRatio, wantRatio)
	}

	if !out[1].HasTimeSinceLast || out[1].TimeSinceLastSeconds != 10 {
		t.Errorf("e2 TimeSinceLastSeconds = %v (has=%v), want 10 (has=true)",
			out[1].TimeSinceLastSeconds, out[1].HasTimeSinceLast)
	}
}

func TestRun_EmptyInput(t *testing.T) {
	out := Run(nil)
	if len(out) != 0 {
		t.Errorf("len(out) = %d, want 0 for empty input", len(out))
	}
}
