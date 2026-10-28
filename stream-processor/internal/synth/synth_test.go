package synth

import "testing"

func testConfig() Config {
	return Config{
		NumUsers:         200,
		MinEventsPerUser: 10,
		MaxEventsPerUser: 30,
		FraudUserRate:    0.3,
		Seed:             42,
	}
}

func TestGenerate_Deterministic(t *testing.T) {
	a := Generate(testConfig())
	b := Generate(testConfig())

	if len(a) != len(b) {
		t.Fatalf("len(a)=%d len(b)=%d, want equal for the same seed", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("row %d differs between runs with the same seed:\n%+v\n%+v", i, a[i], b[i])
		}
	}
}

func TestGenerate_DifferentSeedDiffers(t *testing.T) {
	cfg1 := testConfig()
	cfg2 := testConfig()
	cfg2.Seed = 43

	a := Generate(cfg1)
	b := Generate(cfg2)

	if len(a) == len(b) {
		same := true
		for i := range a {
			if a[i] != b[i] {
				same = false
				break
			}
		}
		if same {
			t.Fatal("different seeds produced identical output")
		}
	}
}

func TestGenerate_NoFraudBeforeMinNormalEvents(t *testing.T) {
	rows := Generate(testConfig())

	byUser := make(map[string][]LabeledRow)
	for _, r := range rows {
		byUser[r.UserID] = append(byUser[r.UserID], r)
	}

	for userID, userRows := range byUser {
		for i, r := range userRows {
			if r.IsFraud && i < minNormalBeforeFraud {
				t.Errorf("user %s: fraud row at index %d, want no fraud before index %d (needs a baseline first)",
					userID, i, minNormalBeforeFraud)
			}
		}
	}
}

func TestGenerate_FraudRowsCarrySignal(t *testing.T) {
	rows := Generate(testConfig())

	var normalVelocity, fraudVelocitySum float64
	var normalCount, velocityBurstCount int

	var normalDeviation, spikeDeviationSum float64
	var spikeCount int

	var normalDistance, jumpDistanceSum float64
	var jumpCount int

	for _, r := range rows {
		switch {
		case !r.IsFraud:
			normalVelocity += float64(r.VelocityCount)
			if r.HasAmountBaseline {
				normalDeviation += r.AmountDeviationRatio
			}
			if r.HasDistance {
				normalDistance += r.DistanceFromLastKM
			}
			normalCount++
		case r.FraudPattern == "velocity_burst":
			fraudVelocitySum += float64(r.VelocityCount)
			velocityBurstCount++
		case r.FraudPattern == "amount_spike":
			spikeDeviationSum += r.AmountDeviationRatio
			spikeCount++
		case r.FraudPattern == "geo_jump":
			jumpDistanceSum += r.DistanceFromLastKM
			jumpCount++
		}
	}

	if velocityBurstCount == 0 || spikeCount == 0 || jumpCount == 0 {
		t.Fatalf("expected all three isolated fraud patterns to occur at least once in a dataset of %d rows; got velocity_burst=%d amount_spike=%d geo_jump=%d",
			len(rows), velocityBurstCount, spikeCount, jumpCount)
	}

	avgNormalVelocity := normalVelocity / float64(normalCount)
	avgFraudVelocity := fraudVelocitySum / float64(velocityBurstCount)
	if avgFraudVelocity <= avgNormalVelocity {
		t.Errorf("velocity_burst rows should show elevated VelocityCount: normal avg=%.2f, fraud avg=%.2f",
			avgNormalVelocity, avgFraudVelocity)
	}

	avgNormalDeviation := normalDeviation / float64(normalCount)
	avgSpikeDeviation := spikeDeviationSum / float64(spikeCount)
	if avgSpikeDeviation <= avgNormalDeviation {
		t.Errorf("amount_spike rows should show elevated AmountDeviationRatio: normal avg=%.2f, fraud avg=%.2f",
			avgNormalDeviation, avgSpikeDeviation)
	}

	avgNormalDistance := normalDistance / float64(normalCount)
	avgJumpDistance := jumpDistanceSum / float64(jumpCount)
	if avgJumpDistance <= avgNormalDistance {
		t.Errorf("geo_jump rows should show elevated DistanceFromLastKM: normal avg=%.2f, fraud avg=%.2f",
			avgNormalDistance, avgJumpDistance)
	}
}

func TestGenerate_NormalLocationChangesExist(t *testing.T) {
	// Sanity check for the realism mechanic: not every nonzero distance
	// should be fraud, or the dataset would be trivially separable on
	// HasDistance alone.
	rows := Generate(testConfig())

	foundNormalNonzeroDistance := false
	for _, r := range rows {
		if !r.IsFraud && r.HasDistance && r.DistanceFromLastKM > 1 {
			foundNormalNonzeroDistance = true
			break
		}
	}

	if !foundNormalNonzeroDistance {
		t.Error("expected at least one normal (non-fraud) row with a nonzero travel distance (alt-location mechanic)")
	}
}

// normalAltLocationDistanceBoundKM and geoJumpMinDistanceBoundKM pin down
// the separation the realism mechanics are supposed to guarantee: a
// regional alt-location switch should never read as far as a geo_jump,
// and a geo_jump should never read as close as a regional switch. These
// are deliberately loose relative to the generator's actual constants
// (altLocationOffsetDegMax=2, geoJumpMinDistanceDeg=40) so the test
// tracks the intended separation, not the exact tuning values.
const (
	normalAltLocationDistanceBoundKM = 1000
	geoJumpMinDistanceBoundKM        = 2000
)

func TestGenerate_NormalAndFraudDistancesDoNotOverlap(t *testing.T) {
	rows := Generate(testConfig())

	for _, r := range rows {
		if !r.HasDistance {
			continue
		}
		if !r.IsFraud && r.DistanceFromLastKM > normalAltLocationDistanceBoundKM {
			t.Errorf("normal row %s has distance %.0fkm, want <= %dkm (regional alt-location travel only)",
				r.EventID, r.DistanceFromLastKM, normalAltLocationDistanceBoundKM)
		}
		if r.FraudPattern == "geo_jump" || r.FraudPattern == "combined" {
			if r.DistanceFromLastKM < geoJumpMinDistanceBoundKM {
				t.Errorf("%s row %s has distance %.0fkm, want >= %dkm",
					r.FraudPattern, r.EventID, r.DistanceFromLastKM, geoJumpMinDistanceBoundKM)
			}
		}
	}
}

func TestGenerate_GeoJumpTimeSinceLastIsShort(t *testing.T) {
	// Locks in the actual "impossible travel" semantic: a geo_jump's own
	// incoming gap must be short, not inherited from whatever the prior
	// normal event's gap happened to be.
	rows := Generate(testConfig())

	found := false
	for _, r := range rows {
		if r.FraudPattern != "geo_jump" && r.FraudPattern != "combined" {
			continue
		}
		found = true
		if r.TimeSinceLastSeconds > geoJumpGapMaxSeconds {
			t.Errorf("%s row %s has TimeSinceLastSeconds=%.0f, want <= %.0f (geoJumpGapMaxSeconds)",
				r.FraudPattern, r.EventID, r.TimeSinceLastSeconds, geoJumpGapMaxSeconds)
		}
	}
	if !found {
		t.Fatal("expected at least one geo_jump or combined row in a dataset of this size")
	}
}
