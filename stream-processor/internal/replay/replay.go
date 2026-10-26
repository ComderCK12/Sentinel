// Package replay recomputes features.Features from historical decisions
// rows, reusing the exact same features.Compute function stream-processor
// uses on the live Kafka path. Replay itself takes no dependency on
// Postgres or any other storage — that's cmd/offline-replay's job — so
// the grouping/ordering logic here can be unit tested with fabricated
// rows instead of a live database.
//
// Known gap versus the online path: the decisions table has no location
// column and no original event timestamp, only created_at (decision
// time). So every Row below carries Location as always absent, and
// CreatedAt stands in for the event's own Timestamp — close in practice,
// but not guaranteed identical. Geo-distance can never be recomputed from
// this table; velocity and time-since-last are only as accurate as
// created_at is to the real event time.
package replay

import (
	"sort"
	"time"

	"github.com/ComderCK12/Sentinel/shared"
	"github.com/ComderCK12/Sentinel/stream-processor/internal/features"
)

// Row is one historical decision, as read from Postgres.
type Row struct {
	EventID   string
	UserID    string
	Amount    float64
	Currency  string
	Decision  string
	Reason    string
	CreatedAt time.Time
}

// OutputRow is one row of the resulting training dataset: the original
// row's identifying fields and label, plus the features computed from
// everything that came before it for that user.
type OutputRow struct {
	EventID  string
	UserID   string
	Amount   float64
	Currency string
	Decision string
	Reason   string

	VelocityCount int

	AmountEWMA           float64
	HasAmountBaseline    bool
	AmountDeviationRatio float64

	TimeSinceLastSeconds float64
	HasTimeSinceLast     bool
}

// Run groups rows by UserID and replays each user's history in
// chronological order through features.Compute, starting from a zero
// features.State per user — exactly mirroring what a fresh Redis key
// looks like the first time stream-processor's consumer sees that user.
//
// Rows need not arrive pre-sorted: Run groups and sorts defensively, so a
// caller's query ordering is a performance nicety, not a correctness
// requirement. Output is grouped by user (sorted by UserID for a
// deterministic order), chronological within each user.
func Run(rows []Row) []OutputRow {
	byUser := make(map[string][]Row)
	for _, r := range rows {
		byUser[r.UserID] = append(byUser[r.UserID], r)
	}

	userIDs := make([]string, 0, len(byUser))
	for id := range byUser {
		userIDs = append(userIDs, id)
	}
	sort.Strings(userIDs)

	out := make([]OutputRow, 0, len(rows))
	for _, userID := range userIDs {
		userRows := byUser[userID]
		sort.Slice(userRows, func(i, j int) bool {
			return userRows[i].CreatedAt.Before(userRows[j].CreatedAt)
		})

		var state features.State
		for _, r := range userRows {
			event := &shared.Event{
				EventID:   r.EventID,
				UserID:    r.UserID,
				Amount:    r.Amount,
				Currency:  r.Currency,
				Timestamp: r.CreatedAt,
				// Location intentionally omitted — not persisted in
				// decisions, see package doc.
			}

			f, next := features.Compute(state, event)
			state = next

			out = append(out, OutputRow{
				EventID:              r.EventID,
				UserID:               r.UserID,
				Amount:               r.Amount,
				Currency:             r.Currency,
				Decision:             r.Decision,
				Reason:               r.Reason,
				VelocityCount:        f.VelocityCount,
				AmountEWMA:           f.AmountEWMA,
				HasAmountBaseline:    f.HasAmountBaseline,
				AmountDeviationRatio: f.AmountDeviationRatio,
				TimeSinceLastSeconds: f.TimeSinceLast.Seconds(),
				HasTimeSinceLast:     f.HasTimeSinceLast,
			})
		}
	}

	return out
}
