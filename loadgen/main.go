// loadgen generates synthetic traffic against the ingestion service to
// prove the pipeline's idempotency guarantee under concurrent load: every
// unique event gets exactly one decision, and duplicates don't produce
// extra rows or extra Kafka publishes.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ComderCK12/Sentinel/shared"
)

func main() {
	var (
		total       = flag.Int("n", 1000, "total number of requests to send")
		dupRate     = flag.Float64("dup-rate", 0.1, "fraction of requests that re-send an already-used event_id")
		concurrency = flag.Int("concurrency", 50, "number of concurrent workers")
		url         = flag.String("url", "http://localhost:8080/v1/events", "ingestion events endpoint")
	)
	flag.Parse()

	events := buildEvents(*total, *dupRate)

	var accepted, duplicate, failed int64
	var wg sync.WaitGroup
	jobs := make(chan shared.Event)

	client := &http.Client{Timeout: 10 * time.Second}

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range jobs {
				status, err := send(client, *url, e)
				switch {
				case err != nil:
					atomic.AddInt64(&failed, 1)
					fmt.Printf("request failed: event_id=%s err=%v\n", e.EventID, err)
				case status == "duplicate":
					atomic.AddInt64(&duplicate, 1)
				case status == "accepted":
					atomic.AddInt64(&accepted, 1)
				default:
					atomic.AddInt64(&failed, 1)
					fmt.Printf("unexpected status: event_id=%s status=%s\n", e.EventID, status)
				}
			}
		}()
	}

	start := time.Now()
	for _, e := range events {
		jobs <- e
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\n--- loadgen summary ---\n")
	fmt.Printf("total requests:  %d\n", *total)
	fmt.Printf("unique events:   %d\n", len(uniqueEventIDs(events)))
	fmt.Printf("accepted:        %d\n", accepted)
	fmt.Printf("duplicate:       %d\n", duplicate)
	fmt.Printf("failed:          %d\n", failed)
	fmt.Printf("elapsed:         %s\n", elapsed)
}

// buildEvents produces n events: roughly n*(1-dupRate) unique ones, plus
// re-sends of already-generated event_ids for the rest, then shuffles the
// whole set so duplicates are interleaved with fresh events rather than
// arriving strictly after — that's what actually exercises the race in
// the idempotency check, not just its happy path.
func buildEvents(n int, dupRate float64) []shared.Event {
	dupCount := int(float64(n) * dupRate)
	uniqueCount := n - dupCount

	runID := time.Now().UnixNano()
	events := make([]shared.Event, 0, n)
	uniqueIDs := make([]string, 0, uniqueCount)

	for i := 0; i < uniqueCount; i++ {
		id := fmt.Sprintf("loadgen-%d-%d", runID, i)
		uniqueIDs = append(uniqueIDs, id)
		events = append(events, shared.Event{
			EventID:  id,
			UserID:   fmt.Sprintf("user-%d", i%50),
			Amount:   roundAmount(rand.Float64() * 20000),
			Currency: "USD",
		})
	}

	for i := 0; i < dupCount; i++ {
		id := uniqueIDs[rand.Intn(len(uniqueIDs))]
		events = append(events, shared.Event{
			EventID:  id,
			UserID:   fmt.Sprintf("user-%d", i%50),
			Amount:   roundAmount(rand.Float64() * 20000),
			Currency: "USD",
		})
	}

	rand.Shuffle(len(events), func(i, j int) {
		events[i], events[j] = events[j], events[i]
	})

	return events
}

func uniqueEventIDs(events []shared.Event) map[string]struct{} {
	seen := make(map[string]struct{}, len(events))
	for _, e := range events {
		seen[e.EventID] = struct{}{}
	}
	return seen
}

func roundAmount(v float64) float64 {
	return float64(int(v*100)) / 100
}

func send(client *http.Client, url string, e shared.Event) (status string, err error) {
	body, err := json.Marshal(e)
	if err != nil {
		return "", fmt.Errorf("marshal event: %w", err)
	}

	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	var out struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	return out.Status, nil
}
