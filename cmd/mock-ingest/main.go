// Command mock-ingest is a local, conformance-checking stand-in for the batch
// ingest endpoint, so EXPORT_MODE=http can be run end to end without a deployed
// backend and the batches are VALIDATED rather than merely accepted.
//
// It ships in no release artifact: the Dockerfile and the release workflow both
// build ./cmd/agent by explicit path.
//
//	mock-ingest --addr :8088 --api-key local-dev-key
//	curl -XPOST localhost:8088/control -d '{"every_nth":3,"every_nth_status":503}'
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-metrics-agent/internal/mockingest"
)

func main() {
	// The only os.Exit, so the deferred shutdown of the listener actually runs.
	os.Exit(run())
}

func run() int {
	var (
		addr      = flag.String("addr", ":8088", "listen address")
		path      = flag.String("path", mockingest.DefaultPath, "ingest path")
		apiKey    = flag.String("api-key", os.Getenv("MOCK_INGEST_API_KEY"), "required X-API-Key; empty disables the check")
		strict    = flag.Bool("strict", false, "promote every warning to a conformance failure (CI only: a failure permanently rejects the batch)")
		warnOnly  = flag.Bool("warn-only", false, "never reject a batch; report everything as a warning")
		require   = flag.Int("require", 0, "exit 0 after this many accepted batches; 0 runs until interrupted")
		keep      = flag.Int("keep", 20, "how many decompressed bodies to keep for GET /batches")
		failDir   = flag.String("fail-dir", os.TempDir(), "where to write the body of a batch that fails conformance")
		maxBodyMB = flag.Int64("max-body-mb", 32, "reject a decompressed body larger than this")
		clockSkew = flag.Duration("clock-skew", 5*time.Minute, "tolerated distance between collected_at and this server's clock")
		maxErrors = flag.Int("max-errors", 1000, "warn above this many errors in one batch")
		summary   = flag.Duration("summary", 30*time.Second, "how often to print the running summary; 0 disables")

		failFirst      = flag.Int("fail-first", 0, "answer the first N requests with --fail-status")
		failStatus     = flag.Int("fail-status", http.StatusInternalServerError, "status for --fail-first")
		everyNth       = flag.Int("every-nth", 0, "answer every Nth request with --every-nth-status")
		everyNthStatus = flag.Int("every-nth-status", http.StatusTooManyRequests, "status for --every-nth")
		retryAfter     = flag.Int("retry-after", 1, "Retry-After seconds sent with 429 and 503")
		delayMS        = flag.Int("delay-ms", 0, "sleep this long before responding")
		delayFirst     = flag.Int("delay-first", 0, "limit --delay-ms to the first N requests; 0 delays every one")
		dropEvery      = flag.Int("drop-every", 0, "close the connection with no response on every Nth request")
		dropFirst      = flag.Int("drop-first", 0, "close the connection with no response on the first N requests")
		terminalOnce   = flag.Int("terminal-once", 0, "answer exactly the next request with this status, then disarm")
	)
	flag.Parse()

	srv, err := mockingest.New(mockingest.Config{
		Path:         *path,
		APIKey:       *apiKey,
		Strict:       *strict,
		WarnOnly:     *warnOnly,
		Require:      *require,
		Keep:         *keep,
		FailDir:      *failDir,
		MaxBodyBytes: *maxBodyMB << 20,
		ClockSkew:    *clockSkew,
		MaxErrors:    *maxErrors,
		Faults: mockingest.Faults{
			FailFirst:      *failFirst,
			FailStatus:     *failStatus,
			EveryNth:       *everyNth,
			EveryNthStatus: *everyNthStatus,
			RetryAfterSec:  *retryAfter,
			DelayMS:        *delayMS,
			DelayFirst:     *delayFirst,
			DropEvery:      *dropEvery,
			DropFirst:      *dropFirst,
			TerminalOnce:   *terminalOnce,
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	server := &http.Server{
		Addr:    *addr,
		Handler: srv.Handler(),
		// Only the header read is bounded, so an injected delay can still run
		// its course against a long agent export timeout.
		ReadHeaderTimeout: 10 * time.Second,
	}

	srv.Banner(*addr)

	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServe() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var tick <-chan time.Time
	if *summary > 0 {
		t := time.NewTicker(*summary)
		defer t.Stop()
		tick = t.C
	}

	code := 0
	for done := false; !done; {
		select {
		case err := <-errs:
			fmt.Fprintln(os.Stderr, err)
			return 1
		case <-tick:
			srv.PrintSummary()
		case <-srv.Done():
			done = true
		case <-ctx.Done():
			// Under --require an interrupt means the batches never arrived,
			// which is a failure; in demo mode it is just how you stop it.
			if *require > 0 && srv.Stats().Accepted < *require {
				code = 1
			}
			done = true
		}
	}

	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)

	srv.PrintSummary()

	st := srv.Stats()
	switch {
	case st.Fatal > 0:
		fmt.Fprintf(os.Stderr, "%d batches failed conformance\n", st.Fatal)
		return 1
	case *require > 0 && st.Accepted < *require:
		fmt.Fprintf(os.Stderr, "accepted %d of the %d required batches\n", st.Accepted, *require)
		return 1
	}
	return code
}
