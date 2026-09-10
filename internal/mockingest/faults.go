package mockingest

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Faults injects failures into the response path so the exporter's retry
// classifier, backoff and idempotency handling can be exercised on demand.
//
// Everything here is DETERMINISTIC — counted or a modulus over the request
// ordinal, never sampled. Randomly injected faults are indistinguishable from
// real regressions and make any CI gate built on them flake.
type Faults struct {
	// FailFirst makes the first N ingest requests answer FailStatus, modelling
	// an ingest that is down at agent startup.
	FailFirst  int `json:"fail_first"`
	FailStatus int `json:"fail_status"`

	// EveryNth makes every Nth request answer EveryNthStatus. It models a rate
	// limiter, which is why the default status is 429.
	EveryNth       int `json:"every_nth"`
	EveryNthStatus int `json:"every_nth_status"`

	// RetryAfterSec is sent as Retry-After on 429 and 503. The exporter honours
	// it in place of its own backoff, so this is the only way to reach that
	// branch.
	RetryAfterSec int `json:"retry_after_sec"`

	// DelayMS sleeps before responding; set it above the agent's EXPORT_TIMEOUT
	// to exercise the client-timeout path. DelayFirst limits the delay to the
	// first N requests; 0 delays every request, including every retry, and so
	// can never be recovered from.
	DelayMS    int `json:"delay_ms"`
	DelayFirst int `json:"delay_first"`

	// DropEvery hijacks and closes the connection on every Nth request with no
	// response at all, the only way to produce a genuine transport error rather
	// than an HTTP status. DropFirst does the same to the first N requests, so
	// the batch is destroyed once and then allowed through and the redelivery
	// can be checked.
	DropEvery int `json:"drop_every"`
	DropFirst int `json:"drop_first"`

	// TerminalOnce answers exactly the next request with this status and then
	// disarms. Set it to 400 to watch a batch be permanently rejected and land
	// in the agent's own batches_rejected counter on the following cycle.
	TerminalOnce int `json:"terminal_once"`
}

// withDefaults fills the status codes that only make sense non-zero. It is
// applied on every assignment, including via /control, so a partial JSON body
// cannot leave a fault armed with status 0.
func (f Faults) withDefaults() Faults {
	if f.FailStatus == 0 {
		f.FailStatus = http.StatusInternalServerError
	}
	if f.EveryNthStatus == 0 {
		f.EveryNthStatus = http.StatusTooManyRequests
	}
	if f.RetryAfterSec == 0 {
		f.RetryAfterSec = 1
	}
	return f
}

func (f Faults) armed() bool {
	return f.FailFirst > 0 || f.EveryNth > 0 || f.DelayMS > 0 ||
		f.DropEvery > 0 || f.DropFirst > 0 || f.TerminalOnce > 0
}

type faultKind int

const (
	faultNone faultKind = iota
	faultStatus
	faultDrop
)

type fault struct {
	kind       faultKind
	status     int
	retryAfter int
	delay      time.Duration
	// reason names the field that fired, so the [fault] line says which knob
	// produced it and nobody mistakes it for a real failure.
	reason string
}

func (f fault) active() bool { return f.kind != faultNone || f.delay > 0 }

func (f fault) String() string {
	switch {
	case f.kind == faultDrop:
		return fmt.Sprintf("dropping connection (%s)", f.reason)
	case f.kind == faultStatus && f.retryAfter > 0:
		return fmt.Sprintf("injecting %d Retry-After: %d (%s)", f.status, f.retryAfter, f.reason)
	case f.kind == faultStatus:
		return fmt.Sprintf("injecting %d (%s)", f.status, f.reason)
	default:
		return fmt.Sprintf("delaying %s (%s)", f.delay, f.reason)
	}
}

// decide picks the fault for ingest request n (1-based). Precedence is
// TerminalOnce > DropEvery > FailFirst > EveryNth, and a delay applies on top
// of whichever fires. The caller must hold s.mu: TerminalOnce is consumed here.
func (s *Server) decide(n int) fault {
	f := s.faults
	var out fault

	if f.DelayMS > 0 && (f.DelayFirst == 0 || n <= f.DelayFirst) {
		out.delay = time.Duration(f.DelayMS) * time.Millisecond
		out.reason = fmt.Sprintf("delay_ms=%d", f.DelayMS)
	}

	switch {
	case f.TerminalOnce > 0 && s.terminalArmed:
		s.terminalArmed = false
		out.kind, out.status = faultStatus, f.TerminalOnce
		out.reason = fmt.Sprintf("terminal_once=%d", f.TerminalOnce)
	case f.DropFirst > 0 && n <= f.DropFirst:
		out.kind = faultDrop
		out.reason = fmt.Sprintf("drop_first=%d", f.DropFirst)
	case f.DropEvery > 0 && n%f.DropEvery == 0:
		out.kind = faultDrop
		out.reason = fmt.Sprintf("drop_every=%d", f.DropEvery)
	case f.FailFirst > 0 && n <= f.FailFirst:
		out.kind, out.status = faultStatus, f.FailStatus
		out.reason = fmt.Sprintf("fail_first=%d", f.FailFirst)
	case f.EveryNth > 0 && n%f.EveryNth == 0:
		out.kind, out.status = faultStatus, f.EveryNthStatus
		out.reason = fmt.Sprintf("every_nth=%d", f.EveryNth)
	}

	// Retry-After is only meaningful on the statuses that mean "come back
	// later"; on a 500 it would misrepresent the contract.
	if out.kind == faultStatus &&
		(out.status == http.StatusTooManyRequests || out.status == http.StatusServiceUnavailable) {
		out.retryAfter = f.RetryAfterSec
	}

	if out.active() {
		s.stats.FaultsInjected++
		s.faultHits[out.reason]++
	}
	return out
}

// drop closes the connection without writing a byte. Hijack is the only way to
// do that — any normal return writes at least a status line — and it needs a
// direct HTTP/1.1 connection, so this will not behave through an HTTP/2 ingress.
func drop(w http.ResponseWriter) error {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return err
	}
	return conn.Close()
}

// sleepCtx reports whether the delay elapsed. False means the client gave up
// first, which the caller must treat as "this delivery never landed".
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
