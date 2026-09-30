package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/nudgequeue"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/worker"
)

// The nudge-honesty rows. Every one of these is a place the nudge path told an
// operator something that was not true: a store that would not open reported no
// cause, and a queued item reported plain success whether or not the live leg
// had even been attempted.

// TestOpenNudgeBeadStoreReportsWhyItCouldNotOpen: the seam used by the poll and
// drain helpers stays nil-tolerant (their contract is "no store means do
// nothing"), but the error form the operator-facing call sites use must carry
// the cause.
func TestOpenNudgeBeadStoreReportsWhyItCouldNotOpen(t *testing.T) {
	// A regular file where the city directory should be: the open genuinely
	// fails, which is the case the swallowed error used to render as an
	// unexplained "opening city store for X".
	notACity := filepath.Join(t.TempDir(), "city-is-a-file")
	if err := os.WriteFile(notACity, []byte("not a city"), 0o600); err != nil {
		t.Fatalf("seeding the fixture: %v", err)
	}

	store, err := openNudgeBeadStoreErr(notACity)
	if err == nil {
		t.Skipf("openNudgeBeadStoreErr(%q) opened a store over a plain file; this row needs a fixture the store layer actually refuses", notACity)
	}
	if store.Store != nil {
		t.Fatal("a failed open returned a usable store")
	}
	if !strings.Contains(err.Error(), notACity) {
		t.Fatalf("error = %q, want the offending city path named", err)
	}
}

// TestQueuedNudgeResultNamesTheQueueAndTheDowngrade: "Queued nudge for X" was
// printed identically whether the nudge had been queued by request or silently
// downgraded from a live delivery the provider cannot take — and it never said
// WHERE the item went. The queue's authority is the flock'd state.json (the
// shadow bead is a projection of it), so that path is what an operator needs.
func TestQueuedNudgeResultNamesTheQueueAndTheDowngrade(t *testing.T) {
	cityPath := t.TempDir()
	target := nudgeTarget{
		cityPath: cityPath,
		alias:    "worker-1",
		agent:    config.Agent{Name: "worker"},
		resolved: &config.ResolvedProvider{Name: "codex"},
	}

	var stdout, stderr bytes.Buffer
	if code := writeQueuedSessionNudgeResult(target, nudgeDeliveryWaitIdle, false,
		worker.NudgeUndeliveredProviderUnsupported, &stdout, &stderr); code != 0 {
		t.Fatalf("writeQueuedSessionNudgeResult = %d, want 0; stderr=%s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, nudgequeue.StatePath(cityPath)) {
		t.Fatalf("queued message = %q, want the state.json queue path named", out)
	}
	if !strings.Contains(out, "live delivery is unsupported") || !strings.Contains(out, "codex") {
		t.Fatalf("queued message = %q, want the skipped live leg and its provider named", out)
	}

	// Control: a nudge queued BY REQUEST carries no downgrade note — the note
	// must describe something that happened, not decorate every queue write.
	stdout.Reset()
	if code := writeQueuedSessionNudgeResult(target, nudgeDeliveryQueue, false, "", &stdout, &stderr); code != 0 {
		t.Fatalf("writeQueuedSessionNudgeResult = %d, want 0", code)
	}
	out = stdout.String()
	if strings.Contains(out, "unsupported") || strings.Contains(out, "idle boundary") {
		t.Fatalf("queued-by-request message = %q, want no downgrade note", out)
	}
	if !strings.Contains(out, nudgequeue.StatePath(cityPath)) {
		t.Fatalf("queued-by-request message = %q, want the queue path named", out)
	}
}

// TestQueuedNudgeDowngradeNoteDistinguishesItsCauses keeps the two downgrades
// distinguishable: an unsupported transport is a permanent property of the
// runtime, while a missed idle boundary is a transient state of the session, and
// an operator acts differently on each.
func TestQueuedNudgeDowngradeNoteDistinguishesItsCauses(t *testing.T) {
	target := nudgeTarget{resolved: &config.ResolvedProvider{Name: "codex"}}
	unsupported := queuedNudgeDowngradeNote(target, worker.NudgeUndeliveredProviderUnsupported)
	noIdle := queuedNudgeDowngradeNote(target, worker.NudgeUndeliveredNoIdleBoundary)
	if unsupported == "" || noIdle == "" || unsupported == noIdle {
		t.Fatalf("downgrade notes must differ and be non-empty; unsupported=%q no-idle=%q", unsupported, noIdle)
	}
	if got := queuedNudgeDowngradeNote(target, ""); got != "" {
		t.Fatalf("note for a non-downgrade = %q, want empty", got)
	}
}

// TestManagedNudgeWakeReportsASkippedWake: the enqueue succeeded and the wake did
// not. Returning nil for both made a queued-but-unwoken nudge indistinguishable
// from a delivered one.
func TestManagedNudgeWakeReportsASkippedWake(t *testing.T) {
	var warnings bytes.Buffer
	prev := nudgeWarningWriter
	nudgeWarningWriter = &warnings
	t.Cleanup(func() { nudgeWarningWriter = prev })

	target := nudgeTarget{cityPath: t.TempDir(), alias: "worker-1", agent: config.Agent{Name: "worker"}}
	if err := requestManagedNudgeWake(target, nil); err != nil {
		t.Fatalf("requestManagedNudgeWake = %v, want nil (the enqueue still stands)", err)
	}
	if !strings.Contains(warnings.String(), "no managed wake was requested") {
		t.Fatalf("warnings = %q, want the skipped wake reported", warnings.String())
	}
	if !strings.Contains(warnings.String(), "no session store") {
		t.Fatalf("warnings = %q, want the missing precondition named", warnings.String())
	}
}

// TestRuntimeWorkerHandleNudgeFollowsProviderFamily: a city provider that
// inherits from the claude built-in takes live delivery like claude itself,
// while a provider with another family keeps reporting it as unsupported.
func TestRuntimeWorkerHandleNudgeFollowsProviderFamily(t *testing.T) {
	claudeBase := "builtin:claude"
	cfg := &config.City{Providers: map[string]config.ProviderSpec{
		"claude-sonnet": {Base: &claudeBase, Command: "claude"},
	}}
	for _, tc := range []struct {
		provider      string
		wantDelivered bool
	}{
		{provider: "claude-sonnet", wantDelivered: true},
		{provider: "claude", wantDelivered: true},
		{provider: "codex", wantDelivered: false},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			sp := runtime.NewFake()
			if err := sp.Start(context.Background(), "worker-1", runtime.Config{}); err != nil {
				t.Fatalf("Start: %v", err)
			}
			sp.WaitForIdleErrors["worker-1"] = nil
			handle, err := runtimeWorkerHandleWithConfig(t.TempDir(), nil, sp, cfg, "worker-1", tc.provider, "", nil)
			if err != nil {
				t.Fatalf("runtimeWorkerHandleWithConfig: %v", err)
			}
			result, err := handle.Nudge(context.Background(), worker.NudgeRequest{Text: "stop now", Delivery: worker.NudgeDeliveryWaitIdle})
			if err != nil {
				t.Fatalf("Nudge: %v", err)
			}
			if result.Delivered != tc.wantDelivered {
				t.Fatalf("Delivered = %v (undelivered %q), want %v", result.Delivered, result.Undelivered, tc.wantDelivered)
			}
			if !tc.wantDelivered && result.Undelivered != worker.NudgeUndeliveredProviderUnsupported {
				t.Fatalf("Undelivered = %q, want %q", result.Undelivered, worker.NudgeUndeliveredProviderUnsupported)
			}
		})
	}
}
