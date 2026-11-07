package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TestIssueWakeupConcurrentAcrossAgentsSingleFlight exercises the original
// PostgreSQL Issue/Task/Wakeup tables with two different native Agent IDs.
// Per-(Issue,Agent) uniqueness cannot prevent a duplicate across Agents;
// the Issue row lock must serialize the active-Task check and insertion.
// This is an isolated regression for CCY1-2746, not a production dispatch.
func TestIssueWakeupConcurrentAcrossAgentsSingleFlight(t *testing.T) {
	f, s, issue, firstAgent := wakeFixture(t)
	secondAgent := f.privateAgentOwnedBy(t, f.UserID, "second cross-agent wakeup")

	event := wakeCreate(t, f, s, issue, WakeupInput{
		AgentID: firstAgent, Kind: "event", Mode: "continuous",
		EventTypes: []string{"comment.created"}, Instruction: "original event",
	})
	timer := wakeCreate(t, f, s, issue, WakeupInput{
		AgentID: secondAgent, Kind: "every", Mode: "continuous",
		IntervalSeconds: 360, Instruction: "original periodic fallback",
	})
	f.Comment(t, util.UUIDToString(issue), "two agents race on one Issue")
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second' WHERE id=$1", timer.ID)

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, rule := range []db.IssueWakeup{event, timer} {
		rule := rule
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			results <- s.dispatch(ctx, rule)
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	accepted := 0
	for err := range results {
		switch {
		case err == nil:
			accepted++
		case !isLockTimeout(err):
			t.Errorf("unexpected cross-agent concurrent dispatch error: %v", err)
		}
	}
	if accepted == 0 {
		t.Fatal("neither wakeup dispatcher made progress")
	}
	const active = "('queued','deferred','dispatched','running','waiting_local_directory')"
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND status IN "+active, issue); n != 1 {
		t.Fatalf("different Agent wakeups created %d simultaneous Tasks for one Issue; want 1", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issue); n != 1 {
		t.Fatalf("different Agent wakeups inserted %d original Task rows; want 1", n)
	}

	// Whichever Agent won, the other rule must not queue another Task
	// while the first is running. The timer remains enabled for later ticks.
	f.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=now() WHERE issue_id=$1 AND status='queued'", issue)
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second' WHERE id=$1", timer.ID)
	wakeDispatch(t, s, timer)
	f.Comment(t, util.UUIDToString(issue), "new event while previous agent runs")
	wakeDispatch(t, s, event)
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND status IN "+active, issue); n != 1 {
		t.Fatalf("second Agent queued %d active Tasks while first was running; want 1", n)
	}
}
