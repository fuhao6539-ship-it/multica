package service

import (
	"context"
	"testing"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

// A person's explicit trigger is not a redundant clock tick. Keep it
// durable while an existing task owns the Issue's only active slot.
func TestIssueWakeupManualReceiptSurvivesBusyPeriodicRule(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	ctx := context.Background()
	w := wakeCreate(t, f, s, issue, WakeupInput{
		AgentID: agent, Kind: "every", Mode: "continuous",
		IntervalSeconds: 360, Instruction: "periodic fallback",
	})
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second' WHERE id=$1", w.ID)
	wakeDispatch(t, s, w)
	current, err := f.q.GetIssueWakeup(ctx, db.GetIssueWakeupParams{ID: w.ID, WorkspaceID: w.WorkspaceID})
	if err != nil || !current.LastTaskID.Valid {
		t.Fatalf("initial clock tick did not create a Task: %+v %v", current, err)
	}
	first := current.LastTaskID
	f.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1", first)

	manual, err := f.q.RecordWakeupReceipt(ctx, db.RecordWakeupReceiptParams{
		ID: dbid.NewV7(), WakeupID: w.ID, Revision: w.Revision,
		EventKey: "manual-explicit-while-busy", EventType: wakeupManualEventType,
		Payload: []byte(`{"reason":"explicit follow-up"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second' WHERE id=$1", w.ID)
	wakeDispatch(t, s, w)
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE id=$1 AND processed_at IS NULL", manual.ID); n != 1 {
		t.Fatalf("explicit manual input silently consumed by periodic dedupe: %d", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND event_type='time.due' AND processed_at IS NULL", w.ID); n != 0 {
		t.Fatalf("periodic fallback incorrectly kept %d redundant timer ticks", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND status IN ('queued','deferred','dispatched','running','waiting_local_directory')", issue); n != 1 {
		t.Fatalf("busy Issue obtained %d active tasks", n)
	}

	f.Exec(t, "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1", first)
	wakeDispatch(t, s, w)
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND status='queued'", issue); n != 1 {
		t.Fatalf("manual follow-up created %d queued successor tasks", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE id=$1 AND processed_at IS NOT NULL AND task_id IS NOT NULL", manual.ID); n != 1 {
		t.Fatalf("manual input not attached to its successor Task: %d", n)
	}
}
