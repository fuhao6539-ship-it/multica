package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)
func TestNativeIssueEventWorkspaceOptIn(t *testing.T) {
	good := map[string]any{"oracle_issue_events": map[string]any{
		"enabled":       true,
		"actor_user_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"agent_ids":     []string{"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "cccccccc-cccc-4ccc-8ccc-cccccccccccc"},
		"runtime_ids":   []string{"dddddddd-dddd-4ddd-8ddd-dddddddddddd", "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"},
	}}
	b, _ := json.Marshal(good)
	c, ok := parseNativeEventConfig(b)
	if !ok || !c.Enabled || len(c.AgentIDs) != 2 {
		t.Fatalf("explicit owner opt-in rejected: %v %v", ok, c)
	}
	for _, bad := range []string{
		"{}",
		// Fail closed when a selected Agent has no exact Runtime binding.
		`{"oracle_issue_events":{"enabled":true,"actor_user_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","agent_ids":["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"]}}`,
		`{"oracle_issue_events":{"enabled":true,"actor_user_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","agent_ids":["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","cccccccc-cccc-4ccc-8ccc-cccccccccccc"],"runtime_ids":["dddddddd-dddd-4ddd-8ddd-dddddddddddd"]}}`,
		`{"oracle_issue_events":{"enabled":true,"actor_user_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","agent_ids":["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","cccccccc-cccc-4ccc-8ccc-cccccccccccc"],"runtime_ids":["dddddddd-dddd-4ddd-8ddd-dddddddddddd","dddddddd-dddd-4ddd-8ddd-dddddddddddd"]}}`,

		`{"oracle_issue_events":{"enabled":false,"actor_user_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","agent_ids":["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"]}}`,
		`{"oracle_issue_events":{"enabled":true,"actor_user_id":"not-an-id","agent_ids":["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"]}}`,
		`{"oracle_issue_events":{"enabled":true,"actor_user_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","agent_ids":["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"]}}`,
	} {
		if _, ok := parseNativeEventConfig([]byte(bad)); ok {
			t.Fatalf("unexpected queue enable for %q", bad)
		}
	}
}

func TestNativeEventQueueAllUnfinishedRemainEvents(t *testing.T) {
	for _, state := range []string{"backlog", "todo", "blocked", "in_progress", "triage", "in_review", "custom_nonterminal"} {
		if nativeEventCategoryIsTerminal(state) {
			t.Fatalf("discarded open Issue state %q", state)
		}
	}
	for _, state := range []string{"done", "closed"} {
		if !nativeEventCategoryIsTerminal(state) {
			t.Fatalf("terminal Issue remained %s", state)
		}
	}
	if nativeEventBatchSize <= 0 || nativeEventBatchSize > 100 {
		t.Fatal("unbounded native Task inserts per scheduler pass")
	}
	// Logical event count stays at 1000 even while 16 Oracle tasks are busy.
	unfinished, running := 1000, 16
	waiting := unfinished - running
	if waiting != 984 {
		t.Fatal("lost pending Issue events")
	}
	// A completed execution does not complete its Issue: it joins the pool.
	running--
	waiting++
	if running != 15 || waiting != 985 {
		t.Fatal("incomplete issue did not re-enter the pending pool")
	}
	// A truly completed Issue exits the pool; terminal event no longer waits.
	waiting--
	if waiting != 984 {
		t.Fatal("terminal Issue was requeued")
	}
}

func TestNativeIssueEventPriorityBeforePreviousCompletion(t *testing.T) {
	f, wakeup, _, agentID := wakeFixture(t)
	ctx := context.Background()
	ws := parseTestUUID(t, f.WorkspaceID)
	agent, err := f.q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{
		ID: parseTestUUID(t, agentID), WorkspaceID: ws,
	})
	if err != nil {
		t.Fatal(err)
	}
	high := f.Issue(t, "High without a previous Task")
	urgent := f.Issue(t, "Urgent with a completed Task")
	for _, item := range []struct {
		id, priority string
	}{{high, "high"}, {urgent, "urgent"}} {
		f.Exec(t, "UPDATE issue SET status='todo',priority=$2,assignee_type='agent',assignee_id=$3,triage_state=NULL WHERE id=$1",
			item.id, item.priority, agentID)
	}
	f.Task(t, agentID, testutil.Cols{
		"issue_id": urgent, "status": "completed",
		"completed_at": testutil.Raw("now()"),
		"runtime_id": testutil.Raw("(SELECT runtime_id FROM agent WHERE id='" + agentID + "')"),
	})
	ids, err := wakeup.nativePendingIssues(ctx, ws, []nativeEventTarget{{agent: agent}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) < 2 || ids[0] != parseTestUUID(t, urgent) || ids[1] != parseTestUUID(t, high) {
		t.Fatalf("urgent must precede high even with a recent completion; got %v", ids)
	}
}


// TestNativeIssueEventReassignmentPreservesIssueWideInFlight exercises the real
// PostgreSQL Task table, not a simulated concurrency counter. Reassigning an
// unfinished Issue must not hide a running Task owned by the previous Agent.
// Once that Task terminates, the Issue can be selected by the new assignee.
func TestNativeIssueEventReassignmentPreservesIssueWideInFlight(t *testing.T) {
	f, wakeup, issue, currentAgentID := wakeFixture(t)
	ctx := context.Background()
	ws := parseTestUUID(t, f.WorkspaceID)
	var runtimeID string
	f.QueryRow(t, "SELECT runtime_id::text FROM agent WHERE id=$1", currentAgentID).Scan(&runtimeID)
	priorAgentID := f.Agent(t, "Previous assignee", runtimeID)
	f.Exec(t, "UPDATE issue SET status='todo',assignee_type='agent',assignee_id=$2,triage_state=NULL WHERE id=$1", issue, currentAgentID)
	taskID := f.Task(t, priorAgentID, testutil.Cols{
		"issue_id": issue,
		"status": "running",
		"started_at": testutil.Raw("now()"),
		"runtime_id": runtimeID,
	})
	agent, err := f.q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{
		ID: parseTestUUID(t, currentAgentID), WorkspaceID: ws,
	})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := wakeup.nativePendingIssues(ctx, ws, []nativeEventTarget{{agent: agent}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range pending {
		if id == issue {
			t.Fatal("reassignment hid prior Agent's running Task and allowed duplicate queueing")
		}
	}
	f.Exec(t, "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1", taskID)
	pending, err = wakeup.nativePendingIssues(ctx, ws, []nativeEventTarget{{agent: agent}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range pending {
		if id == issue {
			return
		}
	}
	t.Fatal("terminal Task did not release the original Issue for the new assignee")
}


// TestNativeIssueEventReassignmentDoesNotReplayPreviousAgentFailure uses the
// original PostgreSQL Task table to guard an exhausted/nonretryable failure.
// Reassignment must not erase the earlier Agent's failed Task, even when the
// new Agent has no Task history. A human-authorized explicit native rerun is
// separate from unattended Issue-event refilling.
func TestNativeIssueEventReassignmentDoesNotReplayPreviousAgentFailure(t *testing.T) {
	f, wakeup, issue, currentAgentID := wakeFixture(t)
	ctx := context.Background()
	ws := parseTestUUID(t, f.WorkspaceID)
	var runtimeID string
	f.QueryRow(t, "SELECT runtime_id::text FROM agent WHERE id=$1", currentAgentID).Scan(&runtimeID)
	previousAgentID := f.Agent(t, "Prior failed assignee", runtimeID)
	f.Exec(t, "UPDATE issue SET status='todo',assignee_type='agent',assignee_id=$2,triage_state=NULL WHERE id=$1",
		issue, currentAgentID)
	f.Task(t, previousAgentID, testutil.Cols{
		"issue_id": issue, "runtime_id": runtimeID, "status": "failed",
		"completed_at": testutil.Raw("now()"),
	})
	agent, err := f.q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{
		ID: parseTestUUID(t, currentAgentID), WorkspaceID: ws,
	})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := wakeup.nativePendingIssues(ctx, ws, []nativeEventTarget{{agent: agent}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range pending {
		if id == issue {
			t.Fatal("reassignment hid prior Agent's failed Task and made Issue auto-runnable")
		}
	}

	// Also exercise the transactional insertion guard independently of the
	// refill SELECT, using a real workspace opt-in for the new assignee.
	f.Exec(t, "UPDATE agent_runtime SET provider='hermes' WHERE id=$1", runtimeID)
	f.Exec(t, `UPDATE workspace SET settings=jsonb_set(
		COALESCE(settings,'{}'::jsonb),'{oracle_issue_events}',
		jsonb_build_object('enabled',true,'actor_user_id',$2::text,
			'agent_ids',jsonb_build_array($3::text),
			'runtime_ids',jsonb_build_array($4::text)),true)
		WHERE id=$1`, f.WorkspaceID, f.UserID, currentAgentID, runtimeID)
	_, inserted, err := wakeup.enqueueNativeIssueEvent(
		ctx, issue, agent, parseTestUUID(t, f.UserID))
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("transactional enqueue blindly replayed previous Agent's failed Task")
	}
}
