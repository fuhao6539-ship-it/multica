package service

import (
    "context"
    "testing"

    "github.com/multica-ai/multica/server/internal/testutil"
    db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TestNativeIssueEventCancelledDuplicateDoesNotClearPriorFailure guards a
// non-replayable failed Task even when a newer unstarted duplicate was cancelled.
// Only a later explicit successful Task may re-open automatic event continuation.
func TestNativeIssueEventCancelledDuplicateDoesNotClearPriorFailure(t *testing.T) {
    f, wakeup, issue, agentID := wakeFixture(t)
    ctx := context.Background()
    ws := parseTestUUID(t, f.WorkspaceID)
    var runtimeID string
    f.QueryRow(t, "SELECT runtime_id::text FROM agent WHERE id=$1", agentID).Scan(&runtimeID)
    f.Exec(t, "UPDATE issue SET status='todo',assignee_type='agent',assignee_id=$2,triage_state=NULL WHERE id=$1", issue, agentID)

    failed := f.Task(t, agentID, testutil.Cols{
        "issue_id": issue, "runtime_id": runtimeID, "status": "failed",
        "completed_at": testutil.Raw("now()"),
    })
    cancelled := f.Task(t, agentID, testutil.Cols{
        "issue_id": issue, "runtime_id": runtimeID, "status": "cancelled",
        "completed_at": testutil.Raw("now()"),
    })
    // Pin the ordering: the latest Task is cancelled, but the previous
    // non-cancelled Task still records a failure requiring explicit recovery.
    f.Exec(t, "UPDATE agent_task_queue SET created_at=(SELECT created_at + interval '1 minute' FROM agent_task_queue WHERE id=$1) WHERE id=$2", failed, cancelled)

    agent, err := f.q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{
        ID: parseTestUUID(t, agentID), WorkspaceID: ws,
    })
    if err != nil {
        t.Fatal(err)
    }
    assertNotPending := func() {
        t.Helper()
        ids, err := wakeup.nativePendingIssues(ctx, ws, []nativeEventTarget{{agent: agent}})
        if err != nil {
            t.Fatal(err)
        }
        for _, id := range ids {
            if id == issue {
                t.Fatal("cancelled duplicate silently cleared preceding failed Task")
            }
        }
    }
    assertNotPending()

    // Exercise the transactional gate independently of the refill selector.
    f.Exec(t, "UPDATE agent_runtime SET provider='hermes' WHERE id=$1", runtimeID)
    f.Exec(t, `UPDATE workspace SET settings=jsonb_set(
        COALESCE(settings,'{}'::jsonb),'{oracle_issue_events}',
        jsonb_build_object('enabled',true,'actor_user_id',$2::text,
            'agent_ids',jsonb_build_array($3::text),
            'runtime_ids',jsonb_build_array($4::text)),true)
        WHERE id=$1`, f.WorkspaceID, f.UserID, agentID, runtimeID)
    _, inserted, err := wakeup.enqueueNativeIssueEvent(
        ctx, issue, agent, parseTestUUID(t, f.UserID))
    if err != nil {
        t.Fatal(err)
    }
    if inserted {
        t.Fatal("transactional enqueue replayed failed Task after later cancellation")
    }

    // A later completed Task is a distinct recovery outcome, unlike cancellation.
    recovered := f.Task(t, agentID, testutil.Cols{
        "issue_id": issue, "runtime_id": runtimeID, "status": "completed",
        "completed_at": testutil.Raw("now()"),
    })
    f.Exec(t, "UPDATE agent_task_queue SET created_at=(SELECT created_at + interval '1 minute' FROM agent_task_queue WHERE id=$1) WHERE id=$2", cancelled, recovered)
    ids, err := wakeup.nativePendingIssues(ctx, ws, []nativeEventTarget{{agent: agent}})
    if err != nil {
        t.Fatal(err)
    }
    for _, id := range ids {
        if id == issue {
            return
        }
    }
    t.Fatal("later successful Task did not re-open event continuation")
}
