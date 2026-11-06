package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// nativeEventStartFixture uses the original PostgreSQL test database and Task
// queue. It does not stub Claim or invent a secondary event controller.
func nativeEventStartFixture(t *testing.T) (principalFixture, string, string, string) {
	t.Helper()
	f, _, issue, agent := wakeFixture(t)
	var runtimeID string
	f.QueryRow(t, "SELECT runtime_id::text FROM agent WHERE id=$1", agent).Scan(&runtimeID)
	f.Exec(t, "UPDATE agent_runtime SET provider='hermes' WHERE id=$1", runtimeID)
	f.Exec(t, "UPDATE issue SET assignee_type='agent',assignee_id=$2,triage_state=NULL WHERE id=$1", issue, agent)
	f.Exec(t, `UPDATE workspace SET settings=jsonb_set(
		COALESCE(settings,'{}'::jsonb),'{oracle_issue_events}',
		jsonb_build_object('enabled',true,'actor_user_id',$2::text,
		  'agent_ids',jsonb_build_array($3::text),
		  'runtime_ids',jsonb_build_array($4::text)),true)
		WHERE id=$1`, f.WorkspaceID, f.UserID, agent, runtimeID)
	taskID := f.Task(t, agent, testutil.Cols{
		"issue_id": issue,
		"runtime_id": runtimeID,
		"status": "dispatched",
		"dispatched_at": testutil.Raw("now()"),
		"context": testutil.Raw(`'{"oracle_issue_event":true}'::jsonb`),
	})
	return f, taskID, agent, runtimeID
}

func nativeEventStartTask(ctx context.Context, f principalFixture, taskID string) (db.AgentTaskQueue, error) {
	return f.q.StartAgentTaskWithSupplement(ctx, db.StartAgentTaskWithSupplementParams{
		TaskID: parseStartEventUUID(taskID),
		EnableTaskSupplement: false,
		EnableProcessCleanup: false,
	})
}

// The runner needs no test-only second queue: all IDs are real native UUIDs.
func parseStartEventUUID(raw string) pgtype.UUID {
	return util.MustParseUUID(raw)
}

func TestNativeOracleIssueEventStartRevocation(t *testing.T) {
	tests := []struct {
		name   string
		revoke func(t *testing.T, f principalFixture, agent, runtime string)
		allow  bool
	}{
		{"valid original authority", nil, true},
		{"workspace disabled", func(t *testing.T, f principalFixture, _, _ string) {
			f.Exec(t, `UPDATE workspace SET settings=jsonb_set(settings,'{oracle_issue_events,enabled}','false'::jsonb) WHERE id=$1`, f.WorkspaceID)
		}, false},
		{"configured member removed", func(t *testing.T, f principalFixture, _, _ string) {
			f.Exec(t, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", f.WorkspaceID, f.UserID)
		}, false},
		{"agent invocation revoked", func(t *testing.T, f principalFixture, agent, _ string) {
			f.Exec(t, "UPDATE agent SET owner_id=NULL, permission_mode='private' WHERE id=$1", agent)
		}, false},
		{"agent no longer bound", func(t *testing.T, f principalFixture, agent, _ string) {
			f.Exec(t, "UPDATE agent SET runtime_id=NULL WHERE id=$1", agent)
		}, false},
		{"not an Oracle runtime", func(t *testing.T, f principalFixture, _, runtime string) {
			f.Exec(t, "UPDATE agent_runtime SET provider='codex' WHERE id=$1", runtime)
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, taskID, agent, runtime := nativeEventStartFixture(t)
			if tt.revoke != nil {
				tt.revoke(t, f, agent, runtime)
			}
			got, err := nativeEventStartTask(context.Background(), f, taskID)
			if tt.allow {
				if err != nil || got.Status != "running" {
					t.Fatalf("authorized event start: status=%q err=%v", got.Status, err)
				}
			} else if !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("revoked event start error=%v; want no rows", err)
			}
		})
	}
}

func TestNativeOracleIssueEventStartVsWorkspaceRevoke(t *testing.T) {
	f, taskID, _, _ := nativeEventStartFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	revokeTx, err := f.Pool.Begin(ctx)
	if err != nil { t.Fatal(err) }
	defer revokeTx.Rollback(context.Background())
	// Write but do not commit yet: Start must serialize against this
	// exact existing workspace row, then re-evaluate the revoked value.
	if _, err := revokeTx.Exec(ctx, `UPDATE workspace SET settings=jsonb_set(
		settings,'{oracle_issue_events,enabled}','false'::jsonb) WHERE id=$1`, f.WorkspaceID); err != nil {
		t.Fatal(err)
	}

	startDone := make(chan error, 1)
	go func() {
		_, e := nativeEventStartTask(ctx, f, taskID)
		startDone <- e
	}()
	select {
	case e := <-startDone:
		t.Fatalf("Start escaped uncommitted workspace revoke: %v", e)
	case <-time.After(150 * time.Millisecond):
		// Start is blocked by the original permission row's lock.
	}
	if err := revokeTx.Commit(ctx); err != nil { t.Fatal(err) }
	select {
	case err := <-startDone:
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("Start after revoke returned %v; want no rows", err)
		}
	case <-ctx.Done():
		t.Fatalf("Start did not resolve after revoke committed: %v", ctx.Err())
	}
	if got := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE id=$1 AND status='running'", taskID); got != 0 {
		t.Fatalf("revoked event started %d native Tasks", got)
	}
}


// TestNativeOracleClaimStartVsIssueClose exercises the real claim-token
 // service entrypoint, not only the generated Start SQL. A concurrent Issue
 // close holds its Issue row before it cancels the native queued Task.
func TestNativeOracleClaimStartVsIssueClose(t *testing.T) {
	f, taskID, _, runtimeID := nativeEventStartFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	var issueID pgtype.UUID
	var dispatchedAt pgtype.Timestamptz
	f.QueryRow(t,
		"SELECT issue_id, dispatched_at FROM agent_task_queue WHERE id=$1",
		taskID).Scan(&issueID, &dispatchedAt)
	claim := db.LockAgentTaskStartClaimParams{
		ID: util.MustParseUUID(taskID),
		RuntimeID: util.MustParseUUID(runtimeID),
		DispatchedAt: dispatchedAt,
	}

	tx, err := f.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	qtx := f.q.WithTx(tx)
	closed, err := qtx.UpdateIssueStatus(ctx, db.UpdateIssueStatusParams{
		ID: issueID, WorkspaceID: util.MustParseUUID(f.WorkspaceID),
		Status: "done",
	})
	if err != nil {
		t.Fatalf("close Issue: %v", err)
	}
	started := make(chan error, 1)
	go func() {
		_, startErr := f.svc.TaskSvc.StartTaskForClaim(ctx, claim)
		started <- startErr
	}()
	select {
	case startErr := <-started:
		t.Fatalf("Start returned before close committed: %v", startErr)
	case <-time.After(120*time.Millisecond):
		// Start is expected to wait on the original Issue lock.
	}
	cancelled, err := StopClosedIssueWakeups(ctx, qtx, closed)
	if err != nil {
		t.Fatalf("cancel unstarted task while Start waits: %v", err)
	}
	if len(cancelled) != 1 {
		t.Fatalf("cancelled %d tasks; want exactly one", len(cancelled))
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit Issue close: %v", err)
	}
	select {
	case startErr := <-started:
		if !errors.Is(startErr, pgx.ErrNoRows) {
			t.Fatalf("stale claim after close = %v; want no rows", startErr)
		}
	case <-ctx.Done():
		t.Fatalf("Start and close did not finish: %v", ctx.Err())
	}
	if got := f.Count(t,
		"SELECT count(*) FROM agent_task_queue WHERE id=$1 AND status='cancelled' AND started_at IS NULL",
		taskID); got != 1 {
		t.Fatalf("closed Issue left %d safely cancelled Tasks; want one", got)
	}
}


// TestNativeOracleClaimReclaimRevokedActor executes the native queue SQL
 // on a real PostgreSQL test database, never a copied authorization predicate.
func TestNativeOracleClaimReclaimRevokedActor(t *testing.T) {
	for _, tc := range []struct {
		name string
		recover bool
		batch bool
	}{
		{"claim", false, false},
		{"reclaim_one", true, false},
		{"reclaim_batch", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, taskID, agentID, runtimeID := nativeEventStartFixture(t)
			ctx := context.Background()
			rt := util.MustParseUUID(runtimeID)
			// A removed invoker must not regain authority merely because
			// their Task was already queued or dispatched.
			f.Exec(t, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
				f.WorkspaceID, f.UserID)
			if !tc.recover {
				f.Exec(t, "UPDATE agent_task_queue SET status='queued',dispatched_at=NULL WHERE id=$1", taskID)
				_, err := f.q.ClaimAgentTask(ctx, db.ClaimAgentTaskParams{
					AgentID: util.MustParseUUID(agentID), RuntimeID: rt,
					PrepareLeaseSecs: 60, RuntimeStaleSecs: RuntimeClaimFreshnessSeconds,
				})
				if !errors.Is(err, pgx.ErrNoRows) {
					t.Fatalf("revoked actor was claimable: %v", err)
				}
			} else {
				f.Exec(t, `UPDATE agent_task_queue SET
					dispatched_at=now()-interval '8 minutes',
					prepare_lease_expires_at=now()-interval '2 minutes'
					WHERE id=$1`, taskID)
				if tc.batch {
					tasks, err := f.q.ReclaimStaleDispatchedTasksForRuntimes(ctx,
						db.ReclaimStaleDispatchedTasksForRuntimesParams{
							RuntimeIds: []pgtype.UUID{rt}, PrepareLeaseSecs: 60,
							ClaimRecoverySecs: 30, RuntimeStaleSecs: RuntimeClaimFreshnessSeconds,
							MaxTasks: 3,
						})
					if err != nil || len(tasks) != 0 {
						t.Fatalf("batch reclaimed revoked actor: %d tasks, error %v", len(tasks), err)
					}
				} else {
					_, err := f.q.ReclaimStaleDispatchedTaskForRuntime(ctx,
						db.ReclaimStaleDispatchedTaskForRuntimeParams{
							RuntimeID: rt, PrepareLeaseSecs: 60,
							ClaimRecoverySecs: 30, RuntimeStaleSecs: RuntimeClaimFreshnessSeconds,
						})
					if !errors.Is(err, pgx.ErrNoRows) {
						t.Fatalf("reclaimed revoked actor: %v", err)
					}
				}
			}
			if count := f.Count(t,
				"SELECT count(*) FROM agent_task_queue WHERE id=$1 AND status='running'",
				taskID); count != 0 {
				t.Fatalf("revoked actor started %d Tasks", count)
			}
		})
	}
}
