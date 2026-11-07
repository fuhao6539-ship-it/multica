package service

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// The original PostgreSQL Task/Claim/Start contract must fail closed, not
// throw a JSON array-expansion error, if legacy workspace settings are malformed.
func TestNativeOracleMalformedConfiguredArraysFailClosed(t *testing.T) {
	cases := []struct {
		name, key, value string
	}{
		{"agent_ids_object", "agent_ids", `{"invalid":"shape"}`},
		{"agent_ids_null", "agent_ids", "null"},
		{"agent_ids_string", "agent_ids", `"not-an-array"`},
		{"runtime_ids_object", "runtime_ids", `{"invalid":"shape"}`},
		{"runtime_ids_null", "runtime_ids", "null"},
		{"runtime_ids_number", "runtime_ids", "42"},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/start", func(t *testing.T) {
			f, taskID, _, _ := nativeEventStartFixture(t)
			f.Exec(t, `UPDATE workspace
				SET settings=jsonb_set(settings,$2::text[],$3::jsonb,true)
				WHERE id=$1`, f.WorkspaceID,
				[]string{"oracle_issue_events", tc.key}, tc.value)
			_, err := nativeEventStartTask(context.Background(), f, taskID)
			if !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("malformed %s=%s: Start returned %v; want safe denial", tc.key, tc.value, err)
			}
		})
		t.Run(tc.name+"/claim", func(t *testing.T) {
			f, taskID, agentID, runtimeID := nativeEventStartFixture(t)
			f.Exec(t, "UPDATE agent_task_queue SET status='queued',dispatched_at=NULL WHERE id=$1", taskID)
			f.Exec(t, `UPDATE workspace
				SET settings=jsonb_set(settings,$2::text[],$3::jsonb,true)
				WHERE id=$1`, f.WorkspaceID,
				[]string{"oracle_issue_events", tc.key}, tc.value)
			_, err := f.q.ClaimAgentTask(context.Background(), db.ClaimAgentTaskParams{
				AgentID: util.MustParseUUID(agentID),
				RuntimeID: util.MustParseUUID(runtimeID),
				PrepareLeaseSecs: 60,
				RuntimeStaleSecs: RuntimeClaimFreshnessSeconds,
			})
			if !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("malformed %s=%s: Claim returned %v; want safe denial", tc.key, tc.value, err)
			}
		})
	}
}

// Recovery is not a second scheduler: it uses the original native queued Task
// and cancels an unstarted event Task whose config no longer authorizes it.
func TestNativeOracleMalformedConfiguredArraysRecovery(t *testing.T) {
	f, taskID, _, _ := nativeEventStartFixture(t)
	f.Exec(t, "UPDATE agent_task_queue SET status='queued',dispatched_at=NULL WHERE id=$1", taskID)
	f.Exec(t, `UPDATE workspace
		SET settings=jsonb_set(settings,'{oracle_issue_events,agent_ids}','{}'::jsonb,true)
		WHERE id=$1`, f.WorkspaceID)
	wakeup := IssueWakeupService{Tasks: f.svc.TaskSvc}
	if err := wakeup.RecoverOrphanedNativeEventTasks(context.Background()); err != nil {
		t.Fatalf("malformed config caused native recovery SQL error: %v", err)
	}
	if got := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE id=$1 AND status='cancelled' AND started_at IS NULL", taskID); got != 1 {
		t.Fatalf("recovery cancelled %d original unstarted event Tasks; want 1", got)
	}
}
