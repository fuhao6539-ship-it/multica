package service

// Recovery of stale native Oracle event Tasks after an abnormal Issue close
// bypassed the existing application-level StopClosedIssueWakeups contract.
//
// This is queue hygiene, NOT another eligibility or routing predicate.
// Existing issue_effective_status and workspace owner settings remain the
// sources of truth. No additional table, scheduler, daemon or Agent is created.
import (
	"context"
	"time"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const nativeEventRecoveryBatch = 64

func (s *IssueWakeupService) RecoverOrphanedNativeEventTasks(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	tx, err := s.Tasks.TxStarter.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// SKIP LOCKED cannot steal a Task currently being claimed or started.
	// Only not-yet-started Task rows tagged by THIS feature are touched.
	rows, err := tx.Query(ctx, `WITH stale AS (
      SELECT t.id FROM agent_task_queue t
       LEFT JOIN issue i ON i.id=t.issue_id
       LEFT JOIN workspace w ON w.id=i.workspace_id
       LEFT JOIN agent a ON a.id=t.agent_id
       LEFT JOIN agent_runtime er ON er.id=t.runtime_id
       WHERE t.context->>'oracle_issue_event'='true'
         AND t.status IN ('queued','deferred','dispatched','waiting_local_directory')
         AND t.started_at IS NULL
         AND (
           i.id IS NULL
           OR issue_effective_status(i.workspace_id,i.status) NOT IN ('todo','in_progress')
           OR i.triage_state IS NOT NULL
           OR i.assignee_type IS DISTINCT FROM 'agent'
           OR i.assignee_id IS DISTINCT FROM t.agent_id
           OR a.workspace_id IS DISTINCT FROM i.workspace_id
           OR a.runtime_id IS DISTINCT FROM t.runtime_id
           OR a.kind IS DISTINCT FROM 'user'
           OR a.archived_at IS NOT NULL
           OR er.workspace_id IS DISTINCT FROM i.workspace_id
           OR er.provider IS DISTINCT FROM 'hermes'
           -- The configured original invoker must still be a member
           -- permitted by the original agent invocation policy.
           OR NOT EXISTS (
             SELECT 1 FROM member em
             WHERE em.workspace_id=i.workspace_id
               AND em.user_id::text=w.settings#>>'{oracle_issue_events,actor_user_id}'
               AND (a.owner_id=em.user_id
                 OR (a.permission_mode='public_to' AND EXISTS (
                   SELECT 1 FROM agent_invocation_target ait
                   WHERE ait.agent_id=a.id
                     AND ((ait.target_type='workspace' AND ait.target_id=i.workspace_id)
                       OR (ait.target_type='member' AND ait.target_id=em.user_id))
                 )))
           )
           OR COALESCE(w.settings->'oracle_issue_events'->>'enabled','false') <> 'true'
           OR NOT EXISTS (
             SELECT 1 FROM jsonb_array_elements_text(
               COALESCE(w.settings#>'{oracle_issue_events,agent_ids}','[]'::jsonb))
               WITH ORDINALITY AS ea(agent_id,idx)
             JOIN jsonb_array_elements_text(
               COALESCE(w.settings#>'{oracle_issue_events,runtime_ids}','[]'::jsonb))
               WITH ORDINALITY AS er(runtime_id,idx) ON ea.idx=er.idx
             WHERE ea.agent_id=t.agent_id::text AND er.runtime_id=t.runtime_id::text)
         )
      ORDER BY t.created_at,t.id
      LIMIT $1 FOR UPDATE OF t SKIP LOCKED
    )
    UPDATE agent_task_queue t
     SET status='cancelled', completed_at=now(),
         error='Native Oracle event Task no longer eligible for claim'
    FROM stale
    WHERE t.id=stale.id
    RETURNING t.id,t.issue_id,t.agent_id,t.runtime_id`, int32(nativeEventRecoveryBatch))
	if err != nil {
		return err
	}
	var cancelled []db.AgentTaskQueue
	for rows.Next() {
		var task db.AgentTaskQueue
		task.Status = "cancelled"
		if err := rows.Scan(&task.ID, &task.IssueID, &task.AgentID, &task.RuntimeID); err != nil {
			rows.Close()
			return err
		}
		cancelled = append(cancelled, task)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if err = tx.Commit(ctx); err != nil {
		return err
	}

	for _, task := range cancelled {
		s.Tasks.broadcastTaskEvent(ctx, protocol.EventTaskCancelled, task)
	}
	s.Tasks.notifyTasksFinished(cancelled)
	return nil
}
