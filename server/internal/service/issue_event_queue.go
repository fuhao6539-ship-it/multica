package service

// Opt-in native event consumption. The Issue table IS the durable backlog;
// executions are ordinary agent_task_queue rows claimed by existing Daemons.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/attribution"
	"github.com/multica-ai/multica/server/internal/issuestatus"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const nativeEventBatchSize = 32

// Use Multica's native Task terminal/retry state; do not add another
// business-level failure cooldown or eligibility rule at Issue dispatch.
// The native Multica provider registered for the existing Oracle ACP adapters.
// Both production Mac and VN Oracle runtime IDs use provider=hermes.
const nativeOracleACPProvider = "hermes"

type nativeEventConfig struct {
	Enabled     bool     `json:"enabled"`
	ActorUserID string   `json:"actor_user_id"`
	AgentIDs    []string `json:"agent_ids"`
	// One exact, pre-approved Runtime per configured Agent. A later agent
	// rebind cannot silently redirect Oracle work to a different provider.
	RuntimeIDs []string `json:"runtime_ids"`
}

type nativeEventWorkspace struct {
	id     pgtype.UUID
	config nativeEventConfig
}
type nativeEventTarget struct {
	agent db.Agent
}

func nativeEventUUID(raw string) (pgtype.UUID, error) {
	var id pgtype.UUID
	if err := id.Scan(raw); err != nil || !id.Valid {
		return pgtype.UUID{}, fmt.Errorf("invalid event queue UUID")
	}
	return id, nil
}

func parseNativeEventConfig(raw []byte) (nativeEventConfig, bool) {
	var settings struct {
		Events nativeEventConfig `json:"oracle_issue_events"`
	}
	if json.Unmarshal(raw, &settings) != nil {
		return nativeEventConfig{}, false
	}
	c := settings.Events
	if !c.Enabled || c.ActorUserID == "" || len(c.AgentIDs) == 0 ||
		len(c.RuntimeIDs) != len(c.AgentIDs) {
		return nativeEventConfig{}, false
	}
	if _, err := nativeEventUUID(c.ActorUserID); err != nil {
		return nativeEventConfig{}, false
	}
	agents := map[string]bool{}
	for idx, a := range c.AgentIDs {
		rt := c.RuntimeIDs[idx]
		if _, err := nativeEventUUID(a); err != nil || agents[a] {
			return nativeEventConfig{}, false
		}
		if _, err := nativeEventUUID(rt); err != nil {
			return nativeEventConfig{}, false
		}
		agents[a] = true
	}
	return c, true
}

func (s *IssueWakeupService) nativeEventWorkspaces(ctx context.Context) ([]nativeEventWorkspace, error) {
	tx, err := s.Tasks.TxStarter.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id,settings FROM workspace
 WHERE settings->'oracle_issue_events'->>'enabled'='true'
 -- Rotate each tick so a busy server never starves the workspace that
 -- sorts after the first 50; no extra cursor/state is stored.
 ORDER BY hashtextextended(id::text, floor(extract(epoch FROM now())/30)::bigint)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []nativeEventWorkspace
	for rows.Next() {
		var id pgtype.UUID
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		if cfg, ok := parseNativeEventConfig(raw); ok {
			out = append(out, nativeEventWorkspace{id, cfg})
		}
	}
	return out, rows.Err()
}

// Terminal categories are the only reason an Issue leaves the event pool.
func nativeEventCategoryIsTerminal(category string) bool {
	return category == "done" || category == "closed"
}

// Auto-execution is a subset of the original unfinished Issue event population.
// Respect existing assignee/workflow/Triage; blocked Issues may still be researched
// by an authorized explicit native Run but are never blindly auto-dispatched.
func nativeEventMayRun(issue db.Issue, category string, agent db.Agent) bool {
	return issue.AssigneeType.Valid && issue.AssigneeType.String == "agent" &&
		issue.AssigneeID.Valid && issue.AssigneeID == agent.ID &&
		!issue.TriageState.Valid && (category == "todo" || category == "in_progress")
}

// Reads the full unfinished-event population logically, without inserting
// a shadow event table. Active original Tasks reserve their Issue.
func (s *IssueWakeupService) nativePendingIssues(ctx context.Context, ws pgtype.UUID, targets []nativeEventTarget) ([]pgtype.UUID, error) {
	tx, err := s.Tasks.TxStarter.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// Do not change the event population: only select native-executable
	// assignments for this Task refill. Backlog/blocked remain original Issues.
	agentIDs := make([]pgtype.UUID, 0, len(targets))
	for _, target := range targets {
		agentIDs = append(agentIDs, target.agent.ID)
	}
	rows, err := tx.Query(ctx, `SELECT i.id FROM issue i WHERE i.workspace_id=$1
 AND issue_effective_status(i.workspace_id,i.status) IN ('todo','in_progress')
 AND i.assignee_type='agent' AND i.assignee_id=ANY($3::uuid[])
 AND i.triage_state IS NULL
 -- An enabled original Wakeup is itself an Issue reservation, even before
 -- its first Task exists; the existing Wakeup service owns that continuation.
 AND NOT EXISTS (SELECT 1 FROM issue_wakeup w WHERE w.issue_id=i.id AND w.enabled)
  AND NOT EXISTS (SELECT 1 FROM agent_task_queue t WHERE t.issue_id=i.id
    AND t.status IN ('queued','dispatched','running','deferred','waiting_local_directory'))
 -- Terminal Task failures are consumed by the original retry mechanism;
 -- do not mint a fresh attempt=1 Task after an exhausted/nonretryable failure.
 -- A subsequently cancelled unstarted duplicate is not explicit recovery.
 AND COALESCE((SELECT t.status FROM agent_task_queue t
   WHERE t.issue_id=i.id AND t.status<>'cancelled'
    ORDER BY t.created_at DESC,t.id DESC LIMIT 1),'')<>'failed'
 ORDER BY
   CASE i.priority WHEN 'urgent' THEN 4 WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 ELSE 0 END DESC,
   COALESCE((SELECT MAX(t.completed_at) FROM agent_task_queue t WHERE t.issue_id=i.id),to_timestamp(0)) ASC,
   i.created_at,i.id LIMIT $2::integer`, ws, int32(nativeEventBatchSize), agentIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pgtype.UUID
	for rows.Next() {
		var id pgtype.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *IssueWakeupService) nativeEventTargets(ctx context.Context, ws nativeEventWorkspace) ([]nativeEventTarget, pgtype.UUID, error) {
	actor, _ := nativeEventUUID(ws.config.ActorUserID)
	var out []nativeEventTarget
	for idx, name := range ws.config.AgentIDs {
		id, _ := nativeEventUUID(name)
		agent, err := s.Tasks.Queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: id, WorkspaceID: ws.id})
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, actor, err
		}
		if !agent.RuntimeID.Valid || agent.ArchivedAt.Valid {
			continue
		}
		expectedRuntime, _ := nativeEventUUID(ws.config.RuntimeIDs[idx])
		if agent.RuntimeID != expectedRuntime {
			continue
		}
		// An online non-Oracle Runtime must never silently join this
		// unattended Oracle-only flow.
		rt, err := s.Tasks.Queries.GetAgentRuntime(ctx, agent.RuntimeID)
		if err != nil || rt.WorkspaceID != ws.id || rt.Provider != nativeOracleACPProvider {
			continue
		}

		if s.authorize(ctx, s.Tasks.Queries, ws.id, actor, agent) != nil {
			continue
		}
		// Reuse the native Agent readiness contract, not a parallel Oracle policy.
		verdict, readyErr := AgentReadiness(ctx, s.Tasks.runtimeLookup(), agent)
		if readyErr != nil || !verdict.Ready() {
			continue
		}
		// The Oracle runtime owns availability, queue depth and concurrency.
		// No separate slot check, heartbeat threshold, or agent load balancing.
		out = append(out, nativeEventTarget{agent})
	}
	return out, actor, nil
}

// The authoritative lock/Issue status/active Task check and task insert are
// one native database transaction. Source Task identity never comes from an
// untrusted ChatGPT prompt.
func (s *IssueWakeupService) enqueueNativeIssueEvent(ctx context.Context, issueID pgtype.UUID,
	agent db.Agent, actor pgtype.UUID) (db.AgentTaskQueue, bool, error) {
	var zero db.AgentTaskQueue
	// Resolve the native provenance and runtime overlay BEFORE taking DB locks.
	// An unattended event is not a fresh human click by the config actor.
	original, err := s.Tasks.Queries.GetIssue(ctx, issueID)
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	attr := s.Tasks.attributionForIssueTask(ctx, original, pgtype.UUID{}, attribution.SourceCommentSource, pgtype.UUID{})
	attr, err = s.Tasks.applyAttributionFallback(ctx, attr, agent)
	if err != nil {
		return zero, false, err
	}
	overlay := s.Tasks.buildRuntimeMCPOverlay(ctx, attr.UserID, agent)
	attrSource, delegatedFrom, evidenceKind, evidenceRef := attributionCreateParams(attr)
	head := headShaText(s.Tasks.ResolveIssueReviewSHA(ctx, issueID))
	tx, err := s.Tasks.TxStarter.Begin(ctx)
	if err != nil {
		return zero, false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='50ms'"); err != nil {
		return zero, false, err
	}
	// Recheck workspace owner opt-in under row lock: disable/rebind must
	// either complete first or cancel our unstarted Task after this commit.
	var settings []byte
	if err = tx.QueryRow(ctx, "SELECT settings FROM workspace WHERE id=$1 FOR SHARE",
		agent.WorkspaceID).Scan(&settings); err != nil {
		return zero, false, err
	}
	cfg, enabled := parseNativeEventConfig(settings)
	if !enabled || cfg.ActorUserID != util.UUIDToString(actor) {
		return zero, false, nil
	}
	bound := false
	for idx, configuredAgent := range cfg.AgentIDs {
		if configuredAgent == util.UUIDToString(agent.ID) &&
			cfg.RuntimeIDs[idx] == util.UUIDToString(agent.RuntimeID) {
			bound = true
			break
		}
	}
	if !bound {
		return zero, false, nil
	}
	// Reuse the original member/invocation policy inside this native
	// enqueue transaction, rather than only a prior target-list read.
	q := s.Tasks.Queries.WithTx(tx)
	if err = s.authorize(ctx, q, agent.WorkspaceID, actor, agent); err != nil {
		if errors.Is(err, ErrWakeupForbidden) {
			return zero, false, nil
		}
		return zero, false, err
	}
	// Rely on the existing native owner and Issue row locks below.
	var ok bool
	if err = tx.QueryRow(ctx, "SELECT lock_task_owner_rows($1,$2,$3)",
		agent.ID, issueID, agent.RuntimeID).Scan(&ok); err != nil {
		return zero, false, err
	}
	if !ok {
		return zero, false, nil
	}
	// A concurrent Issue close/status update uses NO KEY UPDATE; hold
	// SHARE through both status recheck and Task insert, preventing stale
	// queued work from committing after a terminal change.
	var lockedIssue pgtype.UUID
	if err = tx.QueryRow(ctx, "SELECT id FROM issue WHERE id=$1 FOR SHARE",
		issueID).Scan(&lockedIssue); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return zero, false, nil
		}
		return zero, false, err
	}
	issue, err := q.GetIssue(ctx, issueID)
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	if issue.WorkspaceID != agent.WorkspaceID || issue.Revision != original.Revision {
		// Provenance, overlay and review SHA were computed for the original
		// Issue revision outside this short transaction. Never enqueue a
		// mixed-version Task; the native Tick will retry fresh.
		return zero, false, nil
	}
	// The owner/runtime/permission observed when building the MCP overlay
	// must still describe the same Agent after the owner-row locks.
	currentAgent, err := q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: agent.ID, WorkspaceID: agent.WorkspaceID})
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	if currentAgent.UpdatedAt.Valid != agent.UpdatedAt.Valid ||
		!currentAgent.UpdatedAt.Time.Equal(agent.UpdatedAt.Time) ||
		currentAgent.RuntimeID != agent.RuntimeID ||
		currentAgent.OwnerID != agent.OwnerID ||
		currentAgent.PermissionMode != agent.PermissionMode ||
		currentAgent.ArchivedAt.Valid {
		return zero, false, nil
	}
	category, err := issuestatus.CategoryWithError(ctx, q, issue.WorkspaceID, issue.Status)
	if err != nil {
		return zero, false, err
	}
	// Custom nonterminal statuses, triage and in-review remain pending events.
	if nativeEventCategoryIsTerminal(category) {
		return zero, false, nil
	}
	if !nativeEventMayRun(issue, issuestatus.Effective(ctx, q, issue.WorkspaceID, issue.Status), agent) {
		return zero, false, nil
	}
	// The original Issue SHARE lock serializes with Wakeup registration's
	// NO KEY UPDATE lock: check the already-registered reservation again in
	// the same transaction that would create a native event Task.
	var wakeupReserved bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM issue_wakeup w
	  WHERE w.issue_id=$1 AND w.enabled)`, issueID).Scan(&wakeupReserved); err != nil {
		return zero, false, err
	}
	if wakeupReserved {
		return zero, false, nil
	}
	// Native FailTask owns its retry child and budget. An exhausted or
	// nonretryable latest failure requires an explicit native rerun/correction;
	// never reset attempt=1 just because the Issue is still unfinished.
	var failedTerminal bool
	err = tx.QueryRow(ctx, `SELECT COALESCE((SELECT t.status='failed'
	  FROM agent_task_queue t WHERE t.issue_id=$1 AND t.status<>'cancelled'
	  ORDER BY t.created_at DESC,t.id DESC LIMIT 1),false)`, issueID).Scan(&failedTerminal)
	if err != nil {
		return zero, false, err
	}
	if failedTerminal {
		return zero, false, nil
	}
	// Preserve Issue-wide occupancy across reassignment and different native Task origins.
	// The per-(Issue, Agent) database uniqueness alone cannot exclude an older Agent's Task.
	var occupied bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agent_task_queue
   WHERE issue_id=$1 AND status IN
   ('queued','dispatched','running','deferred','waiting_local_directory'))`, issueID).Scan(&occupied)
	if err != nil {
		return zero, false, err
	}
	if occupied {
		return zero, false, nil
	}
	task, err := q.CreateAgentTask(ctx, db.CreateAgentTaskParams{
		ID: dbid.NewV7(), AgentID: agent.ID, RuntimeID: agent.RuntimeID,
		IssueID: issueID, Priority: priorityToInt(issue.Priority),
		TriggerSummary:   pgtype.Text{String: "Native unfinished-Issue event", Valid: true},
		OriginatorUserID: attr.UserID, AccountableUserID: attr.AccountableUserID,
		OriginatorSource: attrSource, DelegatedFromTaskID: delegatedFrom,
		TriggerEvidenceKind: evidenceKind, TriggerEvidenceRefID: evidenceRef,
		RuleVersionID: attr.RuleVersionID, HeadSha: head,
		RuntimeMcpOverlay: overlay.Overlay, RuntimeConnectedApps: overlay.ConnectedApps,
	})
	if err != nil {
		if isDuplicatePendingTaskErr(err) {
			return zero, false, nil
		}
		return zero, false, err
	}
	_, err = tx.Exec(ctx, `UPDATE agent_task_queue
  SET context=COALESCE(context,'{}'::jsonb)||'{"oracle_issue_event":true}'::jsonb
  WHERE id=$1`, task.ID)
	if err != nil {
		if isDuplicatePendingTaskErr(err) {
			return zero, false, nil
		}
		return zero, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return zero, false, err
	}
	s.Tasks.broadcastTaskEvent(ctx, protocol.EventTaskQueued, task)
	s.Tasks.NotifyTaskEnqueued(ctx, task)
	return task, true, nil
}

func nativeEventRecipient(targets []nativeEventTarget, issue db.Issue) db.Agent {
	// Routing uses existing issue ownership, not a separate slot allocator.
	if issue.AssigneeType.Valid && issue.AssigneeType.String == "agent" && issue.AssigneeID.Valid {
		for _, target := range targets {
			if target.agent.ID == issue.AssigneeID {
				return target.agent
			}
		}
	}
	return targets[0].agent
}

// A completed Task releases its Issue, but it does not grant that Issue
// exclusive use of the next Oracle slot. Select from the existing workspace
// Issue pool by native priority/age, using the same transactional enqueue gate.
func (s *IssueWakeupService) enqueueNextPendingNativeIssue(ctx context.Context,
	workspaceID pgtype.UUID, targets []nativeEventTarget, actor pgtype.UUID) (bool, error) {
	ids, err := s.nativePendingIssues(ctx, workspaceID, targets)
	if err != nil {
		return false, err
	}
	var errs []error
	for _, id := range ids {
		issue, lookupErr := s.Tasks.Queries.GetIssue(ctx, id)
		if lookupErr != nil {
			errs = append(errs, lookupErr)
			continue
		}
		_, enqueued, enqueueErr := s.enqueueNativeIssueEvent(ctx, id,
			nativeEventRecipient(targets, issue), actor)
		if enqueueErr != nil {
			errs = append(errs, enqueueErr)
			continue
		}
		if enqueued {
			return true, nil
		}
	}
	return false, errors.Join(errs...)
}

// After any original Task terminal event, refill from ALL eligible Issue
// events in this workspace, not unconditionally from the just-finished Issue.
// The current Task queue/Claim/Lease remains the only execution machinery.
func (s *TaskService) dispatchNativeIssueSuccessor(issueID pgtype.UUID) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if s == nil || s.Queries == nil || s.TxStarter == nil || !issueID.Valid {
		return
	}
	issue, err := s.Queries.GetIssue(ctx, issueID)
	if err != nil {
		return
	}
	workspace, err := s.Queries.GetWorkspace(ctx, issue.WorkspaceID)
	if err != nil {
		return
	}
	cfg, enabled := parseNativeEventConfig(workspace.Settings)
	if !enabled {
		return
	}
	wakeup := IssueWakeupService{Tasks: s}
	targets, actor, err := wakeup.nativeEventTargets(ctx,
		nativeEventWorkspace{id: issue.WorkspaceID, config: cfg})
	if err != nil || len(targets) == 0 {
		return
	}
	// Queueing remains transactional and race-safe. If no candidate can be
	// inserted, the existing leased Tick is the normal recovery fallback.
	if _, err = wakeup.enqueueNextPendingNativeIssue(ctx, issue.WorkspaceID,
		targets, actor); err != nil {
		slog.Warn("native issue continuation refill failed; leased Tick will retry",
			"source_issue_id", util.UUIDToString(issueID), "error", err)
	}
}

// Called from the existing 30s Wakeup scheduler lease, not another worker.
// A terminal Task frees an Issue for selection in the next pass.
func (s *IssueWakeupService) RefillNativeIssueEvents(ctx context.Context) error {
	// Wakeup Tick already has a 15s dispatch budget within the 45s leased Job.
	// Reserve only a bounded additional slice for event refills.
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	workspaces, err := s.nativeEventWorkspaces(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, ws := range workspaces {
		if ctx.Err() != nil {
			break
		}

		targets, actor, err := s.nativeEventTargets(ctx, ws)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if len(targets) == 0 {
			continue
		}
		ids, err := s.nativePendingIssues(ctx, ws.id, targets)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, id := range ids {
			if ctx.Err() != nil {
				break
			}

			issue, lookupErr := s.Tasks.Queries.GetIssue(ctx, id)
			if lookupErr != nil {
				errs = append(errs, lookupErr)
				continue
			}
			task, ok, err := s.enqueueNativeIssueEvent(ctx, id, nativeEventRecipient(targets, issue), actor)
			if err != nil {
				slog.Warn("native issue event enqueue failed", "issue", util.UUIDToString(id), "error", err)
				errs = append(errs, err)
			} else if ok {
				slog.Debug("native Issue event queued", "task", util.UUIDToString(task.ID))
			}
		}
	}
	return errors.Join(errs...)
}
