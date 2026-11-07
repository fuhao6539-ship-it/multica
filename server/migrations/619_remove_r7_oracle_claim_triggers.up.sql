-- CCY1-2746: converge databases that previously applied the removed R7
-- migration 618. Do not create a second Claim or event scheduler.
-- This is intentionally idempotent for new installations and old R7 upgrades.
DROP TRIGGER IF EXISTS native_oracle_issue_claim_guard ON agent_task_queue;
DROP FUNCTION IF EXISTS native_oracle_issue_claim_guard();
DROP TRIGGER IF EXISTS cancel_oracle_event_tasks_on_workspace_change ON workspace;
DROP FUNCTION IF EXISTS cancel_oracle_event_tasks_on_workspace_change();
