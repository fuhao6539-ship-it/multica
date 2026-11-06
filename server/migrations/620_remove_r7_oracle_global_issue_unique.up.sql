-- CCY1-2746: converge databases where removed R7 migration 617 ran.
-- Native per-(Issue, Agent) uniqueness remains intact. No new index is added.
-- The migration runner executes each file outside an explicit transaction;
-- DROP INDEX CONCURRENTLY avoids blocking active Task writers.
DROP INDEX CONCURRENTLY IF EXISTS idx_native_oracle_event_one_active_issue;
