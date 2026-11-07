-- Safe rollback deliberately does not resurrect the R7 global Issue
-- uniqueness restriction. Native per-(Issue, Agent) constraints persist.
SELECT 1;
