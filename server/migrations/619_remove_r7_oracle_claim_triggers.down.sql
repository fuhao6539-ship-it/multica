-- Safety-first rollback: R8 must never reintroduce the legacy R7
-- Claim/config triggers that conflict with the native Task lifecycle.
-- Retaining the safe removal avoids recreating second-layer controls.
SELECT 1;
