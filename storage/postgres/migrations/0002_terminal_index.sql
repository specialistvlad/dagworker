-- Drives retention collection: CollectTerminal looks for terminal nodes whose
-- last update predates a cutoff, and without this it scans the whole scope.
--
-- Partial on the terminal phase for the same reason the others are partial --
-- the index stays proportional to what the query looks at. Note the literal 4:
-- a partial index's predicate has to be provable from the query text, so the
-- query that uses it bakes the same literal in rather than binding it. See the
-- note above claimSQL in lease.go for what happens when it does not.
--
-- This is a separate file rather than an edit to 0001 because 0001 has already
-- been applied wherever it is going to be, and a migration that has run does
-- not run again.
CREATE INDEX IF NOT EXISTS nodes_terminal_idx
    ON dagw.nodes (scope, updated_at)
    WHERE phase = 4;
