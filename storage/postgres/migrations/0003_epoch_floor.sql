-- epoch_floor is the fencing epoch a newly created node starts from.
--
-- A node identifier can be recycled: delete one and add another with the same
-- id. If the epoch restarted at zero each time, a worker still holding a lease
-- from the deleted generation could present an epoch that happens to match the
-- new node's, and have its write accepted against a node it never claimed --
-- which is precisely the failure the fencing token exists to prevent.
--
-- Raising this past a node's epoch as it is deleted means a recycled identifier
-- never re-issues an epoch the previous generation already used.
ALTER TABLE dagw.scopes
    ADD COLUMN IF NOT EXISTS epoch_floor bigint NOT NULL DEFAULT 0;
