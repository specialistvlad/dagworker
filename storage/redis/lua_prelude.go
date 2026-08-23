package redis

// luaPrelude is prepended, verbatim, to the source of every script this
// package loads. Redis has no mechanism to share a Lua module between
// separately-EVALSHA'd scripts (that is what Redis Functions are for, and the
// task instructions ask for plain EVALSHA/EVAL with a NOSCRIPT fallback
// instead — see the package doc comment for the tradeoff), so the shared
// graph-mutation logic — settle, terminate, the edge/order maintenance, the
// backoff formula — is written exactly once here and compiled into each
// script's own text. Every script's SHA1 therefore differs (it hashes the
// prelude too), but there is exactly one copy of the logic to get right.
//
// Field and enum encodings mirror the root package's own iota values exactly
// (dagworker.Status, .Phase, .Reason, .TriggerRule, .EventKind, .CascadePolicy)
// so Go can cast a decoded integer straight to the typed enum with no lookup
// table, and this file's numeric literals below are exactly those values,
// named for readability.
const luaPrelude = `
local STATUS_NEW, STATUS_INPROGRESS, STATUS_SUCCESS, STATUS_ERROR = 0, 1, 2, 3
local PHASE_BLOCKED, PHASE_SCHEDULED, PHASE_READY, PHASE_CLAIMED, PHASE_DONE = 0, 1, 2, 3, 4
local REASON_NONE, REASON_WORKERERROR, REASON_TIMEOUT, REASON_UPSTREAMFAILED,
      REASON_SKIPPED, REASON_CANCELLED, REASON_REMOVED = 0, 1, 2, 3, 4, 5, 6
local TRIGGER_ALLSUCCESS, TRIGGER_ALLDONE, TRIGGER_NONEFAILED,
      TRIGGER_NONEFAILEDMIN1, TRIGGER_ALWAYS = 0, 1, 2, 3, 4
local EVENT_CREATED, EVENT_TRANSITION, EVENT_READY = 0, 1, 2
local CASCADE_REJECT, CASCADE_DETACH, CASCADE_FAIL = 0, 1, 2

local DEFAULT_RETRY_BASE_MS = 1000
local DEFAULT_RETRY_MAX_MS = 300000
local DEFAULT_MAX_ATTEMPTS = 3

-- PROMOTE_CAP bounds how many scheduled retries a single script call will
-- release in one pass. Redis blocks the whole server for a script's entire
-- runtime (docs/research/05 §8), so even an operation with no natural
-- per-call limit from the caller (unlike Claim's Max or Sweep's limit) must
-- still be bounded by *something*; a call that would promote more than this
-- simply leaves the remainder scheduled for the next Claim/Sweep to pick up,
-- which is safe because promotion is idempotent and re-evaluated from
-- scratch every time.
local PROMOTE_CAP = 10000

-- ---------------------------------------------------------------- keys

local function kNode(p, id) return p .. 'n:' .. id end
local function kBlob(p, id) return p .. 'b:' .. id end
local function kSucc(p, id) return p .. 's:' .. id end
local function kPred(p, id) return p .. 'p:' .. id end
local function kReady(p, kind) return p .. 'r:' .. kind end
local function kStats(p) return p .. 'stats' end
local function kIdx(p) return p .. 'idx' end
local function kKinds(p) return p .. 'kinds' end
local function kLeases(p) return p .. 'leases' end
local function kSched(p) return p .. 'sched' end
local function kCfg(p) return p .. 'cfg' end
local function kCursor(p) return p .. 'cursor' end
local function kNextOrd(p) return p .. 'nextord' end
local function kNextFifo(p) return p .. 'nextfifo' end
local function kEvents(p) return p .. 'events' end
local function kBell(p) return p .. 'bell' end

-- ---------------------------------------------------------------- clock

-- nowMs reads the server's own clock, never a client-supplied value
-- (ADR-0008). Absolute instants are kept in whole milliseconds throughout
-- this backend — never nanoseconds — because a Redis/Lua number is a double,
-- exactly representable only up to 2^53: nanoseconds since the Unix epoch
-- already exceed that today, while milliseconds since the epoch will not for
-- roughly the next 285,000 years. Durations (retry backoff, lease length)
-- comfortably fit even in nanoseconds at any realistic scale, but are also
-- carried in milliseconds here for uniformity with the instants they are
-- added to.
local function nowMs()
  local t = redis.call('TIME')
  return math.floor(tonumber(t[1])) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- ---------------------------------------------------------------- ready-set score packing

-- packScore encodes (priority DESC, fifo ASC) into one ZSET score so a single
-- ZRANGE(0,0) on a kind's ready set yields the correct next claim: higher
-- priority must sort first, and Redis ZSETs sort ascending, so priority is
-- inverted into a "rank" (0 = best) and fifo is added as a tie-breaker in the
-- remaining low bits. FIFO_SCALE=2^32 leaves priority's 16-bit range (65536
-- values) comfortably separated from a fifo counter up to ~4.29 billion,
-- summing to at most ~2.8e14 — far inside a double's exact 2^53 integer range.
local PRIORITY_BIAS = 32768
local FIFO_SCALE = 4294967296.0
local function packScore(priority, fifo)
  local rank = 65535 - (priority + PRIORITY_BIAS)
  return rank * FIFO_SCALE + fifo
end

-- ---------------------------------------------------------------- stats buckets

local function bucketField(phase, status)
  if phase == PHASE_BLOCKED then return 'Blocked'
  elseif phase == PHASE_SCHEDULED then return 'Scheduled'
  elseif phase == PHASE_READY then return 'Ready'
  elseif phase == PHASE_CLAIMED then return 'InProgress'
  elseif status == STATUS_SUCCESS then return 'Succeeded'
  else return 'Failed' end
end

local function adjustBucket(p, phase, status, delta)
  redis.call('HINCRBY', kStats(p), bucketField(phase, status), delta)
end

-- ---------------------------------------------------------------- effects / events

-- EFFECTS accumulates every effect this script call produces, in emission
-- order, exactly mirroring dagworker.Effect. It is returned verbatim as the
-- second element of every mutating script's reply; Go decodes each row back
-- into a dagworker.Effect with no further lookups, which is what lets Claim
-- and Complete report what happened without a second round trip.
local EFFECTS = {}
local function pushEffect(id, kind, from, to, reason, message, attempt, nodeKind, seq, cursor, atMs)
  table.insert(EFFECTS, {id, kind, from, to, reason, message or '', attempt, nodeKind or '', seq, cursor, atMs})
end

-- recordEvent stamps a state change with a fresh per-node Seq and a fresh
-- scope-wide Cursor, appends it to the scope's durable Stream, and records it
-- as an effect. It must run after the node's fields are already written, per
-- the same read-your-writes discipline the in-memory reference documents: an
-- event describing state a concurrent reader cannot yet observe is exactly
-- the hazard this ordering avoids.
--
-- The Stream entry ID is deliberately "<cursor>-0", not Redis's default
-- auto-generated ID: it turns "resume after cursor N" into a native,
-- O(log n) XRANGE seek instead of a linear scan, and makes "has this cursor
-- been trimmed by MAXLEN yet" a single comparison against the stream's own
-- oldest entry.
local function recordEvent(p, id, kind, fromStatus, toStatus, reason, message, attempt, nodeKind, atMs)
  local seq = redis.call('HINCRBY', kNode(p, id), 'seq', 1)
  local cursor = redis.call('INCR', kCursor(p))
  redis.call('XADD', kEvents(p), 'MAXLEN', '~', '20000', cursor .. '-0',
    'kind', kind, 'id', id, 'from', fromStatus, 'to', toStatus,
    'reason', reason, 'message', message or '', 'attempt', attempt,
    'nodeKind', nodeKind or '', 'seq', seq, 'at', atMs)
  pushEffect(id, kind, fromStatus, toStatus, reason, message, attempt, nodeKind, seq, cursor, atMs)
  return seq, cursor
end

-- ---------------------------------------------------------------- phase transitions

local function terminalMessage(reason)
  if reason == REASON_UPSTREAMFAILED then
    return 'a predecessor failed and the trigger rule can no longer be satisfied'
  elseif reason == REASON_SKIPPED then
    return 'the trigger rule can no longer be satisfied'
  elseif reason == REASON_REMOVED then
    return 'a predecessor was removed'
  elseif reason == REASON_CANCELLED then
    return 'cancelled'
  else
    return ''
  end
end

-- makeReady is the only path by which a node becomes claimable, mirroring
-- storage/memory/scope.go's function of the same name field for field.
local function makeReady(p, id, kind, priority, oldPhase, oldStatus)
  adjustBucket(p, oldPhase, oldStatus, -1)
  redis.call('ZREM', kSched(p), id)
  local fifo = redis.call('INCR', kNextFifo(p))
  local score = packScore(priority, fifo)
  redis.call('SADD', kKinds(p), kind)
  redis.call('ZADD', kReady(p, kind), score, id)
  redis.call('HSET', kNode(p, id), 'phase', PHASE_READY, 'status', STATUS_NEW, 'fifo', fifo, 'readyAt', 0)
  adjustBucket(p, PHASE_READY, STATUS_NEW, 1)
  redis.call('PUBLISH', kBell(p), '1')
end

-- makeBlocked pulls a node out of the ready set in the very same script call
-- that recorded the new edge, which is what stops a worker claiming it
-- through the gap (T6 / I4 in the contract).
local function makeBlocked(p, id, kind, oldPhase, oldStatus)
  adjustBucket(p, oldPhase, oldStatus, -1)
  redis.call('ZREM', kReady(p, kind), id)
  redis.call('ZREM', kSched(p), id)
  redis.call('HSET', kNode(p, id), 'phase', PHASE_BLOCKED, 'status', STATUS_NEW, 'readyAt', 0)
  adjustBucket(p, PHASE_BLOCKED, STATUS_NEW, 1)
end

local function scheduleRetry(p, id, kind, atMs, oldPhase, oldStatus)
  adjustBucket(p, oldPhase, oldStatus, -1)
  redis.call('ZREM', kReady(p, kind), id)
  redis.call('HSET', kNode(p, id), 'phase', PHASE_SCHEDULED, 'status', STATUS_NEW, 'readyAt', atMs)
  redis.call('ZADD', kSched(p), atMs, id)
  adjustBucket(p, PHASE_SCHEDULED, STATUS_NEW, 1)
end

-- ---------------------------------------------------------------- trigger rules
--
-- Ported field-for-field from DepCounts.Ready/.Unsatisfiable/.TerminalReason
-- in node.go: five rules, evaluated against the four incrementally-maintained
-- counters, never by scanning predecessors.

local function depsReady(trigger, unsat, succ, skip, fail)
  if trigger == TRIGGER_ALWAYS then return true end
  if unsat > 0 then return false end
  if trigger == TRIGGER_ALLSUCCESS then return fail == 0 and skip == 0
  elseif trigger == TRIGGER_ALLDONE then return true
  elseif trigger == TRIGGER_NONEFAILED then return fail == 0
  elseif trigger == TRIGGER_NONEFAILEDMIN1 then return fail == 0 and succ > 0
  else return false end
end

local function depsUnsatisfiable(trigger, unsat, succ, skip, fail)
  if trigger == TRIGGER_ALWAYS or trigger == TRIGGER_ALLDONE then return false
  elseif trigger == TRIGGER_ALLSUCCESS then return fail > 0 or skip > 0
  elseif trigger == TRIGGER_NONEFAILED then return fail > 0
  elseif trigger == TRIGGER_NONEFAILEDMIN1 then return fail > 0 or (unsat == 0 and succ == 0)
  else return false end
end

local function depsTerminalReason(fail)
  if fail > 0 then return REASON_UPSTREAMFAILED else return REASON_SKIPPED end
end

-- ---------------------------------------------------------------- edges

local function hasEdge(p, from, to)
  return redis.call('HEXISTS', kPred(p, to), from) == 1
end

-- markSatisfied records that one incoming edge resolved and updates the
-- successor's incremental tally. It reports whether anything changed, so a
-- repeated fan-out (a node reachable via two different terminating ancestors
-- in the same cascade) costs one comparison rather than a double count —
-- mirrors storage/memory/scope.go's function of the same name.
local function markSatisfied(p, succId, predId, predStatus, predReason)
  local key = kPred(p, succId)
  local val = redis.call('HGET', key, predId)
  if val == false or val == '1' then return false end
  redis.call('HSET', key, predId, '1')
  local nk = kNode(p, succId)
  local cur = tonumber(redis.call('HGET', nk, 'depsUnsatisfied'))
  if cur and cur > 0 then redis.call('HINCRBY', nk, 'depsUnsatisfied', -1) end
  if predStatus == STATUS_SUCCESS then
    redis.call('HINCRBY', nk, 'depsSucceeded', 1)
  elseif predReason == REASON_SKIPPED then
    redis.call('HINCRBY', nk, 'depsSkipped', 1)
  else
    redis.call('HINCRBY', nk, 'depsFailed', 1)
  end
  return true
end

local function linkEdge(p, from, to)
  local fn = redis.call('HMGET', kNode(p, from), 'phase', 'status', 'reason')
  local fphase, fstatus, freason = tonumber(fn[1]), tonumber(fn[2]), tonumber(fn[3])
  local satisfied = (fphase == PHASE_DONE)
  redis.call('HSET', kPred(p, to), from, satisfied and '1' or '0')
  redis.call('SADD', kSucc(p, from), to)
  local nk = kNode(p, to)
  if not satisfied then
    redis.call('HINCRBY', nk, 'depsUnsatisfied', 1)
  elseif fstatus == STATUS_SUCCESS then
    redis.call('HINCRBY', nk, 'depsSucceeded', 1)
  elseif freason == REASON_SKIPPED then
    redis.call('HINCRBY', nk, 'depsSkipped', 1)
  else
    redis.call('HINCRBY', nk, 'depsFailed', 1)
  end
end

-- unlinkEdge removes a specific predecessor->successor dependency, reporting
-- whether it existed. Per-edge state (ADR-0005) is what makes this possible
-- at all: a bare counter cannot express "undo *this* dependency" idempotently.
local function unlinkEdge(p, from, to)
  local key = kPred(p, to)
  local val = redis.call('HGET', key, from)
  if val == false then return false end
  redis.call('HDEL', key, from)
  local nk = kNode(p, to)
  if val == '0' then
    local cur = tonumber(redis.call('HGET', nk, 'depsUnsatisfied'))
    if cur and cur > 0 then redis.call('HINCRBY', nk, 'depsUnsatisfied', -1) end
  else
    local fn = redis.call('HMGET', kNode(p, from), 'status', 'reason')
    local fstatus, freason = tonumber(fn[1]), tonumber(fn[2])
    local field
    if fstatus == STATUS_SUCCESS then field = 'depsSucceeded'
    elseif freason == REASON_SKIPPED then field = 'depsSkipped'
    else field = 'depsFailed' end
    local cur = tonumber(redis.call('HGET', nk, field))
    if cur and cur > 0 then redis.call('HINCRBY', nk, field, -1) end
  end
  redis.call('SREM', kSucc(p, from), to)
  return true
end

-- ---------------------------------------------------------------- topological order
--
-- Iterative port of Pearce-Kelly (storage/memory/topo.go): bounded forward
-- and backward searches over the region between the two endpoints' current
-- ranks, cycle detection falling straight out of the forward search reaching
-- x again, and a reorder step that reuses the exact rank values the affected
-- region already occupied. BFS replaces the reference's recursive DFS only to
-- keep the traversal's stack in an explicit Lua table rather than Lua's own
-- call stack — the reachability and boundedness the algorithm relies on are
-- traversal-order-independent, so this is a mechanical, not a semantic,
-- change. See the package doc comment for why the reconstructed cycle path
-- is not carried back (CycleError.Path is left empty, which the contract
-- explicitly allows).
local function addEdgeOrder(p, x, y)
  local ordx = tonumber(redis.call('HGET', kNode(p, x), 'ord'))
  local ordy = tonumber(redis.call('HGET', kNode(p, y), 'ord'))
  if ordx < ordy then return 'fast' end

  local lb, ub = ordy, ordx

  local deltaF = { y }
  local visitedF = { [y] = true }
  local head = 1
  while head <= #deltaF do
    local n = deltaF[head]
    head = head + 1
    local succs = redis.call('SMEMBERS', kSucc(p, n))
    for i = 1, #succs do
      local w = succs[i]
      if w == x then return 'cycle' end
      if not visitedF[w] then
        local ow = tonumber(redis.call('HGET', kNode(p, w), 'ord'))
        if ow < ub then
          visitedF[w] = true
          table.insert(deltaF, w)
        end
      end
    end
  end

  local deltaB = { x }
  local visitedB = { [x] = true }
  head = 1
  while head <= #deltaB do
    local n = deltaB[head]
    head = head + 1
    local preds = redis.call('HKEYS', kPred(p, n))
    for i = 1, #preds do
      local w = preds[i]
      if not visitedB[w] then
        local ow = tonumber(redis.call('HGET', kNode(p, w), 'ord'))
        if ow > lb then
          visitedB[w] = true
          table.insert(deltaB, w)
        end
      end
    end
  end

  local function byOrd(list)
    local out = {}
    for i = 1, #list do
      out[i] = { id = list[i], ord = tonumber(redis.call('HGET', kNode(p, list[i]), 'ord')) }
    end
    table.sort(out, function(a, b) return a.ord < b.ord end)
    return out
  end
  local sB, sF = byOrd(deltaB), byOrd(deltaF)
  local pool = {}
  for i = 1, #sB do table.insert(pool, sB[i].ord) end
  for i = 1, #sF do table.insert(pool, sF[i].ord) end
  table.sort(pool)
  local idx = 1
  for i = 1, #sB do
    redis.call('HSET', kNode(p, sB[i].id), 'ord', pool[idx]); idx = idx + 1
  end
  for i = 1, #sF do
    redis.call('HSET', kNode(p, sF[i].id), 'ord', pool[idx]); idx = idx + 1
  end
  return 'reordered'
end

-- ---------------------------------------------------------------- settle / terminate
--
-- settle and terminate are mutually recursive (settle can drive a node
-- terminal, terminate's fan-out re-settles every successor it does not
-- itself terminate), so both are forward-declared before either is defined.

local terminate, settle

settle = function(p, id, nowMsVal)
  local n = redis.call('HMGET', kNode(p, id), 'phase', 'status', 'kind', 'trigger',
    'depsUnsatisfied', 'depsSucceeded', 'depsSkipped', 'depsFailed', 'priority')
  local phase = tonumber(n[1])
  if phase ~= PHASE_BLOCKED and phase ~= PHASE_READY then return end
  local status = tonumber(n[2])
  local kind = n[3]
  local trigger = tonumber(n[4])
  local unsat, succ, skip, fail = tonumber(n[5]), tonumber(n[6]), tonumber(n[7]), tonumber(n[8])
  local priority = tonumber(n[9])

  if depsUnsatisfiable(trigger, unsat, succ, skip, fail) then
    local reason = depsTerminalReason(fail)
    terminate(p, id, STATUS_ERROR, reason, terminalMessage(reason), nowMsVal)
  elseif depsReady(trigger, unsat, succ, skip, fail) then
    if phase ~= PHASE_READY then
      makeReady(p, id, kind, priority, phase, status)
      recordEvent(p, id, EVENT_READY, STATUS_NEW, STATUS_NEW, REASON_NONE, '', 0, kind, nowMsVal)
    end
  else
    if phase ~= PHASE_BLOCKED then
      makeBlocked(p, id, kind, phase, status)
    end
  end
end

-- terminate drives a node (and everything its failure/success cascades into)
-- to a final status. The walk is an explicit FIFO queue, never Lua-call
-- recursion, for the same reason storage/memory/scope.go uses a Go slice
-- queue instead of recursion: an arbitrarily deep dependency chain must not
-- be bounded by a call-stack limit. A node can be enqueued more than once
-- (two terminating predecessors of a common, not-yet-processed successor);
-- the phase==PHASE_DONE guard at dequeue time makes the second occurrence a
-- no-op, exactly mirroring the reference implementation.
terminate = function(p, root, status, reason, msg, nowMsVal)
  local queue = { root }
  local qStatus, qReason, qMsg = { [root] = status }, { [root] = reason }, { [root] = msg }
  local head = 1
  while head <= #queue do
    local id = queue[head]
    local st, rs, mg = qStatus[id], qReason[id], qMsg[id]
    head = head + 1

    local n = redis.call('HMGET', kNode(p, id), 'phase', 'status', 'kind', 'attempt')
    local phase = tonumber(n[1])
    if phase ~= nil and phase ~= PHASE_DONE then
      local oldStatus = tonumber(n[2])
      local kind = n[3]
      local attempt = tonumber(n[4])
      adjustBucket(p, phase, oldStatus, -1)
      redis.call('ZREM', kReady(p, kind), id)
      redis.call('ZREM', kSched(p), id)
      redis.call('ZREM', kLeases(p), id)
      redis.call('HSET', kNode(p, id), 'phase', PHASE_DONE, 'status', st, 'reason', rs,
        'message', mg, 'deadline', 0, 'readyAt', 0, 'updatedAt', nowMsVal)
      adjustBucket(p, PHASE_DONE, st, 1)
      recordEvent(p, id, EVENT_TRANSITION, oldStatus, st, rs, mg, attempt, kind, nowMsVal)

      local succs = redis.call('SMEMBERS', kSucc(p, id))
      for i = 1, #succs do
        local w = succs[i]
        if markSatisfied(p, w, id, st, rs) then
          local wn = redis.call('HMGET', kNode(p, w), 'phase', 'status', 'kind', 'trigger',
            'depsUnsatisfied', 'depsSucceeded', 'depsSkipped', 'depsFailed', 'priority')
          local wphase = tonumber(wn[1])
          if wphase == PHASE_BLOCKED or wphase == PHASE_READY then
            local wstatus, wkind, wtrig = tonumber(wn[2]), wn[3], tonumber(wn[4])
            local wu, ws, wk, wf = tonumber(wn[5]), tonumber(wn[6]), tonumber(wn[7]), tonumber(wn[8])
            local wprio = tonumber(wn[9])
            if depsUnsatisfiable(wtrig, wu, ws, wk, wf) then
              local wreason = depsTerminalReason(wf)
              table.insert(queue, w)
              qStatus[w], qReason[w], qMsg[w] = STATUS_ERROR, wreason, terminalMessage(wreason)
            elseif depsReady(wtrig, wu, ws, wk, wf) then
              if wphase ~= PHASE_READY then
                makeReady(p, w, wkind, wprio, wphase, wstatus)
                recordEvent(p, w, EVENT_READY, STATUS_NEW, STATUS_NEW, REASON_NONE, '', 0, wkind, nowMsVal)
              end
            end
          end
        end
      end
    end
  end
end

-- ---------------------------------------------------------------- retry backoff
--
-- backoffWindow reproduces dagworker.Backoff's deterministic window formula
-- (backoff.go) byte-for-byte; jitterMs draws the actual delay from it. This
-- is the one place this backend knowingly reimplements logic the project's
-- hard rule otherwise forbids reimplementing — see the package doc comment
-- for why it is unavoidable here: Sweep discovers which nodes to back off
-- entirely inside this atomic script, from the server's own clock, so there
-- is no opportunity to call the real Go function first the way Claim/Extend
-- call the real ClampLease before the script runs. Redis seeds Lua's
-- math.random identically across primary and replica for command-effect
-- replication, so this is deterministic-enough entropy, not true randomness
-- — irrelevant here, since jitter only needs to spread retries, never to be
-- unpredictable.
local function backoffWindow(attempt, base, maxDelay)
  if attempt == nil or attempt == 0 then attempt = 1 end
  if base == nil or base <= 0 then base = DEFAULT_RETRY_BASE_MS end
  if maxDelay == nil or maxDelay <= 0 then maxDelay = DEFAULT_RETRY_MAX_MS end
  local window = base
  local i = 1
  while i < attempt do
    if window >= maxDelay / 2 then window = maxDelay; break end
    window = window * 2
    i = i + 1
  end
  if window > maxDelay then window = maxDelay end
  return window
end

local function jitterMs(window)
  if window <= 0 then return 0 end
  return math.floor(math.random() * window)
end

-- failAttempt is the single path for every way an attempt can fail — a
-- worker's Nack and a reclaimed lease alike — mirroring
-- storage/memory/lease.go's function of the same name so the two can never
-- diverge in how they count attempts, compute backoff, or fan out.
-- Returns true when the node was rescheduled for another attempt, false when
-- it was driven terminal because attempts are exhausted.
local function failAttempt(p, id, reason, msg, nowMsVal)
  local n = redis.call('HMGET', kNode(p, id), 'kind', 'attempt', 'retryMaxAttempts', 'retryBaseMs', 'retryMaxMs')
  local kind, attempt = n[1], tonumber(n[2])
  local rma, rbd, rmd = tonumber(n[3]), tonumber(n[4]), tonumber(n[5])

  local cfg = redis.call('HMGET', kCfg(p), 'maxAttempts', 'retryBaseMs', 'retryMaxMs')
  local cfgMaxAttempts = tonumber(cfg[1]) or DEFAULT_MAX_ATTEMPTS
  local cfgBase = tonumber(cfg[2]) or DEFAULT_RETRY_BASE_MS
  local cfgMax = tonumber(cfg[3]) or DEFAULT_RETRY_MAX_MS

  local maxAttempts = cfgMaxAttempts
  if rma and rma > 0 then maxAttempts = rma end
  local base = cfgBase
  if rbd and rbd > 0 then base = rbd end
  local maxDelay = cfgMax
  if rmd and rmd > 0 then maxDelay = rmd end

  redis.call('ZREM', kLeases(p), id)
  redis.call('HSET', kNode(p, id), 'deadline', 0)

  if attempt >= maxAttempts then
    terminate(p, id, STATUS_ERROR, reason, msg, nowMsVal)
    return false
  end

  local window = backoffWindow(attempt, base, maxDelay)
  local delay = jitterMs(window)
  local phase = tonumber(redis.call('HGET', kNode(p, id), 'phase'))
  local status = tonumber(redis.call('HGET', kNode(p, id), 'status'))

  redis.call('HSET', kNode(p, id), 'reason', reason, 'message', msg, 'updatedAt', nowMsVal)
  scheduleRetry(p, id, kind, nowMsVal + delay, phase, status)
  recordEvent(p, id, EVENT_TRANSITION, status, STATUS_NEW, reason, msg, attempt, kind, nowMsVal)
  return true
end

-- reclaimExpired revokes every lease whose deadline has passed, up to limit,
-- via ZRANGEBYSCORE ... LIMIT — an index ordered on deadline, never a scan of
-- in-progress nodes, per the contract's O(m log n) sweep bound.
local function reclaimExpired(p, nowMsVal, limit)
  local expired = redis.call('ZRANGEBYSCORE', kLeases(p), '-inf', nowMsVal, 'LIMIT', 0, limit)
  for i = 1, #expired do
    failAttempt(p, expired[i], REASON_TIMEOUT,
      'the worker did not acknowledge before the lease deadline', nowMsVal)
  end
  return #expired
end

-- promoteScheduled releases every node whose retry backoff has elapsed, up to
-- PROMOTE_CAP per call (see its own comment above for why a cap exists here
-- at all). It runs on both the claim and sweep paths, so a retry becomes
-- visible without depending on a timer having fired.
local function promoteScheduled(p, nowMsVal)
  local n = 0
  while n < PROMOTE_CAP do
    local res = redis.call('ZRANGE', kSched(p), 0, 0, 'WITHSCORES')
    if #res == 0 then break end
    local id, score = res[1], tonumber(res[2])
    if score > nowMsVal then break end
    redis.call('ZREM', kSched(p), id)
    local fields = redis.call('HMGET', kNode(p, id), 'phase', 'status', 'kind', 'priority')
    local phase, status, kind, priority = tonumber(fields[1]), tonumber(fields[2]), fields[3], tonumber(fields[4])
    makeReady(p, id, kind, priority, phase, status)
    recordEvent(p, id, EVENT_READY, STATUS_NEW, STATUS_NEW, REASON_NONE, '', 0, kind, nowMsVal)
    n = n + 1
  end
end

-- ---------------------------------------------------------------- node lifecycle

local function deleteNodeKeys(p, id)
  redis.call('DEL', kNode(p, id))
  redis.call('DEL', kBlob(p, id))
  redis.call('DEL', kSucc(p, id))
  redis.call('DEL', kPred(p, id))
  redis.call('ZREM', kIdx(p), id)
end

local function createNode(p, id, kind, priority, trigger, rma, rbd, rmd, payload, labels, nowMsVal)
  local ordv = redis.call('INCR', kNextOrd(p))
  redis.call('HSET', kNode(p, id),
    'id', id, 'kind', kind, 'status', STATUS_NEW, 'phase', PHASE_BLOCKED,
    'reason', REASON_NONE, 'message', '', 'priority', priority, 'trigger', trigger,
    'retryMaxAttempts', rma, 'retryBaseMs', rbd, 'retryMaxMs', rmd,
    'attempt', 0, 'epoch', 0, 'seq', 0, 'createdAt', nowMsVal, 'updatedAt', nowMsVal,
    'ord', ordv, 'deadline', 0, 'readyAt', 0, 'fifo', 0,
    'depsUnsatisfied', 0, 'depsSucceeded', 0, 'depsSkipped', 0, 'depsFailed', 0)
  redis.call('HSET', kBlob(p, id), 'payload', payload, 'result', '', 'labels', labels)
  redis.call('ZADD', kIdx(p), 0, id)
  redis.call('HINCRBY', kStats(p), 'Total', 1)
  adjustBucket(p, PHASE_BLOCKED, STATUS_NEW, 1)
end

local function specMatches(p, id, kind, priority, trigger, rma, rbd, rmd, payload, labels)
  local n = redis.call('HMGET', kNode(p, id), 'kind', 'priority', 'trigger', 'retryMaxAttempts', 'retryBaseMs', 'retryMaxMs')
  if n[1] ~= kind then return false end
  if tonumber(n[2]) ~= priority then return false end
  if tonumber(n[3]) ~= trigger then return false end
  if tonumber(n[4]) ~= rma then return false end
  if tonumber(n[5]) ~= rbd then return false end
  if tonumber(n[6]) ~= rmd then return false end
  local b = redis.call('HMGET', kBlob(p, id), 'payload', 'labels')
  if b[1] ~= payload then return false end
  if b[2] ~= labels then return false end
  return true
end

-- rollbackBatch undoes exactly the mutations AddNodes/AddEdges journal before
-- committing (created nodes, linked edges), never anything settle() goes on
-- to do afterward. This is possible only because nothing in the abortable
-- phase ever touches the ready/scheduled sets or emits an event — a fresh
-- node is always still Blocked, an edge insertion's only side effects besides
-- the pred/succ/dep-count bookkeeping are rank reassignments, which the
-- in-memory reference also deliberately leaves unrestored (reordering a
-- surviving invariant into a different-but-still-valid ordering is
-- unobservable to any caller). Applied in reverse order, matching the
-- reference's own journal-based rollback.
local function rollbackBatch(p, createdIds, linkedEdges)
  for i = #linkedEdges, 1, -1 do
    unlinkEdge(p, linkedEdges[i][1], linkedEdges[i][2])
  end
  for i = #createdIds, 1, -1 do
    local id = createdIds[i]
    adjustBucket(p, PHASE_BLOCKED, STATUS_NEW, -1)
    redis.call('HINCRBY', kStats(p), 'Total', -1)
    deleteNodeKeys(p, id)
  end
end
`
