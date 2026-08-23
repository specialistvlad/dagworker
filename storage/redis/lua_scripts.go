package redis

import goredis "github.com/redis/go-redis/v9"

// Every script below returns {header, effects}: header is a short,
// script-specific array of scalars (documented at each call site in ops.go),
// and effects is EFFECTS from the prelude — an array of 11-element rows, one
// per dagworker.Effect produced, in emission order. A script that cannot
// proceed returns a Redis error reply of the form "DWERR <CODE> <detail...>";
// mapErr in errors.go turns CODE back into the matching dagworker sentinel.
//
// KEYS[1] is never read or written; it exists solely so every call names at
// least one key sharing the scope's {scope} hash tag, which is what a Redis
// Cluster client needs to route the command to the right shard. Every real
// key the script touches is built from ARGV[1] (the prefix) by
// concatenation, per Redis's own scripting guidance never to construct a
// key's name from anything else.
const (
	scriptAddNodes = luaPrelude + `
local p = ARGV[1]
local maxBatch = tonumber(ARGV[2])
local nSpecs = tonumber(ARGV[3])
local cursor = 4

if redis.call('HGET', kCfg(p), 'sealed') == '1' then
  return redis.error_reply('DWERR SEALED ' .. p)
end
if nSpecs > maxBatch then
  return redis.error_reply('DWERR BATCHSIZE ' .. nSpecs .. ' ' .. maxBatch)
end

local specs = {}
for i = 1, nSpecs do
  local s = {}
  s.id = ARGV[cursor]; cursor = cursor + 1
  s.kind = ARGV[cursor]; cursor = cursor + 1
  s.priority = tonumber(ARGV[cursor]); cursor = cursor + 1
  s.trigger = tonumber(ARGV[cursor]); cursor = cursor + 1
  s.rma = tonumber(ARGV[cursor]); cursor = cursor + 1
  s.rbd = tonumber(ARGV[cursor]); cursor = cursor + 1
  s.rmd = tonumber(ARGV[cursor]); cursor = cursor + 1
  s.payload = ARGV[cursor]; cursor = cursor + 1
  s.labels = ARGV[cursor]; cursor = cursor + 1
  local nDeps = tonumber(ARGV[cursor]); cursor = cursor + 1
  s.deps = {}
  for j = 1, nDeps do s.deps[j] = ARGV[cursor]; cursor = cursor + 1 end
  specs[i] = s
end

local nowMsVal = nowMs()
local createdIds, linkedEdges, freshIds = {}, {}, {}

for i = 1, nSpecs do
  local s = specs[i]
  if redis.call('EXISTS', kNode(p, s.id)) == 1 then
    if not specMatches(p, s.id, s.kind, s.priority, s.trigger, s.rma, s.rbd, s.rmd, s.payload, s.labels) then
      rollbackBatch(p, createdIds, linkedEdges)
      return redis.error_reply('DWERR IDCONFLICT ' .. s.id)
    end
  else
    createNode(p, s.id, s.kind, s.priority, s.trigger, s.rma, s.rbd, s.rmd, s.payload, s.labels, nowMsVal)
    table.insert(createdIds, s.id)
    table.insert(freshIds, s.id)
  end
end

local touched, touchedSeen = {}, {}
local function touch(id) if not touchedSeen[id] then touchedSeen[id] = true; table.insert(touched, id) end end

for i = 1, nSpecs do
  local s = specs[i]
  touch(s.id)
  for j = 1, #s.deps do
    local dep = s.deps[j]
    if redis.call('EXISTS', kNode(p, dep)) ~= 1 then
      rollbackBatch(p, createdIds, linkedEdges)
      return redis.error_reply('DWERR NOTFOUND ' .. s.id .. ' ' .. dep)
    end
    if not hasEdge(p, dep, s.id) then
      local dphase = tonumber(redis.call('HGET', kNode(p, s.id), 'phase'))
      if dphase == PHASE_DONE then
        rollbackBatch(p, createdIds, linkedEdges)
        return redis.error_reply('DWERR TERMINAL ' .. s.id)
      end
      local res = addEdgeOrder(p, dep, s.id)
      if res == 'cycle' then
        rollbackBatch(p, createdIds, linkedEdges)
        return redis.error_reply('DWERR CYCLE ' .. dep .. ' ' .. s.id)
      end
      linkEdge(p, dep, s.id)
      table.insert(linkedEdges, { dep, s.id })
    end
  end
end

for i = 1, #freshIds do
  local id = freshIds[i]
  local kind = redis.call('HGET', kNode(p, id), 'kind')
  recordEvent(p, id, EVENT_CREATED, STATUS_NEW, STATUS_NEW, REASON_NONE, '', 0, kind, nowMsVal)
end
for i = 1, #touched do settle(p, touched[i], nowMsVal) end

return { {}, EFFECTS }
`

	scriptAddEdges = luaPrelude + `
local p = ARGV[1]
local nEdges = tonumber(ARGV[2])
local cursor = 3
local edges = {}
for i = 1, nEdges do
  edges[i] = { ARGV[cursor], ARGV[cursor + 1] }
  cursor = cursor + 2
end

local nowMsVal = nowMs()
local linkedEdges = {}
local touched, touchedSeen = {}, {}
local function touch(id) if not touchedSeen[id] then touchedSeen[id] = true; table.insert(touched, id) end end

for i = 1, nEdges do
  local from, to = edges[i][1], edges[i][2]
  if redis.call('EXISTS', kNode(p, from)) ~= 1 then
    rollbackBatch(p, {}, linkedEdges)
    return redis.error_reply('DWERR NOTFOUND-FROM ' .. from)
  end
  if redis.call('EXISTS', kNode(p, to)) ~= 1 then
    rollbackBatch(p, {}, linkedEdges)
    return redis.error_reply('DWERR NOTFOUND-TO ' .. to)
  end
  if from == to then
    rollbackBatch(p, {}, linkedEdges)
    return redis.error_reply('DWERR CYCLESELF ' .. from)
  end
  if not hasEdge(p, from, to) then
    local tophase = tonumber(redis.call('HGET', kNode(p, to), 'phase'))
    if tophase == PHASE_DONE then
      rollbackBatch(p, {}, linkedEdges)
      return redis.error_reply('DWERR TERMINAL ' .. to)
    end
    local res = addEdgeOrder(p, from, to)
    if res == 'cycle' then
      rollbackBatch(p, {}, linkedEdges)
      return redis.error_reply('DWERR CYCLE ' .. from .. ' ' .. to)
    end
    linkEdge(p, from, to)
    table.insert(linkedEdges, { from, to })
    touch(to)
  end
end

for i = 1, #touched do settle(p, touched[i], nowMsVal) end
return { {}, EFFECTS }
`

	scriptRemoveEdges = luaPrelude + `
local p = ARGV[1]
local nEdges = tonumber(ARGV[2])
local cursor = 3
local nowMsVal = nowMs()
local touched, touchedSeen = {}, {}
local function touch(id) if not touchedSeen[id] then touchedSeen[id] = true; table.insert(touched, id) end end

for i = 1, nEdges do
  local from, to = ARGV[cursor], ARGV[cursor + 1]
  cursor = cursor + 2
  if redis.call('EXISTS', kNode(p, from)) ~= 1 then
    return redis.error_reply('DWERR NOTFOUND-FROM ' .. from)
  end
  if redis.call('EXISTS', kNode(p, to)) ~= 1 then
    return redis.error_reply('DWERR NOTFOUND-TO ' .. to)
  end
  if unlinkEdge(p, from, to) then touch(to) end
end
for i = 1, #touched do settle(p, touched[i], nowMsVal) end
return { {}, EFFECTS }
`

	scriptRemoveNode = luaPrelude + `
local p = ARGV[1]
local id = ARGV[2]
local policy = tonumber(ARGV[3])
local nowMsVal = nowMs()

if redis.call('EXISTS', kNode(p, id)) ~= 1 then
  return redis.error_reply('DWERR NOTFOUND ' .. id)
end
local phase = tonumber(redis.call('HGET', kNode(p, id), 'phase'))
if phase == PHASE_CLAIMED then
  return redis.error_reply('DWERR INFLIGHT ' .. id)
end

local succs = redis.call('SMEMBERS', kSucc(p, id))
if #succs > 0 then
  if policy == CASCADE_REJECT then
    return redis.error_reply('DWERR HASSUCC ' .. id .. ' ' .. #succs)
  elseif policy == CASCADE_FAIL then
    for i = 1, #succs do
      terminate(p, succs[i], STATUS_ERROR, REASON_REMOVED, terminalMessage(REASON_REMOVED), nowMsVal)
    end
  end
end

local kind = redis.call('HGET', kNode(p, id), 'kind')
for i = 1, #succs do unlinkEdge(p, id, succs[i]) end
local preds = redis.call('HKEYS', kPred(p, id))
for i = 1, #preds do unlinkEdge(p, preds[i], id) end

local status = tonumber(redis.call('HGET', kNode(p, id), 'status'))
redis.call('ZREM', kReady(p, kind), id)
redis.call('ZREM', kSched(p), id)
redis.call('ZREM', kLeases(p), id)
adjustBucket(p, phase, status, -1)
redis.call('HINCRBY', kStats(p), 'Total', -1)
deleteNodeKeys(p, id)

if policy == CASCADE_DETACH then
  for i = 1, #succs do settle(p, succs[i], nowMsVal) end
end

return { {}, EFFECTS }
`

	scriptCancelNodes = luaPrelude + `
local p = ARGV[1]
local n = tonumber(ARGV[2])
local ids = {}
for i = 1, n do ids[i] = ARGV[2 + i] end

for i = 1, n do
  if redis.call('EXISTS', kNode(p, ids[i])) ~= 1 then
    return redis.error_reply('DWERR NOTFOUND ' .. ids[i])
  end
end

local nowMsVal = nowMs()
for i = 1, n do
  local phase = tonumber(redis.call('HGET', kNode(p, ids[i]), 'phase'))
  if phase ~= PHASE_DONE then
    terminate(p, ids[i], STATUS_ERROR, REASON_CANCELLED, terminalMessage(REASON_CANCELLED), nowMsVal)
  end
end
return { {}, EFFECTS }
`

	scriptCancelScope = luaPrelude + `
local p = ARGV[1]
local nowMsVal = nowMs()
local ids = redis.call('ZRANGEBYLEX', kIdx(p), '-', '+')
for i = 1, #ids do
  local id = ids[i]
  local phase = tonumber(redis.call('HGET', kNode(p, id), 'phase'))
  if phase ~= nil and phase ~= PHASE_DONE then
    terminate(p, id, STATUS_ERROR, REASON_CANCELLED, terminalMessage(REASON_CANCELLED), nowMsVal)
  end
end
return { {}, EFFECTS }
`

	scriptClaim = luaPrelude + `
local p = ARGV[1]
local nKinds = tonumber(ARGV[2])
local kinds = {}
for i = 1, nKinds do kinds[i] = ARGV[2 + i] end
local cursor = 3 + nKinds
local maxN = tonumber(ARGV[cursor]); cursor = cursor + 1
local leaseMs = tonumber(ARGV[cursor]); cursor = cursor + 1
local sweepBatch = tonumber(ARGV[cursor]); cursor = cursor + 1
local maxInFlight = tonumber(ARGV[cursor]); cursor = cursor + 1

local nowMsVal = nowMs()
reclaimExpired(p, nowMsVal, sweepBatch)
promoteScheduled(p, nowMsVal)

local candidateKinds = kinds
if nKinds == 0 then candidateKinds = redis.call('SMEMBERS', kKinds(p)) end

local leases = {}
local granted = 0
while granted < maxN do
  if maxInFlight > 0 then
    local inprog = tonumber(redis.call('HGET', kStats(p), 'InProgress'))
    if inprog and inprog >= maxInFlight then break end
  end
  local bestKind, bestId, bestScore = nil, nil, nil
  for i = 1, #candidateKinds do
    local k = candidateKinds[i]
    local res = redis.call('ZRANGE', kReady(p, k), 0, 0, 'WITHSCORES')
    if #res > 0 then
      local id, score = res[1], tonumber(res[2])
      if bestScore == nil or score < bestScore then
        bestKind, bestId, bestScore = k, id, score
      end
    end
  end
  if bestId == nil then break end
  redis.call('ZREM', kReady(p, bestKind), bestId)

  local n = redis.call('HMGET', kNode(p, bestId), 'phase', 'status', 'epoch', 'attempt')
  local oldPhase, oldStatus = tonumber(n[1]), tonumber(n[2])
  -- epoch and attempt are separate counters. The epoch fences writes and must
  -- never go backwards for a recycled identifier; the attempt counts tries and
  -- must start again at zero for what is, to the caller, a new node.
  local epoch = (tonumber(n[3]) or 0) + 1
  local attempt = (tonumber(n[4]) or 0) + 1
  local deadline = nowMsVal + leaseMs
  adjustBucket(p, oldPhase, oldStatus, -1)
  redis.call('HSET', kNode(p, bestId), 'phase', PHASE_CLAIMED, 'status', STATUS_INPROGRESS,
    'epoch', epoch, 'attempt', attempt, 'updatedAt', nowMsVal, 'deadline', deadline)
  adjustBucket(p, PHASE_CLAIMED, STATUS_INPROGRESS, 1)
  redis.call('ZADD', kLeases(p), deadline, bestId)

  recordEvent(p, bestId, EVENT_TRANSITION, oldStatus, STATUS_INPROGRESS, REASON_NONE, '', attempt, bestKind, nowMsVal)
  local nodeFlat = redis.call('HGETALL', kNode(p, bestId))
  local blobFlat = redis.call('HGETALL', kBlob(p, bestId))
  table.insert(leases, { bestId, epoch, deadline, nodeFlat, blobFlat })
  granted = granted + 1
end

return { leases, EFFECTS }
`

	scriptComplete = luaPrelude + `
local p = ARGV[1]
local id = ARGV[2]
local presentedEpoch = tonumber(ARGV[3])
local success = tonumber(ARGV[4])
local reason = tonumber(ARGV[5])
local message = ARGV[6]
local result = ARGV[7]
local payloadCap = tonumber(ARGV[8])

if redis.call('EXISTS', kNode(p, id)) ~= 1 then
  return redis.error_reply('DWERR NOTFOUND ' .. id)
end
local n = redis.call('HMGET', kNode(p, id), 'phase', 'epoch')
local phase, epoch = tonumber(n[1]), tonumber(n[2])
if phase ~= PHASE_CLAIMED or epoch ~= presentedEpoch then
  return redis.error_reply('DWERR LEASEMISMATCH ' .. id .. ' ' .. epoch .. ' ' .. presentedEpoch)
end

local nowMsVal = nowMs()
local retrying, nextAttemptMs = 0, 0

if success == 1 then
  if #result > payloadCap then
    return redis.error_reply('DWERR PAYLOADCAP ' .. #result .. ' ' .. payloadCap)
  end
  redis.call('HSET', kBlob(p, id), 'result', result)
  terminate(p, id, STATUS_SUCCESS, REASON_NONE, '', nowMsVal)
else
  if reason == REASON_NONE then reason = REASON_WORKERERROR end
  if reason == REASON_SKIPPED then
    redis.call('ZREM', kLeases(p), id)
    redis.call('HSET', kNode(p, id), 'deadline', 0)
    terminate(p, id, STATUS_ERROR, REASON_SKIPPED, message, nowMsVal)
  else
    local wasRetrying = failAttempt(p, id, reason, message, nowMsVal)
    if wasRetrying then
      retrying = 1
      nextAttemptMs = tonumber(redis.call('HGET', kNode(p, id), 'readyAt'))
    end
  end
end

return { { retrying, nextAttemptMs }, EFFECTS }
`

	scriptExtend = luaPrelude + `
local p = ARGV[1]
local id = ARGV[2]
local presentedEpoch = tonumber(ARGV[3])
local leaseMs = tonumber(ARGV[4])

if redis.call('EXISTS', kNode(p, id)) ~= 1 then
  return redis.error_reply('DWERR NOTFOUND ' .. id)
end
local n = redis.call('HMGET', kNode(p, id), 'phase', 'epoch')
local phase, epoch = tonumber(n[1]), tonumber(n[2])
if phase ~= PHASE_CLAIMED or epoch ~= presentedEpoch then
  return redis.error_reply('DWERR LEASEMISMATCH ' .. id)
end
local nowMsVal = nowMs()
local deadline = nowMsVal + leaseMs
redis.call('HSET', kNode(p, id), 'deadline', deadline)
redis.call('ZADD', kLeases(p), deadline, id)
return { { deadline }, {} }
`

	scriptSweep = luaPrelude + `
local p = ARGV[1]
local limit = tonumber(ARGV[2])
local nowMsVal = nowMs()
local reclaimed = reclaimExpired(p, nowMsVal, limit)
promoteScheduled(p, nowMsVal)
local more = 0
if reclaimed >= limit then
  local nxt = redis.call('ZRANGEBYSCORE', kLeases(p), '-inf', nowMsVal, 'LIMIT', 0, 1)
  if #nxt > 0 then more = 1 end
end
return { { reclaimed, more }, EFFECTS }
`

	scriptGetNode = `
local p = ARGV[1]
local id = ARGV[2]
if redis.call('EXISTS', p .. 'n:' .. id) ~= 1 then
  return redis.error_reply('DWERR NOTFOUND ' .. id)
end
local n = redis.call('HGETALL', p .. 'n:' .. id)
local b = redis.call('HGETALL', p .. 'b:' .. id)
return { n, b }
`

	scriptInspect = `
local p = ARGV[1]
local id = ARGV[2]
if redis.call('EXISTS', p .. 'n:' .. id) ~= 1 then
  return redis.error_reply('DWERR NOTFOUND ' .. id)
end
local n = redis.call('HGETALL', p .. 'n:' .. id)
local pr = redis.call('HGETALL', p .. 'p:' .. id)
local su = redis.call('SMEMBERS', p .. 's:' .. id)
return { n, pr, su }
`

	scriptCollectIfEligible = `
local p = ARGV[1]
local id = ARGV[2]
local cutoffMs = tonumber(ARGV[3])
local nodeKey = p .. 'n:' .. id
if redis.call('EXISTS', nodeKey) ~= 1 then return 0 end
local n = redis.call('HMGET', nodeKey, 'phase', 'status', 'updatedAt')
local phase, status, updatedAt = tonumber(n[1]), tonumber(n[2]), tonumber(n[3])
if phase ~= 4 then return 0 end
if updatedAt > cutoffMs then return 0 end
if redis.call('SCARD', p .. 's:' .. id) > 0 then return 0 end
local field = 'Failed'
if status == 2 then field = 'Succeeded' end
redis.call('HINCRBY', p .. 'stats', field, -1)
redis.call('HINCRBY', p .. 'stats', 'Total', -1)
redis.call('DEL', nodeKey)
redis.call('DEL', p .. 'b:' .. id)
redis.call('DEL', p .. 's:' .. id)
redis.call('DEL', p .. 'p:' .. id)
redis.call('ZREM', p .. 'idx', id)
return 1
`
)

// scripts holds every redis.Script this package loads, keyed by the same
// name used in ops.go. go-redis's Script.Run does exactly what the task's
// "Load scripts with EVALSHA and fall back to EVAL on NOSCRIPT" instruction
// asks for: it tries EVALSHA against the client-computed SHA1 first and
// transparently retries with EVAL only on a NOSCRIPT reply.
type scriptSet struct {
	addNodes          *goredis.Script
	addEdges          *goredis.Script
	removeEdges       *goredis.Script
	removeNode        *goredis.Script
	cancelNodes       *goredis.Script
	cancelScope       *goredis.Script
	claim             *goredis.Script
	complete          *goredis.Script
	extend            *goredis.Script
	sweep             *goredis.Script
	getNode           *goredis.Script
	inspect           *goredis.Script
	collectIfEligible *goredis.Script
}

func newScriptSet() *scriptSet {
	return &scriptSet{
		addNodes:          goredis.NewScript(scriptAddNodes),
		addEdges:          goredis.NewScript(scriptAddEdges),
		removeEdges:       goredis.NewScript(scriptRemoveEdges),
		removeNode:        goredis.NewScript(scriptRemoveNode),
		cancelNodes:       goredis.NewScript(scriptCancelNodes),
		cancelScope:       goredis.NewScript(scriptCancelScope),
		claim:             goredis.NewScript(scriptClaim),
		complete:          goredis.NewScript(scriptComplete),
		extend:            goredis.NewScript(scriptExtend),
		sweep:             goredis.NewScript(scriptSweep),
		getNode:           goredis.NewScript(scriptGetNode),
		inspect:           goredis.NewScript(scriptInspect),
		collectIfEligible: goredis.NewScript(scriptCollectIfEligible),
	}
}
