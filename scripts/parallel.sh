#!/usr/bin/env bash
#
# Run labelled commands concurrently, print their output grouped and in order.
#
#   printf '%s\t%s\n' label command ... | scripts/parallel.sh <jobs>
#
# One line per command: a label, a tab, then a shell command.
#
# Why this exists rather than `make -j`: macOS ships GNU Make 3.81, which has no
# --output-sync, so a parallel make interleaves the output of everything it runs
# and nobody can read the failure. This keeps each command's output in its own
# buffer and prints them in the order they were given.
#
# Two properties beyond the parallelism:
#
#   * Every command runs even if an earlier one fails, and all failures are
#     reported together. The serial loops this replaces used `|| exit 1`, so a
#     contributor with three broken modules fixed them one round-trip at a time.
#   * The exit status is non-zero if any command failed.
#
# POSIX sh has no portable job-count builtin that works on the bash 3.2 macOS
# still ships as /bin/sh, so throttling counts our own background PIDs rather
# than using `wait -n`.

set -uo pipefail

jobs=${1:-4}
[ "$jobs" -ge 1 ] 2>/dev/null || jobs=4

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

labels=()
pids=()
i=0

throttle() {
  local alive=() pid
  for pid in ${pids[@]+"${pids[@]}"}; do
    kill -0 "$pid" 2>/dev/null && alive+=("$pid")
  done
  pids=(${alive[@]+"${alive[@]}"})
  if [ "${#pids[@]}" -ge "$jobs" ]; then
    wait "${pids[0]}" 2>/dev/null
    pids=(${pids[@]:1})
  fi
}

while IFS=$'\t' read -r label command; do
  [ -n "${label:-}" ] || continue
  throttle
  labels+=("$label")
  # The exit status is recorded by the subshell itself, into a file. It cannot
  # be recovered afterwards with `wait`: the bare `wait` below reaps every
  # background job, and a later `wait <pid>` on an already-reaped pid returns
  # 127 rather than what the command exited with. An earlier version of this
  # script did exactly that and then mapped 127 back to 0, which discarded
  # every status and left only the output grep -- so `go test` failures were
  # caught (they print FAIL) and lint and tidy failures were reported green.
  ( eval "$command"; echo $? >"$tmp/$i.rc" ) >"$tmp/$i.out" 2>&1 &
  pids+=("$!")
  i=$((i + 1))
done

wait

status=0
for n in $(seq 0 $((i - 1))); do
  # A missing status file means the subshell was killed before it could write
  # one, which is a failure however it happened.
  rc=$(cat "$tmp/$n.rc" 2>/dev/null || echo 1)
  case $rc in ''|*[!0-9]*) rc=1 ;; esac
  if [ "$rc" -ne 0 ]; then
    printf '==> %s  FAILED\n' "${labels[$n]}"
    status=1
  else
    printf '==> %s\n' "${labels[$n]}"
  fi
  cat "$tmp/$n.out"
done

exit "$status"
