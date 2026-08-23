package redis

import (
	"fmt"
	"strconv"
	"strings"

	dw "github.com/specialistvlad/dagworker"
)

// mapScriptErr turns a Redis error reply of the form "DWERR <CODE> <detail...>"
// back into the dagworker sentinel error it names, constructing the concrete
// typed error (*dagworker.CycleError, *dagworker.PayloadTooLargeError, ...)
// where the port promises one. err that is not a DWERR-shaped reply (a
// connection failure, a genuine Lua bug, NOSCRIPT after the fallback also
// failed, ...) is returned unwrapped: those are not part of this backend's
// error vocabulary and must not be misreported as one of it.
func mapScriptErr(err error, scope dw.Scope) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	rest, ok := strings.CutPrefix(msg, "DWERR ")
	if !ok {
		return err
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return err
	}
	code, args := fields[0], fields[1:]

	// Most codes are a sentinel plus whatever detail the script wrote. Those
	// live in a table so that mapScriptErr's branching stays proportional to
	// the cases that genuinely need code rather than growing with every new
	// error the scripts can raise.
	if sentinel, ok := scriptSentinels[code]; ok {
		if detail := strings.Join(args, " "); detail != "" {
			return fmt.Errorf("%w: %s", sentinel, detail)
		}
		return sentinel
	}

	switch code {
	case "BATCHSIZE":
		n, max := arg(args, 0), arg(args, 1)
		return &dw.InvalidArgumentError{Field: "specs", Detail: fmt.Sprintf("batch of %s exceeds the scope's limit of %s", n, max)}
	case "PAYLOADCAP":
		size, _ := strconv.Atoi(arg(args, 0))
		cap, _ := strconv.Atoi(arg(args, 1))
		return &dw.PayloadTooLargeError{Size: size, Cap: cap}
	case "CYCLE":
		return &dw.CycleError{Scope: scope, From: dw.NodeID(arg(args, 0)), To: dw.NodeID(arg(args, 1))}
	case "CYCLESELF":
		id := arg(args, 0)
		return fmt.Errorf("%w: %q depends on itself", dw.ErrCycle, id)
	default:
		return err
	}
}

// scriptSentinels maps a script's error code to the library sentinel it names.
var scriptSentinels = map[string]error{
	"NOTFOUND":      dw.ErrNotFound,
	"NOTFOUND-FROM": dw.ErrNotFound,
	"NOTFOUND-TO":   dw.ErrNotFound,
	"IDCONFLICT":    dw.ErrIDConflict,
	"SEALED":        dw.ErrScopeSealed,
	"TERMINAL":      dw.ErrAlreadyTerminal,
	"HASSUCC":       dw.ErrHasSuccessors,
	"INFLIGHT":      dw.ErrNodeInFlight,
	"LEASEMISMATCH": dw.ErrLeaseMismatch,
}

func arg(args []string, i int) string {
	if i < len(args) {
		return args[i]
	}
	return ""
}
