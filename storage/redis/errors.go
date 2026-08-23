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

	switch code {
	case "NOTFOUND", "NOTFOUND-FROM", "NOTFOUND-TO":
		return fmt.Errorf("%w: %s", dw.ErrNotFound, strings.Join(args, " "))
	case "IDCONFLICT":
		return fmt.Errorf("%w: %s", dw.ErrIDConflict, arg(args, 0))
	case "SEALED":
		return dw.ErrScopeSealed
	case "TERMINAL":
		return fmt.Errorf("%w: %s", dw.ErrAlreadyTerminal, arg(args, 0))
	case "HASSUCC":
		return fmt.Errorf("%w: %s", dw.ErrHasSuccessors, arg(args, 0))
	case "INFLIGHT":
		return fmt.Errorf("%w: %s", dw.ErrNodeInFlight, arg(args, 0))
	case "LEASEMISMATCH":
		return fmt.Errorf("%w: %s", dw.ErrLeaseMismatch, arg(args, 0))
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

func arg(args []string, i int) string {
	if i < len(args) {
		return args[i]
	}
	return ""
}
