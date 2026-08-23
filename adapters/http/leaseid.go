package httpadapter

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"

	dagworker "github.com/specialistvlad/dagworker"
)

// encodeLeaseID packs a node ID and its fencing epoch into the opaque token
// the wire calls "lease_id". The core library has no separate lease resource
// to look one up by — a [dagworker.Lease] is just (scope, node, epoch),
// addressed by the URL it was claimed under — so this adapter mints the token
// itself rather than asking storage for one it does not keep.
//
// The epoch is fixed-width and first, the node ID is everything after it:
// unambiguous to split back apart regardless of what bytes the node ID
// contains, which a delimiter-based encoding (join on ':' or '\x00') would
// not be, since NodeID's only constraint is valid UTF-8 (identifier.go) — it
// may legally contain either. Scope is not in the token because it is already
// in the URL every one of these calls is scoped under; embedding it a second
// time would just be one more place scope and URL could disagree.
func encodeLeaseID(node dagworker.NodeID, epoch uint64) string {
	buf := make([]byte, 8+len(node))
	binary.BigEndian.PutUint64(buf, epoch)
	copy(buf[8:], node)
	return base64.RawURLEncoding.EncodeToString(buf)
}

// decodeLeaseID reverses [encodeLeaseID]. A token that fails to decode is
// treated as a malformed request, not a stale one: it never round-tripped
// through this server's own claim response, so there is no lease it could
// even be stale relative to.
func decodeLeaseID(token string) (dagworker.NodeID, uint64, error) {
	buf, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(buf) < 8 {
		return "", 0, fmt.Errorf("%w: malformed lease id", dagworker.ErrInvalidArgument)
	}
	epoch := binary.BigEndian.Uint64(buf[:8])
	return dagworker.NodeID(buf[8:]), epoch, nil
}
