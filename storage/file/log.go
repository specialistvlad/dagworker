package file

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	dw "github.com/specialistvlad/dagworker"
)

// opKind names the mutation a record replays. Values are persisted, so they are
// append-only: renumbering one silently reinterprets every existing log.
type opKind uint8

const (
	opSetScopeConfig opKind = 1
	opSeal           opKind = 2
	opAddNodes       opKind = 3
	opAddEdges       opKind = 4
	opRemoveEdges    opKind = 5
	opRemoveNode     opKind = 6
	opCancel         opKind = 7
	opCancelScope    opKind = 8
	opClaim          opKind = 9
	opComplete       opKind = 10
	opExtend         opKind = 11
	opSweep          opKind = 12
)

// record is one mutation and the nondeterminism it consumed.
//
// One struct for every command rather than one type per command: gob encodes a
// struct's zero fields cheaply, and a single type keeps the decoder from
// needing a registry that must be kept in step with opKind.
type record struct {
	Op       opKind
	Scope    string
	Readings readings

	Config *dw.ScopeConfig
	Specs  []dw.NodeSpec
	Edges  []dw.Edge
	NodeID string
	IDs    []string
	Policy dw.CascadePolicy
	Claim  *dw.ClaimRequest
	Done   *dw.CompleteRequest
	Extend *dw.ExtendRequest
	Limit  int
}

// Frame layout on disk:
//
//	uint32 length | uint32 crc32(payload) | payload
//
// The checksum is what makes a torn trailing record detectable rather than a
// silent corruption. A process killed mid-append leaves a partial frame; the
// reader stops at the first frame that does not decode or does not checksum,
// truncates the file there, and carries on. That is the well-trodden path and
// it is why an fsynced append-only log is safe where a partial overwrite is
// not.
const frameHeaderSize = 8

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type log struct {
	f    *os.File
	path string
}

func openLog(path string) (*log, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600) //nolint:gosec // the path is the caller's chosen data directory
	if err != nil {
		return nil, fmt.Errorf("file: opening log %s: %w", path, err)
	}
	return &log{f: f, path: path}, nil
}

// append writes one record and fsyncs it. The fsync is the durability
// guarantee and is not optional: without it the record is in the page cache,
// which is exactly the loss window that disqualifies a periodic snapshot from
// claiming CapDurableStorage (ADR-0047).
func (l *log) append(r record) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r); err != nil {
		return fmt.Errorf("file: encoding record: %w", err)
	}
	payload := buf.Bytes()

	frame := make([]byte, frameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(payload))) //nolint:gosec // a record cannot exceed 4GiB; the payload cap is 256KiB
	binary.LittleEndian.PutUint32(frame[4:8], crc32.Checksum(payload, crcTable))
	copy(frame[frameHeaderSize:], payload)

	if _, err := l.f.Write(frame); err != nil {
		return fmt.Errorf("file: appending to log: %w", err)
	}
	return l.f.Sync()
}

// readAll returns every intact record, and truncates the file at the first one
// that is not. A torn trailing frame is expected after a crash; anything torn
// in the middle would be corruption, and truncating there loses the tail --
// which is why the caller is told how many bytes were discarded.
func (l *log) readAll() ([]record, int64, error) {
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("file: rewinding log: %w", err)
	}
	data, err := io.ReadAll(l.f)
	if err != nil {
		return nil, 0, fmt.Errorf("file: reading log: %w", err)
	}

	var out []record
	var off int
	for off+frameHeaderSize <= len(data) {
		n := int(binary.LittleEndian.Uint32(data[off : off+4]))
		sum := binary.LittleEndian.Uint32(data[off+4 : off+8])
		end := off + frameHeaderSize + n
		if n < 0 || end > len(data) || crc32.Checksum(data[off+frameHeaderSize:end], crcTable) != sum {
			break
		}
		var r record
		if err := gob.NewDecoder(bytes.NewReader(data[off+frameHeaderSize : end])).Decode(&r); err != nil {
			break
		}
		out = append(out, r)
		off = end
	}

	discarded := int64(len(data) - off)
	if discarded > 0 {
		if err := l.f.Truncate(int64(off)); err != nil {
			return nil, 0, fmt.Errorf("file: truncating a torn log at %d: %w", off, err)
		}
		if _, err := l.f.Seek(0, io.SeekEnd); err != nil {
			return nil, 0, fmt.Errorf("file: seeking to end after truncation: %w", err)
		}
	}
	return out, discarded, nil
}

func (l *log) close() error {
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

// syncDir fsyncs the directory so a newly created log file's own existence is
// durable, not just its contents. Creating a file and fsyncing only the file
// leaves the directory entry in the page cache, so a crash can lose the file
// entirely while its data was safely written.
func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // the caller's chosen data directory
	if err != nil {
		return fmt.Errorf("file: opening data directory: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) {
		return fmt.Errorf("file: syncing data directory: %w", err)
	}
	return nil
}

func logPath(dir string) string { return filepath.Join(dir, "dagworker.log") }

// reset empties the log. It is called only after a snapshot has been fsynced
// and renamed into place, so the history it discards is already represented.
func (l *log) reset() error {
	if err := l.f.Truncate(0); err != nil {
		return fmt.Errorf("file: truncating the log after a snapshot: %w", err)
	}
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("file: rewinding the truncated log: %w", err)
	}
	return l.f.Sync()
}
