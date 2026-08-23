package file

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	dw "github.com/specialistvlad/dagworker"

	"github.com/specialistvlad/dagworker/storage/memory"
)

// A snapshot bounds startup. Without one the log only grows, and so does the
// time to replay it: a store that has been running for a month would take a
// month's worth of mutations to open.
//
// Filtering the log instead -- keeping only records that mention a live node --
// is tempting and wrong. Completing a node releases its successors, so dropping
// a removed node's records can leave a survivor un-readied on replay. The
// dependency structure is exactly what makes records non-self-contained, so
// compaction has to go through the state rather than through the history.
//
// The file is written whole and renamed into place. rename(2) is atomic within
// a filesystem, so a crash mid-write leaves the previous snapshot intact and
// the log still covers everything after it -- which is why the log is truncated
// only after the rename has been fsynced.
func snapshotPath(dir string) string { return filepath.Join(dir, "dagworker.snapshot") }

type snapshotFile struct {
	CRC  uint32
	Data []byte // gob of memory.Snapshot
}

// writeSnapshot serialises the state and replaces the previous snapshot.
func writeSnapshot(dir string, snap memory.Snapshot) error {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(snap); err != nil {
		return fmt.Errorf("file: encoding snapshot: %w", err)
	}
	var framed bytes.Buffer
	if err := gob.NewEncoder(&framed).Encode(snapshotFile{
		CRC:  crc32.Checksum(payload.Bytes(), crcTable),
		Data: payload.Bytes(),
	}); err != nil {
		return fmt.Errorf("file: framing snapshot: %w", err)
	}

	tmp := snapshotPath(dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec // the caller's data directory
	if err != nil {
		return fmt.Errorf("file: creating snapshot: %w", err)
	}
	if _, err := f.Write(framed.Bytes()); err != nil {
		_ = f.Close()
		return fmt.Errorf("file: writing snapshot: %w", err)
	}
	// fsync before rename: a rename that lands before the data does would
	// publish a snapshot whose contents are still in the page cache.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("file: syncing snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("file: closing snapshot: %w", err)
	}
	if err := os.Rename(tmp, snapshotPath(dir)); err != nil {
		return fmt.Errorf("file: publishing snapshot: %w", err)
	}
	return syncDir(dir)
}

// readSnapshot loads the snapshot, if there is a usable one.
//
// An unreadable or corrupt snapshot is not an error: the log is authoritative
// and replaying it from the beginning reaches the same state, just more slowly.
// Refusing to start because an optimisation is damaged would be the wrong
// trade for a backend whose whole purpose is surviving a bad shutdown.
func readSnapshot(dir string) (memory.Snapshot, bool) {
	raw, err := os.ReadFile(snapshotPath(dir)) //nolint:gosec // the caller's data directory
	if err != nil {
		return memory.Snapshot{}, false
	}
	var sf snapshotFile
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&sf); err != nil {
		return memory.Snapshot{}, false
	}
	if crc32.Checksum(sf.Data, crcTable) != sf.CRC {
		return memory.Snapshot{}, false
	}
	var snap memory.Snapshot
	if err := gob.NewDecoder(bytes.NewReader(sf.Data)).Decode(&snap); err != nil {
		return memory.Snapshot{}, false
	}
	return snap, true
}

// Compact writes a snapshot of the current state and truncates the log.
//
// It is a caller's decision rather than a timer, because only the caller knows
// what a good moment is. Calling it costs one full serialisation and blocks
// mutations for its duration; not calling it costs a longer startup.
//
// The order is snapshot, fsync, rename, fsync the directory, then truncate. A
// crash at any point leaves either the old snapshot with the whole log, or the
// new snapshot with the whole log -- both of which replay to the same state.
// Truncating first would be the one ordering that can lose data.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return dw.ErrClosed
	}
	if err := writeSnapshot(s.dir, s.mem.Snapshot()); err != nil {
		return err
	}
	return s.log.reset()
}
