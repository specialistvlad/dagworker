package file

// WriteSnapshotForTest writes a snapshot without truncating the log, so a test
// can arrange the one state Compact never produces: a snapshot present while
// the log still covers everything after it.
func (s *Store) WriteSnapshotForTest() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeSnapshot(s.dir, s.mem.Snapshot())
}
