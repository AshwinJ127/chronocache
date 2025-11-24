package wal

import (
	"fmt"
	"os"
	"path/filepath"
)

type SegmentManager struct {
	dir        string
	maxSize    int64
	currentIdx int
	currentFile *os.File
}

// NewSegmentManager initializes a segment manager in a directory
func NewSegmentManager(dir string, maxSize int64) (*SegmentManager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	sm := &SegmentManager{
		dir:     dir,
		maxSize: maxSize,
	}

	if err := sm.openNextSegment(); err != nil {
		return nil, err
	}

	return sm, nil
}

// openNextSegment closes the old file and opens a new one
func (sm *SegmentManager) openNextSegment() error {
	if sm.currentFile != nil {
		sm.currentFile.Close()
		sm.currentIdx++
	}

	segmentName := fmt.Sprintf("wal%05d.log", sm.currentIdx)
	path := filepath.Join(sm.dir, segmentName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	sm.currentFile = f
	return nil
}

// CurrentFile returns the file handle for writing
func (sm *SegmentManager) CurrentFile() *os.File {
	return sm.currentFile
}

// Close closes the current segment
func (sm *SegmentManager) Close() error {
	if sm.currentFile != nil {
		return sm.currentFile.Close()
	}
	return nil
}
