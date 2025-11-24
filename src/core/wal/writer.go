package wal

import (
	"os"
	"sync"
)

type WALWriter struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// NewWALWriter opens (or creates) the WAL file.
func NewWALWriter(path string) (*WALWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WALWriter{
		file: f,
		path: path,
	}, nil
}

// Append writes a WAL entry to disk.
func (w *WALWriter) Append(e *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := e.Serialize()

	_, err := w.file.Write(data)
	return err
}

// Sync forces an fsync, ensuring durability.
func (w *WALWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

// Close closes the writer.
func (w *WALWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
