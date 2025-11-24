package wal

import (
	"io"
	"os"
)

type WALReader struct {
	file *os.File
}

// NewWALReader opens a WAL file for reading.
func NewWALReader(path string) (*WALReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &WALReader{file: f}, nil
}

// Next reads the next entry from the WAL.
// Returns (entry, nil) on success.
// Returns (nil, io.EOF) when we reach the end safely.
// Returns an error for corruption or unexpected I/O failures.
func (r *WALReader) Next() (*Entry, error) {
	// Read header first: 8 bytes (CRC + LEN)
	header := make([]byte, 8)
	_, err := io.ReadFull(r.file, header)
	if err == io.EOF {
		return nil, io.EOF // clean end of file
	}
	if err != nil {
		return nil, err
	}

	// Extract payload length
	length := binary.LittleEndian.Uint32(header[4:8])

	// Read payload
	payload := make([]byte, length)
	_, err = io.ReadFull(r.file, payload)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			// This indicates a crash during write → truncated entry
			return nil, ErrCorruptedEntry
		}
		return nil, err
	}

	// Reconstruct full buffer to verify CRC
	buf := make([]byte, 8+length)
	copy(buf[0:8], header)
	copy(buf[8:], payload)

	entry, _, err := DeserializeEntry(buf)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

// Close closes the reader.
func (r *WALReader) Close() error {
	return r.file.Close()
}
