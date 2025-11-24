package wal

import (
	"encoding/binary"
	"hash/crc32"
)

// Entry represents a single WAL record.
type Entry struct {
	Payload []byte // The actual data being logged
}

// Serialize converts an entry into bytes with:
// [CRC32][LEN][PAYLOAD]
func (e *Entry) Serialize() []byte {
	length := uint32(len(e.Payload))

	buf := make([]byte, 8+len(e.Payload)) // 4 bytes CRC + 4 bytes LEN + payload

	binary.LittleEndian.PutUint32(buf[4:8], length)
	copy(buf[8:], e.Payload)

	// Compute checksum over LEN + PAYLOAD
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	return buf
}

// DeserializeEntry reads an entry from a byte slice.
// It verifies the CRC and returns the entry + bytes consumed.
func DeserializeEntry(buf []byte) (*Entry, int, error) {
	if len(buf) < 8 {
		return nil, 0, io.ErrUnexpectedEOF
	}

	crc := binary.LittleEndian.Uint32(buf[0:4])
	length := binary.LittleEndian.Uint32(buf[4:8])

	if len(buf) < int(8+length) {
		return nil, 0, io.ErrUnexpectedEOF
	}

	payload := buf[8 : 8+length]

	// Recompute checksum
	expected := crc32.ChecksumIEEE(buf[4 : 8+length])
	if crc != expected {
		return nil, 0, ErrCorruptedEntry
	}

	return &Entry{Payload: payload}, int(8 + length), nil
}
