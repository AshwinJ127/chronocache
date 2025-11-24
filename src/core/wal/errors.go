package wal

import "errors"

var (
	ErrCorruptedEntry = errors.New("wal: corrupted entry")
)
