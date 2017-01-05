package sequencefile

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const syncMarkerThreshold = 64 * 1024

// Writer writes key/value pairs in the SequenceFile file format.
type Writer struct {
	w           io.Writer
	syncMarker  string
	dataWritten uint64
}

// NewWriter creates a new SequenceFile writer, writing to the provided io.Writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// WriteHeader writes a Header at the beginning of the SequenceFile. Header
// must be called first and before calling WriteRecord. In order for
// WriteHeader to complete successfully, you need to set Version, KeyClassName,
// ValueClassName, and a 16-byte SyncMarker in the Header.
func (w *Writer) WriteHeader(h Header) error {
	var buf bytes.Buffer

	if h.Version >= 10 {
		return fmt.Errorf("invalid version: %d", h.Version)
	}

	buf.WriteString("SEQ")
	buf.WriteByte(byte(h.Version))

	if err := writeString(&buf, h.KeyClassName); err != nil {
		return err
	}

	if err := writeString(&buf, h.ValueClassName); err != nil {
		return err
	}

	if _, err := buf.Write([]byte{0, 0}); err != nil { // value compression, block compression. TODO: implement support for compression
		return err
	}

	// TODO: implement support for metadata pairs
	if _, err := buf.Write([]byte{0, 0, 0, 0}); err != nil { // 0 metadata pairs
		return err
	}

	if len(h.SyncMarker) != SyncSize {
		return fmt.Errorf("SyncMarker is not %d bytes long", SyncSize)
	}

	if _, err := buf.Write([]byte(h.SyncMarker)); err != nil {
		return err
	}

	if _, err := w.w.Write(buf.Bytes()); err != nil {
		return err
	}

	w.syncMarker = h.SyncMarker

	return nil
}

// WriteRecord writes the provided key/value pair as a new record in the SequenceFile.
func (w *Writer) WriteRecord(key, value []byte) error {
	record := encodeRecord(key, value)

	if _, err := w.w.Write(record); err != nil {
		return err
	}

	w.dataWritten += uint64(4 + len(record))
	if w.dataWritten > syncMarkerThreshold {
		if err := w.writeSyncMarker(); err != nil {
			return err
		}
		w.dataWritten = 0
	}

	return nil
}

func encodeRecord(key, value []byte) []byte {
	record := make([]byte, 8, len(key)+len(value)+8)

	binary.BigEndian.PutUint32(record[:4], uint32(len(key)+len(value)))
	binary.BigEndian.PutUint32(record[4:8], uint32(len(key)))
	record = append(record, key...)
	record = append(record, value...)

	return record
}

func (w *Writer) writeSyncMarker() error {
	if w.syncMarker == "" {
		return errors.New("no sync marker found")
	}
	_, err := w.w.Write(append([]byte{0xFF, 0xFF, 0xFF, 0xFF}, []byte(w.syncMarker)...))
	return err
}

func writeString(w io.Writer, s string) error {
	if err := WriteVInt(w, int64(len(s))); err != nil {
		return err
	}

	_, err := w.Write([]byte(s))
	return err
}
