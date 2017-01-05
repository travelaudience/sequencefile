package sequencefile

import (
	"bytes"
	"testing"
)

func TestWriter(t *testing.T) {
	var buf bytes.Buffer

	writer := NewWriter(&buf)

	err := writer.WriteHeader(Header{
		Version:        6,
		KeyClassName:   "string",
		ValueClassName: "string",
		SyncMarker:     "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
	})
	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	testData := []struct {
		Key, Value string
	}{
		{"hello", "world"},
		{"fooo", "baar"},
		{"this is a bit of a longer string", "and a bit of a longer value"},
	}

	for idx, tt := range testData {
		if err := writer.WriteRecord([]byte(tt.Key), []byte(tt.Value)); err != nil {
			t.Fatalf("writing record %d failed: %v", idx, err)
		}
	}

	reader := NewReader(&buf)
	if err := reader.ReadHeader(); err != nil {
		t.Fatalf("ReadHeader failed: %v", err)
	}

	if reader.Header.Version != 6 {
		t.Fatalf("Header version: expected 6, got %d.", reader.Header.Version)
	}
	if reader.Header.KeyClassName != "string" {
		t.Fatalf("KeyClassName: expected string, got %s.", reader.Header.KeyClassName)
	}
	if reader.Header.ValueClassName != "string" {
		t.Fatalf("ValueClassName: expected string, got %s.", reader.Header.ValueClassName)
	}

	for idx, tt := range testData {
		if !reader.Scan() {
			t.Fatalf("Scan for record %d failed: %v", idx, reader.Err())
		}

		if key := string(reader.Key()); key != tt.Key {
			t.Fatalf("%d. Key doesn't match: expected %q, got %q.", idx, tt.Key, key)
		}
		if value := string(reader.Value()); value != tt.Value {
			t.Fatalf("%d. Value doesn't match: expected %q, got %q.", idx, tt.Value, value)
		}
	}

	if reader.Scan() {
		t.Fatalf("Scan() past the last record return true: key = %q value = %q", string(reader.Key()), string(reader.Value()))
	}
}

func TestSyncMarker(t *testing.T) {
	var buf bytes.Buffer
	const iterations = 1000000

	writer := NewWriter(&buf)

	err := writer.WriteHeader(Header{
		Version:        6,
		KeyClassName:   "string",
		ValueClassName: "string",
		SyncMarker:     "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
	})
	if err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}

	for i := 0; i < iterations; i++ {
		if err := writer.WriteRecord([]byte("test key"), []byte("test value")); err != nil {
			t.Fatalf("WriteRecord failed: %v", err)
		}
	}

	reader := NewReader(&buf)
	if err := reader.ReadHeader(); err != nil {
		t.Fatalf("ReadHeader failed: %v", err)
	}
	count := 0
	for reader.Scan() {
		if string(reader.Key()) != "test key" {
			t.Fatalf("Found wrong key %q at position %d, expected \"test key\".", string(reader.Key()), count)
		}
		if string(reader.Value()) != "test value" {
			t.Fatalf("Found wrong value %q at position %d, expected \"test value\".", string(reader.Value()), count)
		}
		count++
	}

	if count != iterations {
		t.Fatalf("expected %d records, got %d.", iterations, count)
	}
}
