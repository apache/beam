package coder

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeVarUint64(t *testing.T) {
	tests := []struct {
		value  uint64
		length int
	}{
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 2},
		{1000000, 3},
		{12345678901234, 7},
		{18446744073709551615, 10},
	}

	for _, test := range tests {
		var buf bytes.Buffer
		if err := EncodeVarUint64(test.value, &buf); err != nil {
			t.Fatalf("EncodeVarUint64(%v) failed: %v", test.value, err)
		}

		t.Logf("Encoded %v to %v", test.value, buf.Bytes())

		if len(buf.Bytes()) != test.length {
			t.Errorf("EncodeVarUint64(%v) = %v, want %v", test.value, len(buf.Bytes()), test.length)
		}

		actual, err := DecodeVarUint64(&buf)
		if err != nil {
			t.Fatalf("DecodeVarUint64(<%v>) failed: %v", test.value, err)
		}
		if actual != test.value {
			t.Errorf("DecodeVarUint64(<%v>) = %v, want %v", test.value, actual, test.value)
		}
	}
}

func TestEncodeDecodeVarInt(t *testing.T) {
	tests := []struct {
		value  int32
		length int
	}{
		{-2147483648, 5},
		{-1, 5},
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 2},
		{1000000, 3},
	}

	for _, test := range tests {
		var buf bytes.Buffer

		if err := EncodeVarInt(test.value, &buf); err != nil {
			t.Fatalf("EncodeVarInt(%v) failed: %v", test.value, err)
		}

		t.Logf("Encoded %v to %v", test.value, buf.Bytes())

		if len(buf.Bytes()) != test.length {
			t.Errorf("EncodeVarInt(%v) = %v, want %v", test.value, len(buf.Bytes()), test.length)
		}

		actual, err := DecodeVarInt(&buf)
		if err != nil {
			t.Fatalf("DecodeVarInt(<%v>) failed: %v", test.value, err)
		}
		if actual != test.value {
			t.Errorf("DecodeVarInt(<%v>) = %v, want %v", test.value, actual, test.value)
		}
	}
}
