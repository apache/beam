package coderx

import "testing"

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
		data := EncodeVarUint64(test.value)
		if len(data) != test.length {
			t.Fatalf("len(EncodeVarUint64(%v)) = %v, want %v", test.value, len(data), test.length)
		}

		actual, length, err := DecodeVarUint64(data)
		if err != nil {
			t.Errorf("DecodeVarUint64(%v) failed for %v: %v", data, test.value, err)
		}
		if length != len(data) {
			t.Errorf("DecodeVarUint64(%v) read %v, want %v", data, length, test.length)
		}
		if actual != test.value {
			t.Errorf("DecodeVarUint64(%v) = %v, want %v", data, actual, test.value)
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
		data := EncodeVarInt(test.value)
		if len(data) != test.length {
			t.Fatalf("len(EncodeVarInt(%v)) = %v, want %v", test.value, len(data), test.length)
		}

		actual, length, err := DecodeVarInt(data)
		if err != nil {
			t.Errorf("DecodeVarInt(%v) failed for %v: %v", data, test.value, err)
		}
		if length != len(data) {
			t.Errorf("DecodeVarInt(%v) read %v, want %v", data, length, test.length)
		}
		if actual != test.value {
			t.Errorf("DecodeVarInt(%v) = %v, want %v", data, actual, test.value)
		}
	}
}
