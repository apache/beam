package coderx

import "testing"

func TestEncodeDecodeUint64(t *testing.T) {
	tests := []uint64{
		0,
		1,
		127,
		128,
		1000000,
		12345678901234,
		18446744073709551615,
	}

	for _, test := range tests {
		data := EncodeUint64(test)
		if len(data) != 8 {
			t.Fatalf("len(EncodeUint64(%v)) = %v, want 8", test, len(data))
		}

		actual, length, err := DecodeUint64(data)
		if err != nil {
			t.Errorf("DecodeUint64(%v) failed for %v: %v", data, test, err)
		}
		if length != len(data) {
			t.Errorf("DecodeUint64(%v) read %v, want 8", data, length)
		}
		if actual != test {
			t.Errorf("DecodeUint64(%v) = %v, want %v", data, actual, test)
		}
	}
}

func TestEncodeDecodeInt32(t *testing.T) {
	tests := []int32{
		-2147483648,
		-100000,
		-1,
		0,
		1,
		127,
		128,
		1000000,
	}

	for _, test := range tests {
		data := EncodeInt32(test)
		if len(data) != 4 {
			t.Fatalf("len(EncodeInt32(%v)) = %v, want 4", test, len(data))
		}

		actual, length, err := DecodeInt32(data)
		if err != nil {
			t.Errorf("DecodeInt32(%v) failed for %v: %v", data, test, err)
		}
		if length != len(data) {
			t.Errorf("DecodeInt32(%v) read %v, want 4", data, length)
		}
		if actual != test {
			t.Errorf("DecodeInt32(%v) = %v, want %v", data, actual, test)
		}
	}
}
