package coder

import (
	"bytes"
	"testing"
)

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
		var buf bytes.Buffer
		if err := EncodeUint64(test, &buf); err != nil {
			t.Fatalf("EncodeUint64(%v) failed: %v", test, err)
		}

		t.Logf("Encoded %v to %v", test, buf.Bytes())

		if len(buf.Bytes()) != 8 {
			t.Errorf("len(EncodeUint64(%v)) = %v, want 8", test, len(buf.Bytes()))
		}

		actual, err := DecodeUint64(&buf)
		if err != nil {
			t.Fatalf("DecodeUint64(<%v>) failed: %v", test, err)
		}
		if actual != test {
			t.Errorf("DecodeUint64(<%v>) = %v, want %v", test, actual, test)
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
		var buf bytes.Buffer
		if err := EncodeInt32(test, &buf); err != nil {
			t.Fatalf("EncodeInt32(%v) failed: %v", test, err)
		}

		t.Logf("Encoded %v to %v", test, buf.Bytes())

		if len(buf.Bytes()) != 4 {
			t.Errorf("len(EncodeInt32(%v)) = %v, want 4", test, len(buf.Bytes()))
		}

		actual, err := DecodeInt32(&buf)
		if err != nil {
			t.Fatalf("DecodeInt32(<%v>) failed: %v", test, err)
		}
		if actual != test {
			t.Errorf("DecodeInt32(<%v>) = %v, want %v", test, actual, test)
		}
	}
}
