package coder

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

func TestEncodeDecodeEventTime(t *testing.T) {
	tests := []struct {
		time        time.Time
		errExpected bool
	}{
		{time: time.Unix(0, 0)},
		{time: time.Unix(10, 0)},
		{time: time.Unix(1257894000, 0)},
		{time: time.Unix(0, 1257894000000000000)},
		{time: time.Time{}, errExpected: true},
	}

	for _, test := range tests {
		var buf bytes.Buffer
		err := EncodeEventTime(typex.EventTime(test.time), &buf)
		if test.errExpected {
			if err != nil {
				continue
			}
			t.Fatalf("EncodeEventTime(%v) failed: got nil error", test.time)
		}
		if err != nil {
			t.Fatalf("EncodeEventTime(%v) failed: %v", test.time, err)
		}
		t.Logf("Encoded %v to %v", test.time, buf.Bytes())

		if len(buf.Bytes()) != 8 {
			t.Errorf("EncodeEventTime(%v) = %v, want %v", test.time, len(buf.Bytes()), 8)
		}

		actual, err := DecodeEventTime(&buf)
		if err != nil {
			t.Fatalf("DecodeEventTime(<%v>) failed: %v", test.time, err)
		}
		if (time.Time)(actual) != test.time {
			t.Errorf("DecodeEventTime(<%v>) = %v, want %v", test.time, actual, test.time)
		}
	}
}
