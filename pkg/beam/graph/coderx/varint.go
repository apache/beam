package coderx

import (
	"fmt"
)

// Variable-length encoding for integers.
//
// Takes between 1 and 10 bytes. Less efficient for negative or large numbers.
// All negative ints are encoded using 5 bytes, longs take 10 bytes. We use
// uint64 (over int64) as the primitive form to get logical bit shifts.

// EncodeVarUint64 encodes an uint64.
func EncodeVarUint64(value uint64) []byte {
	var ret []byte
	for {
		// Encode next 7 bits + terminator bit
		bits := value & 0x7f
		value >>= 7

		var mask uint64
		if value != 0 {
			mask = 0x80
		}
		ret = append(ret, (byte)(bits|mask))
		if value == 0 {
			return ret
		}
	}
}

// TODO(herohde): maybe add an io.Reader or similar form as well.

// DecodeVarUint64 decodes an uint64. The input data is expected to contain
// the encoding, but may be longer.
func DecodeVarUint64(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("empty data")
	}

	var ret uint64
	var shift uint
	for i := 0; i < len(data); i++ {
		// Get 7 bits from next byte
		b := data[i]
		bits := (uint64)(b & 0x7f)

		if shift >= 64 || (shift == 63 && bits > 1) {
			return 0, 0, fmt.Errorf("varint too long")
		}

		ret |= bits << shift
		shift += 7

		if (b & 0x80) == 0 {
			return ret, i + 1, nil
		}
	}
	return 0, 0, fmt.Errorf("varint not terminated: %v", data)
}

// EncodeVarInt encodes an int32.
func EncodeVarInt(value int32) []byte {
	return EncodeVarUint64((uint64)(value) & 0xffffffff)
}

// DecodeVarInt decodes an int32.
func DecodeVarInt(data []byte) (int32, int, error) {
	ret, length, err := DecodeVarUint64(data)
	if err != nil {
		return 0, 0, err
	}
	return (int32)(ret), length, nil
}
