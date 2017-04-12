package coderx

import (
	"encoding/binary"
	"fmt"
)

// EncodeUint64 encodes an uint64 in big endian format.
func EncodeUint64(value uint64) []byte {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, value)
	return ret
}

// DecodeUint64 decodes an uint64 in big endian format.
func DecodeUint64(data []byte) (uint64, int, error) {
	if len(data) < 8 {
		return 0, 0, fmt.Errorf("data too short")
	}
	return binary.BigEndian.Uint64(data), 8, nil
}

// EncodeUint32 encodes an uint32 in big endian format.
func EncodeUint32(value uint32) []byte {
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret, value)
	return ret
}

// DecodeUint32 decodes an uint32 in big endian format.
func DecodeUint32(data []byte) (uint32, int, error) {
	if len(data) < 4 {
		return 0, 0, fmt.Errorf("data too short")
	}
	return binary.BigEndian.Uint32(data), 4, nil
}

// EncodeInt32 encodes an int32 in big endian format.
func EncodeInt32(value int32) []byte {
	return EncodeUint32(uint32(value))
}

// DecodeInt32 decodes an int32 in big endian format.
func DecodeInt32(data []byte) (int32, int, error) {
	ret, size, err := DecodeUint32(data)
	if err != nil {
		return 0, 0, err
	}
	return int32(ret), size, nil
}
