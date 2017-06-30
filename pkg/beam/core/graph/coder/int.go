package coder

import (
	"encoding/binary"
	"io"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
)

// EncodeUint64 encodes an uint64 in big endian format.
func EncodeUint64(value uint64, w io.Writer) error {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, value)
	_, err := w.Write(ret)
	return err
}

// DecodeUint64 decodes an uint64 in big endian format.
func DecodeUint64(r io.Reader) (uint64, error) {
	data, err := ioutilx.ReadN(r, 8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(data), nil
}

// EncodeUint32 encodes an uint32 in big endian format.
func EncodeUint32(value uint32, w io.Writer) error {
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret, value)
	_, err := w.Write(ret)
	return err
}

// DecodeUint32 decodes an uint32 in big endian format.
func DecodeUint32(r io.Reader) (uint32, error) {
	data, err := ioutilx.ReadN(r, 4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(data), nil
}

// EncodeInt32 encodes an int32 in big endian format.
func EncodeInt32(value int32, w io.Writer) error {
	return EncodeUint32(uint32(value), w)
}

// DecodeInt32 decodes an int32 in big endian format.
func DecodeInt32(r io.Reader) (int32, error) {
	ret, err := DecodeUint32(r)
	if err != nil {
		return 0, err
	}
	return int32(ret), nil
}
