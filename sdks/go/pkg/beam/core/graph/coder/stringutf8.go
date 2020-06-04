package coder

import (
	"io"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
)

const bufCap = 64

// EncodeStringUTF8LP encodes a UTF string with a length prefix.
func EncodeStringUTF8LP(s string, w io.Writer) error {
	if err := EncodeVarInt(int64(len(s)), w); err != nil {
		return err
	}
	return encodeStringUTF8(s, w)
}

// encodeStringUTF8 encodes a string.
func encodeStringUTF8(s string, w io.Writer) error {
	l := len(s)
	var b [bufCap]byte
	i := 0
	for l-i > bufCap {
		n := i + bufCap
		copy(b[:], s[i:n])
		if _, err := ioutilx.WriteUnsafe(w, b[:]); err != nil {
			return err
		}
		i = n
	}
	n := l - i
	copy(b[:n], s[i:])
	ioutilx.WriteUnsafe(w, b[:n])
	return nil
}

// decodeStringUTF8 reads l bytes and produces a string.
func decodeStringUTF8(l int64, r io.Reader) (string, error) {
	var builder strings.Builder
	var b [bufCap]byte
	i := l
	for i > bufCap {
		err := ioutilx.ReadNBufUnsafe(r, b[:])
		if err != nil {
			return "", err
		}
		i -= bufCap
		builder.Write(b[:])
	}
	err := ioutilx.ReadNBufUnsafe(r, b[:i])
	if err != nil {
		return "", err
	}
	builder.Write(b[:i])

	return builder.String(), nil
}

// DecodeStringUTF8LP decodes a length prefixed utf8 string.
func DecodeStringUTF8LP(r io.Reader) (string, error) {
	l, err := DecodeVarInt(r)
	if err != nil {
		return "", err
	}
	return decodeStringUTF8(l, r)
}
