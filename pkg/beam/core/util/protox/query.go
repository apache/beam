package protox

import (
	"net/url"

	"github.com/golang/protobuf/proto"
)

// Some Beam data types use URL query escaping to transmit byte strings
// in JSON fields. These utility functions support encoding and decoding
// those message types.

// EncodeQueryEscaped encodes the supplied message using URL query escaping.
func EncodeQueryEscaped(msg proto.Message) (string, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return "", err
	}
	return url.QueryEscape(string(data)), nil
}

// DecodeQueryEscaped recovers a protocol buffer message from the supplied
// URL query escaped string.
func DecodeQueryEscaped(data string, ret proto.Message) error {
	decoded, err := url.QueryUnescape(data)
	if err != nil {
		return err
	}
	return proto.Unmarshal([]byte(decoded), ret)
}
