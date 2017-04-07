package protox

import (
	"encoding/base64"
	"fmt"
	"github.com/golang/protobuf/proto"
)

// EncodeBase64 encodes a proto wrapped in base64.
func EncodeBase64(msg proto.Message) (string, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// DecodeBase64 decodes a base64 wrapped proto.
func DecodeBase64(data string, ret proto.Message) error {
	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return fmt.Errorf("base64 decoding failed: %v", err)
	}
	return proto.Unmarshal(decoded, ret)
}
