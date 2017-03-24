package protox

import (
	"encoding/base64"
	"fmt"
	"github.com/golang/protobuf/proto"
	protobuf "github.com/golang/protobuf/ptypes/any"
	protobufw "github.com/golang/protobuf/ptypes/wrappers"
)

const (
	BytesValueTypeUrl = "type.googleapis.com/google.protobuf.BytesValue"
)

// Unpack decodes a proto.
func Unpack(data *protobuf.Any, url string, ret proto.Message) error {
	if data.TypeUrl != url {
		return fmt.Errorf("Bad type: %v, want %v", data.TypeUrl, url)
	}
	return proto.Unmarshal(data.Value, ret)
}

// UnpackProto decodes a BytesValue wrapped proto.
func UnpackProto(data *protobuf.Any, ret proto.Message) error {
	buf, err := unpackBytes(data)
	if err != nil {
		return err
	}
	return proto.Unmarshal(buf, ret)
}

// UnpackBase64Proto decodes a BytesValue + base64 wrapped proto.
func UnpackBase64Proto(data *protobuf.Any, ret proto.Message) error {
	buf, err := unpackBytes(data)
	if err != nil {
		return err
	}
	decoded, err := base64.StdEncoding.DecodeString(string(buf))
	if err != nil {
		return fmt.Errorf("base64 decoding failed: %v", err)
	}
	return proto.Unmarshal(decoded, ret)
}

func unpackBytes(data *protobuf.Any) ([]byte, error) {
	if data.TypeUrl != BytesValueTypeUrl {
		return nil, fmt.Errorf("Bad type: %v, want %v", data.TypeUrl, BytesValueTypeUrl)
	}

	var buf protobufw.BytesValue
	if err := proto.Unmarshal(data.Value, &buf); err != nil {
		return nil, fmt.Errorf("BytesValue unmarshal failed: %v", err)
	}
	return buf.Value, nil
}
