package protox

import (
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
	buf, err := UnpackBytes(data)
	if err != nil {
		return err
	}
	return proto.Unmarshal(buf, ret)
}

// UnpackBase64Proto decodes a BytesValue + base64 wrapped proto.
func UnpackBase64Proto(data *protobuf.Any, ret proto.Message) error {
	buf, err := UnpackBytes(data)
	if err != nil {
		return err
	}
	return DecodeBase64(string(buf), ret)
}

// UnpackBytes removes the BytesValue wrapper.
func UnpackBytes(data *protobuf.Any) ([]byte, error) {
	if data.TypeUrl != BytesValueTypeUrl {
		return nil, fmt.Errorf("Bad type: %v, want %v", data.TypeUrl, BytesValueTypeUrl)
	}

	var buf protobufw.BytesValue
	if err := proto.Unmarshal(data.Value, &buf); err != nil {
		return nil, fmt.Errorf("BytesValue unmarshal failed: %v", err)
	}
	return buf.Value, nil
}
