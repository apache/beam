package protox

import (
	"bytes"
	"testing"

	"github.com/golang/protobuf/proto"
	protobufw "github.com/golang/protobuf/ptypes/wrappers"
)

func TestProtoPackingInvertibility(t *testing.T) {
	var buf protobufw.BytesValue
	buf.Value = []byte("Here is some data")

	msg, err := PackProto(&buf)
	if err != nil {
		t.Errorf("Failed to pack data: %v", err)
	}

	var res protobufw.BytesValue
	err = UnpackProto(msg, &res)
	if err != nil {
		t.Errorf("Failed to unpack data: %v", err)
	}

	if !proto.Equal(&res, &buf) {
		t.Errorf("Got %v, wanted %v", res, buf)
	}

}

func TestProto64PackingInvertibility(t *testing.T) {
	var buf protobufw.BytesValue
	buf.Value = []byte("Here is some data")

	any, err := PackBase64Proto(&buf)
	if err != nil {
		t.Errorf("Failed to pack data: %v", err)
	}

	var res protobufw.BytesValue
	err = UnpackBase64Proto(any, &res)
	if err != nil {
		t.Errorf("Failed to unpack data: %v", err)
	}

	if !proto.Equal(&res, &buf) {
		t.Errorf("Got %v, wanted %v", res, buf)
	}
}

func TestBytesPackingInvertibility(t *testing.T) {
	data := []byte("Here is some data")

	any, err := PackBytes(data)
	if err != nil {
		t.Errorf("Failed to pack data: %v", err)
	}

	b, err := UnpackBytes(any)
	if err != nil {
		t.Errorf("Failed to unpack data: %v", err)
	}

	if !bytes.Equal(b, data) {
		t.Errorf("Got %v, wanted %v", b, data)
	}
}
