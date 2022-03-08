package coder

import (
	"io"
	"reflect"
)

// NullableDecoder handles when a value is nillable.
// Nillable types have an extra byte prefixing them indicating nil status.
func NullableDecoder(decodeToElem func(reflect.Value, io.Reader) error) func(reflect.Value, io.Reader) error {
	return func(ret reflect.Value, r io.Reader) error {
		hasValue, err := DecodeBool(r)
		if err != nil {
			return err
		}
		if !hasValue {
			return nil
		}
		if err := decodeToElem(ret, r); err != nil {
			return err
		}
		return nil
	}
}

// NullableEncoder handles when a value is nillable.
// Nillable types have an extra byte prefixing them indicating nil status.
func NullableEncoder(encodeElem func(reflect.Value, io.Writer) error) func(reflect.Value, io.Writer) error {
	return func(rv reflect.Value, w io.Writer) error {
		if rv.IsNil() {
			return EncodeBool(false, w)
		}
		if err := EncodeBool(true, w); err != nil {
			return err
		}
		return encodeElem(rv, w)
	}
}
