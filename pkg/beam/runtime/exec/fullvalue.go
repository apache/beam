package exec

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"io"
	"reflect"
	"time"
)

// TODO: require that FullValue Elm/Elm2 are typed as underlying types? Or just
// drop Universals on call.

// FullValue represents the full runtime value for a data element, incl. the
// implicit context. The result of a GBK or CoGBK is not a single FullValue.
type FullValue struct {
	Elm  reflect.Value // Elm or KV key.
	Elm2 reflect.Value // KV value, if not invalid

	Timestamp typex.EventTime
	// TODO: Window, pane
}

func (v FullValue) String() string {
	if v.Elm2.Kind() == reflect.Invalid {
		return fmt.Sprintf("%v [@%v]", v.Elm.Interface(), time.Time(v.Timestamp).Unix())
	}
	return fmt.Sprintf("KV<%v,%v> [@%v]", v.Elm.Interface(), v.Elm2.Interface(), time.Time(v.Timestamp).Unix())
}

// Stream is a FullValue reader. It returns io.EOF when complete, but can be
// prematurely closed.
type Stream interface {
	io.Closer
	Read() (FullValue, error)
}

// ReStream is Stream factory.
type ReStream interface {
	Open() Stream
}

// FixedReStream is a simple in-memory ReSteam.
type FixedReStream struct {
	Buf []FullValue
}

func (n *FixedReStream) Open() Stream {
	return &FixedStream{Buf: n.Buf}
}

// FixedStream is a simple in-memory Stream from a fixed array.
type FixedStream struct {
	Buf  []FullValue
	next int
}

func (s *FixedStream) Close() error {
	s.Buf = nil
	return nil
}

func (s *FixedStream) Read() (FullValue, error) {
	if s.Buf == nil || s.next == len(s.Buf) {
		return FullValue{}, io.EOF
	}
	ret := s.Buf[s.next]
	s.next++
	return ret, nil
}

// Convert converts type of the runtime value to the desired one. It is needed
// to drop the universal type and convert Aggregate types.
func Convert(value reflect.Value, to reflect.Type) reflect.Value {
	from := value.Type()
	switch {
	case from == to:
		return value

	case typex.IsUniversal(from):
		// We need to drop T to obtain the underlying type of the value.

		var untyped interface{}
		untyped = value.Interface()
		return reflect.ValueOf(untyped) // Convert(reflect.ValueOf(untyped), to)

	case typex.IsList(from) && typex.IsList(to):
		// Convert []A to []B.

		ret := reflect.New(to).Elem()
		for i := 0; i < value.Len(); i++ {
			ret = reflect.Append(ret, Convert(value.Index(i), to.Elem()))
		}
		return ret

	default:
		return value
	}
}

// ReadAll read the full stream and returns the result. It always closes the stream.
func ReadAll(s Stream) ([]FullValue, error) {
	defer s.Close()

	var ret []FullValue
	for {
		elm, err := s.Read()
		if err != nil {
			if err == io.EOF {
				return ret, nil
			}
			return nil, err
		}
		ret = append(ret, elm)
	}
}
