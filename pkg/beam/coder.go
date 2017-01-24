package beam

import (
	"encoding/json"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"log"
	"reflect"
)

// Coder represents a coder.
type Coder struct {
	coder *graph.Coder
}

func (c Coder) ID() string {
	if c.coder == nil {
		return "(no coder)"
	}
	return c.coder.ID
}

func NewCoder(id string, encode, decode, data interface{}) (Coder, error) {
	enc, err := graph.ReflectFn(encode)
	if err != nil {
		return Coder{}, fmt.Errorf("Bad encode: %v", err)
	}
	dec, err := graph.ReflectFn(decode)
	if err != nil {
		return Coder{}, fmt.Errorf("Bad decode: %v", err)
	}

	// TODO(herohde): validate coder signature.

	c := &graph.Coder{
		ID:   id,
		Enc:  enc,
		Dec:  dec,
		Data: data,
	}
	return Coder{c}, nil
}

//type UniversalCoder interface {
//	ID() string
//	Encode(t reflect.Type, in reflect.Value /* <-chan T */, out chan<- []byte) error
//	Decode(t reflect.Type, in <-chan []byte, out reflect.Value /* chan<- T */) error
//}

// Universal coders have the below signature.

type jsonContext struct {
	T reflect.Type `beam:"type"`
}

func jsonEnc(_ jsonContext, in reflect.Value /* <-chan T */, out chan<- []byte) error {
	for {
		val, ok := in.Recv()
		if !ok {
			return nil
		}

		data, err := json.Marshal(val.Interface())
		if err != nil {
			return err
		}
		out <- data
	}
}

func jsonDec(ctx jsonContext, out reflect.Value /* chan<- T */, in <-chan []byte) error {
	for data := range in {
		val := reflect.New(ctx.T)
		if err := json.Unmarshal(data, val.Interface()); err != nil {
			return err
		}
		out.Send(val.Elem())
	}
	return nil
}

var InternalCoder *graph.Coder

func init() {
	coder, err := NewCoder("json", jsonEnc, jsonDec, nil)
	if err != nil {
		log.Fatalf("Bad coder: %v", err)
	}
	InternalCoder = coder.coder
}

// TODO(herohde): KV coder, GBK coder

// TODO(herohde): Concretely typed coders have a dynamic signature. We can plumb them
// together and avoid reflection on the data path.

func EncInt(t reflect.Type, in <-chan int, out chan<- []byte) error {
	return nil
}

func DecInt(t reflect.Type, in <-chan []byte, out chan<- int) error {
	return nil
}
