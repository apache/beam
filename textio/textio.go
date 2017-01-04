package textio

import (
	"reflect"

	"beam"
)

// GOOD: Read/Write have natural signatures.

func Read(p *beam.Pipeline, filename string) (*beam.PCollection, error) {
	// TODO: actual source information. Maybe special node.
	n := p.NewNode("textio.Read")
	n.T = reflect.TypeOf("") // string type
	return n, nil
}

func Write(p *beam.Pipeline, filename string, col *beam.PCollection) error {
	// TODO: actual sink information. Maybe special node.
	sink := p.NewNode("textio.Write")
	sink.T = col.T
	return p.Connect(col, sink)
}
