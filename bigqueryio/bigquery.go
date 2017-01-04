package bigqueryio

import (
	"reflect"

	"beam"
)

// Options are BQ table options
type Options int

var (
	None           Options = 0x0
	CreateIfNeeded Options = 0x1
	WriteTruncate  Options = 0x2
	JustCorruptIt  Options = 0x4
)

type TableRow struct {
	columns map[string]string
}

func ReadUntyped(p *beam.Pipeline, table string) (*beam.PCollection, error) {
	// TODO: actual source information. Maybe special node.
	n := p.NewNode("bigquery.Read")
	n.T = reflect.TypeOf(TableRow{}) // untyped row type
	return n, nil
}

// GOOD: we could support implicit conversion of an untyped TableRow to
// a concrete-typed Struct, such as below (or perhaps with an extra ParDo).
// The IO implementation could basically do similar type processing for
// user convenience.

func Read(p *beam.Pipeline, table string, schema interface{}) (*beam.PCollection, error) {
	// TODO: actual source information. Maybe special node.
	n := p.NewNode("bigquery.Read")
	n.T = reflect.TypeOf(schema) // typed row type
	return n, nil
}

// TODO: if the schema is not explicit here, we could fish it out of the col.T. That would
// require a step with explicit typing to accept the result of GBK. Not sure what will work best.

func Write(p *beam.Pipeline, table string, schema interface{}, opt Options, col *beam.PCollection) error {
	// TODO: actual sink information. Maybe special node.
	sink := p.NewNode("bigquery.Write")
	sink.T = reflect.TypeOf(schema)
	return p.Connect(col, sink)
}
