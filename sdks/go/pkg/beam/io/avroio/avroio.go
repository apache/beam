package avroio

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/linkedin/goavro"
)

// Avroio package based directly on textio with some
// elements borrowed from the bigquery implementation.
// Supports reading and unmarshalling Avro files and
// returns either RAW JSON string or Unmarshalled type
// --

func init() {
	// beam.RegisterType(reflect.TypeOf((*writeFileFn)(nil)).Elem())
	// beam.RegisterFunction(readFn)
	beam.RegisterFunction(expandFn)
	beam.RegisterType(reflect.TypeOf((*avroReadFn)(nil)).Elem())
}

// Read reads a set of files and returns the lines as a PCollection<elem>
// based on avro schema. Support a type via < reflect.TypeOf(YourType{}) >  with
// JSON tags defined or if you wish to return the raw JSON string, use < reflect.TypeOf("") >
func Read(s beam.Scope, glob string, t reflect.Type) beam.PCollection {
	s = s.Scope("textio.ReadAvro")
	filesystem.ValidateScheme(glob)
	return read(s, t, beam.Create(s, glob))
}

func read(s beam.Scope, t reflect.Type, col beam.PCollection) beam.PCollection {
	files := beam.ParDo(s, expandFn, col)
	return beam.ParDo(s,
		&avroReadFn{Type: beam.EncodedType{T: t}},
		files,
		beam.TypeDefinition{Var: beam.XType, T: t},
	)
}

func expandFn(ctx context.Context, glob string, emit func(string)) error {
	if strings.TrimSpace(glob) == "" {
		return nil // ignore empty string elements here
	}

	fs, err := filesystem.New(ctx, glob)
	if err != nil {
		return err
	}
	defer fs.Close()

	files, err := fs.List(ctx, glob)
	if err != nil {
		return err
	}
	for _, filename := range files {
		emit(filename)
	}
	return nil
}

type avroReadFn struct {
	// Avro schema type
	Type beam.EncodedType
}

func (f *avroReadFn) ProcessElement(ctx context.Context, filename string, emit func(beam.X)) (err error) {
	log.Infof(ctx, "Reading from %v", filename)

	fs, err := filesystem.New(ctx, filename)
	if err != nil {
		return err
	}
	defer fs.Close()

	fd, err := fs.OpenRead(ctx, filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	ar, err := goavro.NewOCFReader(fd)
	if err != nil {
		log.Errorf(ctx, "error reading avro: %v", err)
		return err
	}

	val := reflect.New(f.Type.T).Interface()
	log.Infof(ctx, "type: %v", err)
	for ar.Scan() {
		var i interface{}
		i, err = ar.Read()
		if err != nil {
			log.Errorf(ctx, "error reading avro row: %v", err)
			continue
		}

		// marshal interface to bytes
		var b []byte
		b, err = json.Marshal(i)
		if err != nil {
			log.Errorf(ctx, "error unmarshalling avro data: %v", err)
			return
		}

		// only emit row if unmarshal was successful
		if err := json.Unmarshal(b, val); err == nil {
			emit(reflect.ValueOf(val).Elem().Interface())
			continue
		}

		emit(string(b))

	}

	return ar.Err()
}

// TODO: Implement Avroio Write Functionanilty
