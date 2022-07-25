package spannerio

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"google.golang.org/api/iterator"
	"reflect"
	"strings"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*queryFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*writeFn)(nil)).Elem())
}

func columnsFromStruct(t reflect.Type) []string {
	var columns []string

	for i := 0; i < t.NumField(); i++ {
		columns = append(columns, t.Field(i).Tag.Get("spanner"))
	}

	return columns
}

// Read reads all rows from the given table. The table must have a schema
// compatible with the given type, t, and Read returns a PCollection<t>. If the
// table has more rows than t, then Read is implicitly a projection.
func Read(s beam.Scope, database, table string, t reflect.Type) beam.PCollection {
	s = s.Scope("spanner.Read")

	// TODO(herohde) 7/13/2017: using * is probably too inefficient. We could infer
	// a focused query from the type.

	cols := strings.Join(columnsFromStruct(t), ",")

	return query(s, database, fmt.Sprintf("SELECT %v from [%v]", cols, table), t)
}

// QueryOptions represents additional options for executing a query.
type QueryOptions struct {
}

// Query executes a query. The output must have a schema compatible with the given
// type, t. It returns a PCollection<t>.
func Query(s beam.Scope, database, q string, t reflect.Type, options ...func(*QueryOptions) error) beam.PCollection {
	s = s.Scope("spanner.Query")
	return query(s, database, q, t, options...)
}

func query(s beam.Scope, database, query string, t reflect.Type, options ...func(*QueryOptions) error) beam.PCollection {
	queryOptions := QueryOptions{}
	for _, opt := range options {
		if err := opt(&queryOptions); err != nil {
			panic(err)
		}
	}

	imp := beam.Impulse(s)
	return beam.ParDo(s, &queryFn{Database: database, Query: query, Type: beam.EncodedType{T: t}, Options: queryOptions}, imp, beam.TypeDefinition{Var: beam.XType, T: t})
}

type queryFn struct {
	// Database is the spanner connection string
	Database string `json:"database"`
	// Table is the table identifier.
	Query string `json:"query"`
	// Type is the encoded schema type.
	Type beam.EncodedType `json:"type"`
	// Options specifies additional query execution options.
	Options QueryOptions `json:"options"`
}

func (f *queryFn) ProcessElement(ctx context.Context, _ []byte, emit func(beam.X)) error {
	client, err := spanner.NewClient(ctx, f.Database)
	if err != nil {
		return err
	}
	defer client.Close()

	// todo: Use Batch Read

	stmt := spanner.Statement{SQL: f.Query}
	it := client.Single().Query(ctx, stmt)
	defer it.Stop()

	for {
		val := reflect.New(f.Type.T).Interface() // val : *T
		row, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}

		if err := row.ToStruct(val); err != nil {
			return err
		}

		emit(reflect.ValueOf(val).Elem().Interface()) // emit(*val)
	}
	return nil
}

// Write writes the elements of the given PCollection<T> to spanner. T is required
// to be the schema type.
func Write(s beam.Scope, database, table string, col beam.PCollection) {
	t := col.Type().Type()

	s = s.Scope("spanner.Write")

	pre := beam.AddFixedKey(s, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeFn{Database: database, Table: table, Type: beam.EncodedType{T: t}, BatchSize: 1000}, post)
}

type writeFn struct {
	Database  string           `json:"database"`  // Fully qualified identifier
	Table     string           `json:"table"`     // The table to write to
	Type      beam.EncodedType `json:"type"`      // Type is the encoded schema type.
	BatchSize int              `json:"batchSize"` // The number of rows to write in a batch
}

func (f *writeFn) ProcessElement(ctx context.Context, _ int, iter func(*beam.X) bool) error {
	client, err := spanner.NewClient(ctx, f.Database)
	if err != nil {
		return err
	}
	defer client.Close()

	var mutations []*spanner.Mutation

	var val beam.X
	for iter(&val) {
		mutation, err := spanner.InsertOrUpdateStruct(f.Table, val)
		if err != nil {
			return err
		}

		mutations = append(mutations, mutation)

		if len(mutations)+1 > f.BatchSize {
			_, err := client.Apply(ctx, mutations)
			if err != nil {
				return err
			}

			mutations = nil
		}
	}

	if mutations != nil {
		_, err := client.Apply(ctx, mutations)
		if err != nil {
			return err
		}
	}

	return nil
}
