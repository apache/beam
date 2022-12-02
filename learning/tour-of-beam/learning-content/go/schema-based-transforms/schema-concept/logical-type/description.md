# Logical types

Currently the Go SDK provides minimal convenience logical types, other than to handle additional integer primitives, and `time.Time`.

```
// Define a logical provider like so:


// TimestampNanos is a logical type using time.Time, but
// encodes as a schema type.
type TimestampNanos time.Time

func (tn TimestampNanos) Seconds() int64 {
	return time.Time(tn).Unix()
}
func (tn TimestampNanos) Nanos() int32 {
	return int32(time.Time(tn).UnixNano() % 1000000000)
}

// tnStorage is the storage schema for TimestampNanos.
type tnStorage struct {
	Seconds int64 `beam:"seconds"`
	Nanos   int32 `beam:"nanos"`
}

var (
	// reflect.Type of the Value type of TimestampNanos
	tnType        = reflect.TypeOf((*TimestampNanos)(nil)).Elem()
	tnStorageType = reflect.TypeOf((*tnStorage)(nil)).Elem()
)

// TimestampNanosProvider implements the beam.SchemaProvider interface.
type TimestampNanosProvider struct{}

// FromLogicalType converts checks if the given type is TimestampNanos, and if so
// returns the storage type.
func (p *TimestampNanosProvider) FromLogicalType(rt reflect.Type) (reflect.Type, error) {
	if rt != tnType {
		return nil, fmt.Errorf("unable to provide schema.LogicalType for type %v, want %v", rt, tnType)
	}
	return tnStorageType, nil
}

// BuildEncoder builds a Beam schema encoder for the TimestampNanos type.
func (p *TimestampNanosProvider) BuildEncoder(rt reflect.Type) (func(interface{}, io.Writer) error, error) {
	if _, err := p.FromLogicalType(rt); err != nil {
		return nil, err
	}
	enc, err := coder.RowEncoderForStruct(tnStorageType)
	if err != nil {
		return nil, err
	}
	return func(iface interface{}, w io.Writer) error {
		v := iface.(TimestampNanos)
		return enc(tnStorage{
			Seconds: v.Seconds(),
			Nanos:   v.Nanos(),
		}, w)
	}, nil
}

// BuildDecoder builds a Beam schema decoder for the TimestampNanos type.
func (p *TimestampNanosProvider) BuildDecoder(rt reflect.Type) (func(io.Reader) (interface{}, error), error) {
	if _, err := p.FromLogicalType(rt); err != nil {
		return nil, err
	}
	dec, err := coder.RowDecoderForStruct(tnStorageType)
	if err != nil {
		return nil, err
	}
	return func(r io.Reader) (interface{}, error) {
		s, err := dec(r)
		if err != nil {
			return nil, err
		}
		tn := s.(tnStorage)
		return TimestampNanos(time.Unix(tn.Seconds, int64(tn.Nanos))), nil
	}, nil
}



// Register it like so:

beam.RegisterSchemaProvider(tnType, &TimestampNanosProvider{})
```