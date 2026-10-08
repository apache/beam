// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// fromyaml generates a resource file from the standard_coders.yaml
// file for use in these coder regression tests.
//
// It expects to be run in it's test directory, or via it's go test.
package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/text/encoding/charmap"
	"google.golang.org/protobuf/proto"
	yaml "gopkg.in/yaml.v2"
)

var unimplementedCoders = map[string]bool{
	"beam:coder:param_windowed_value:v1": true,
	"beam:coder:sharded_key:v1":          true,
	"beam:coder:custom_window:v1":        true,
}

var filteredCases = []struct{ filter, reason string }{
	{"30ea5a25-dcd8-4cdb-abeb-5332d15ab4b9", "https://github.com/apache/beam/issues/21206: Support encoding position."},
	{"beam:logical_type:millis_instant:v1", "https://github.com/apache/beam/issues/39684: Support millis_instant."},
	{"beam:logical_type:decimal:v1", "https://github.com/apache/beam/issues/39684: Support decimal."},
	{"beam:logical_type:fixed_char:v1", "https://github.com/apache/beam/issues/39684: Support char/varchar, binary/varbinary."},
	{"beam:logical_type:timestamp:v1", "https://github.com/apache/beam/issues/39684: Support timestamp."},
}

// Coder is a representation a serialized beam coder.
type Coder struct {
	Urn              string  `yaml:"urn,omitempty"`
	Payload          string  `yaml:"payload,omitempty"`
	Components       []Coder `yaml:"components,omitempty"`
	NonDeterministic bool    `yaml:"non_deterministic,omitempty"`
}

type logger interface {
	Errorf(string, ...any)
	Logf(string, ...any)
}

// Spec is a set of conditions that a coder must pass.
type Spec struct {
	Coder    Coder         `yaml:"coder,omitempty"`
	Nested   *bool         `yaml:"nested,omitempty"`
	Examples yaml.MapSlice `yaml:"examples,omitempty"`
	Log      logger

	id       int // for generating coder ids.
	coderPBs map[string]*pipepb.Coder
}

func (s *Spec) nextID() string {
	ret := fmt.Sprintf("%d", s.id)
	s.id++
	return ret
}

func (s *Spec) testStandardCoder() (err error) {
	if unimplementedCoders[s.Coder.Urn] {
		log.Printf("skipping unimplemented coder urn: %v", s.Coder.Urn)
		return nil
	}
	if s.Coder.Urn == "beam:coder:state_backed_iterable:v1" {
		log.Printf("skipping unimplemented test coverage for beam:coder:state_backed_iterable:v1. https://github.com/apache/beam/issues/21324")
		return nil
	}
	for _, c := range filteredCases {
		if strings.Contains(s.Coder.Payload, c.filter) {
			log.Printf("skipping coder case. Unsupported in the Go SDK for now: %v Payload: %v", c.reason, s.Coder.Payload)
			return nil
		}
	}

	// Construct the coder proto equivalents.

	// Only nested tests need to be run, since nestedness is a pre-portability
	// concept.
	// For legacy Java reasons, the row coder examples are all marked nested: false
	// so we need to check that before skipping unnested tests.
	if s.Coder.Urn != "beam:coder:row:v1" && s.Nested != nil && !*s.Nested {
		log.Printf("skipping unnested coder spec: %v\n", s.Coder)
		return nil
	}

	s.coderPBs = make(map[string]*pipepb.Coder)
	id := s.parseCoder(s.Coder)
	b := graphx.NewCoderUnmarshaller(s.coderPBs)
	underTest, err := b.Coder(id)
	if err != nil {
		return fmt.Errorf("unable to create coder: %v", err)
	}

	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("panicked on coder %v || %v:\n\t%v :\n%s", underTest, s.Coder, e, debug.Stack())
		}
	}()

	var decFails, encFails int
	for _, eg := range s.Examples {

		// Test Decoding
		// Ideally we'd use the beam package coders, but KVs make that complicated.
		// This can be cleaned up once a type parametered beam.KV type exists.
		dec := exec.MakeElementDecoder(underTest)
		encoded := eg.Key.(string)
		var elem exec.FullValue

		// What I would have expected.
		//		r := charmap.ISO8859_1.NewDecoder().Reader(strings.NewReader(encoded))
		recoded, err := charmap.ISO8859_1.NewEncoder().String(encoded)
		if err != nil {
			return err
		}
		r := strings.NewReader(recoded)
		if err := dec.DecodeTo(r, &elem); err != nil {
			return fmt.Errorf("err decoding %q: %v", encoded, err)
		}
		if !diff(s.Coder, &elem, eg) {
			decFails++
			continue
		}

		// Test Encoding
		if s.Coder.NonDeterministic {
			// Skip verifying nondeterministic encodings.
			continue
		}
		enc := exec.MakeElementEncoder(underTest)
		var out bytes.Buffer
		if err := enc.Encode(&elem, &out); err != nil {
			return err
		}
		if d := cmp.Diff(recoded, string(out.Bytes())); d != "" {
			log.Printf("Encoding error: diff(-want,+got): %v\n", d)
			encFails++
		}
	}
	if decFails+encFails > 0 {
		return fmt.Errorf("failed to decode %v times, and encode %v times", decFails, encFails)
	}

	return nil
}

var cmpOpts = []cmp.Option{
	cmp.Transformer("bytes2string", func(in []byte) (out string) {
		return string(in)
	}),
	cmp.Comparer(func(a, b schema.MicrosInstant) bool {
		return a.Time().Equal(b.Time())
	}),
}

// latin1Bytes returns the bytes that a yaml string denotes. The yaml strings
// escape bytes above 0x7f as code points, which yaml decodes to UTF-8.
func latin1Bytes(s string) ([]byte, error) {
	return charmap.ISO8859_1.NewEncoder().Bytes([]byte(s))
}

func diff(c Coder, elem *exec.FullValue, eg yaml.MapItem) bool {
	var got, want any
	switch c.Urn {
	case "beam:coder:bytes:v1":
		got = string(elem.Elm.([]byte))
		switch egv := eg.Value.(type) {
		case string:
			want = egv
		case []byte:
			want = string(egv)
		}
	case "beam:coder:varint:v1":
		got, want = elem.Elm.(int64), int64(eg.Value.(int))
	case "beam:coder:double:v1":
		got = elem.Elm.(float64)
		switch v := eg.Value.(string); v {
		case "NaN":
			// Do the NaN comparison here since NaN by definition != NaN.
			if math.IsNaN(got.(float64)) {
				want, got = 1, 1
			} else {
				want = math.NaN()
			}
		case "-Infinity":
			want = math.Inf(-1)
		case "Infinity":
			want = math.Inf(1)
		default:
			want, _ = strconv.ParseFloat(v, 64)
		}

	case "beam:coder:kv:v1":
		v := eg.Value.(yaml.MapSlice)
		pass := true
		if !diff(c.Components[0], &exec.FullValue{Elm: elem.Elm}, v[0]) {
			pass = false
		}
		if !diff(c.Components[1], &exec.FullValue{Elm: elem.Elm2}, v[1]) {
			pass = false
		}
		return pass

	case "beam:coder:nullable:v1":
		if elem.Elm == nil || eg.Value == nil {
			got, want = elem.Elm, eg.Value
		} else {
			got = string(elem.Elm.([]byte))
			switch egv := eg.Value.(type) {
			case string:
				want = egv
			case []byte:
				want = string(egv)
			}
		}

	case "beam:coder:iterable:v1":
		pass := true
		gotrv := reflect.ValueOf(elem.Elm)
		wantrv := reflect.ValueOf(eg.Value)
		if gotrv.Len() != wantrv.Len() {
			log.Printf("Lengths don't match. got %v, want %v;  %v, %v", gotrv.Len(), wantrv.Len(), gotrv, wantrv)
			return false
		}
		for i := 0; i < wantrv.Len(); i++ {
			if !diff(c.Components[0],
				&exec.FullValue{Elm: gotrv.Index(i).Interface()},
				yaml.MapItem{Value: wantrv.Index(i).Interface()}) {
				pass = false
			}

		}
		return pass
	case "beam:coder:interval_window:v1":
		var a, b int
		val := eg.Value
		if is, ok := eg.Value.([]any); ok {
			val = is[0]
		}
		v := val.(yaml.MapSlice)

		a = v[0].Value.(int)
		b = v[1].Value.(int)
		end := mtime.FromMilliseconds(int64(a))
		start := end - mtime.Time(int64(b))
		want = window.IntervalWindow{Start: start, End: end}
		// If this is nested in an iterable, windows won't be populated.
		if len(elem.Windows) == 0 {
			got = elem.Elm
		} else {
			got = elem.Windows[0]
		}

	case "beam:coder:global_window:v1":
		want = window.GlobalWindow{}
		// If this is nested in an iterable, windows won't be populated.
		if len(elem.Windows) == 0 {
			got = window.GlobalWindow(elem.Elm.(struct{}))
		} else {
			got = elem.Windows[0]
		}
	case "beam:coder:windowed_value:v1", "beam:coder:param_windowed_value:v1":
		// elem contains all the information, but we need to compare the element+timestamp
		// separately from the windows, to avoid repeated expected value parsing logic.
		pass := true
		vs := eg.Value.(yaml.MapSlice)
		if !diff(c.Components[0], elem, vs[0]) {
			pass = false
		}
		if d := cmp.Diff(
			mtime.FromMilliseconds(int64(vs[1].Value.(int))),
			elem.Timestamp, cmpOpts...); d != "" {

			pass = false
		}
		if !diff(c.Components[1], elem, vs[3]) {
			pass = false
		}
		if !diffPane(vs[2].Value, elem.Pane) {
			pass = false
		}
		return pass
	case "beam:coder:row:v1":
		row, err := rowFromYAML(c.Payload, eg.Value.(yaml.MapSlice))
		if err != nil {
			log.Printf("unable to build the expected row: %v\n", err)
			return false
		}
		got, want = elem.Elm, row
	case "beam:coder:timer:v1":
		pass := true
		tm := elem.Elm.(exec.TimerRecv)
		fs := eg.Value.(yaml.MapSlice)
		for _, item := range fs {

			switch item.Key.(string) {
			case "userKey":
				if want := item.Value.(string); want != tm.Key.Elm.(string) {
					pass = false
				}
			case "dynamicTimerTag":
				if want := item.Value.(string); want != tm.Tag {
					pass = false
				}
			case "windows":
				if v, ok := item.Value.([]any); ok {
					for i, val := range v {
						if val.(string) == "global" && fmt.Sprintf("%s", tm.Windows[i]) == "[*]" {
							continue
						} else if val.(string) != fmt.Sprintf("%s", tm.Windows[i]) {
							pass = false
						}
					}
				}
			case "clearBit":
				if want := item.Value.(bool); want != tm.Clear {
					pass = false
				}
			case "fireTimestamp":
				if want := item.Value.(int); want != int(tm.FireTimestamp) {
					pass = false
				}
			case "holdTimestamp":
				if want := item.Value.(int); want != int(tm.HoldTimestamp) {
					pass = false
				}
			case "pane":
				pass = diffPane(item.Value, tm.Pane)
			}
		}
		return pass
	default:
		got, want = elem.Elm, eg.Value
	}
	if d := cmp.Diff(want, got, cmpOpts...); d != "" {
		log.Printf("Decoding error: diff(-want,+got): %v\n", d)
		return false
	}
	return true
}

func diffPane(eg any, got typex.PaneInfo) bool {
	pass := true
	paneTiming := map[typex.PaneTiming]string{
		typex.PaneUnknown: "UNKNOWN",
		typex.PaneEarly:   "EARLY",
		typex.PaneLate:    "LATE",
		typex.PaneOnTime:  "ONTIME",
	}
	for _, item := range eg.(yaml.MapSlice) {
		switch item.Key.(string) {
		case "is_first":
			if want := item.Value.(bool); want != got.IsFirst {
				pass = false
			}
		case "is_last":
			if want := item.Value.(bool); want != got.IsLast {
				pass = false
			}
		case "timing":
			if want := item.Value.(string); want != paneTiming[got.Timing] {
				pass = false
			}
		case "index":
			if want := item.Value.(int); want != int(got.Index) {
				pass = false
			}
		case "on_time_index":
			if want := item.Value.(int); want != int(got.NonSpeculativeIndex) {
				pass = false
			}
		}
	}
	return pass
}

// rowFromYAML builds the expected value of a beam:coder:row:v1 example from
// the schema in the coder payload and the field values of the example.
// The Go type is the one the Go SDK derives from the schema, so logical type
// fields have their registered Go types.
func rowFromYAML(payload string, fields yaml.MapSlice) (any, error) {
	b, err := latin1Bytes(payload)
	if err != nil {
		return nil, err
	}
	var s pipepb.Schema
	if err := proto.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("unmarshalling the row schema: %v", err)
	}
	rt, err := schema.ToType(&s)
	if err != nil {
		return nil, fmt.Errorf("converting the row schema to a type: %v", err)
	}
	rv := reflect.New(rt).Elem()
	if err := setStruct(rv, fields); err != nil {
		return nil, err
	}
	return rv.Interface(), nil
}

// setStruct sets the fields of the struct value rv from the yaml mapping.
func setStruct(rv reflect.Value, fields yaml.MapSlice) error {
	for _, f := range fields {
		name := f.Key.(string)
		fv, ok := fieldByName(rv, name)
		if !ok {
			return fmt.Errorf("no field %q in %v", name, rv.Type())
		}
		if err := setValue(fv, f.Value); err != nil {
			return fmt.Errorf("field %q: %v", name, err)
		}
	}
	return nil
}

// fieldByName returns the field of rv with the given schema field name, which
// is either the Go field name or the name in the beam struct tag.
func fieldByName(rv reflect.Value, name string) (reflect.Value, bool) {
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		if sf.Name == name {
			return rv.Field(i), true
		}
		if tag, ok := sf.Tag.Lookup("beam"); ok && strings.Split(tag, ",")[0] == name {
			return rv.Field(i), true
		}
	}
	return reflect.Value{}, false
}

// setValue sets rv from a yaml example value. A nil value leaves rv at its
// zero value, which is nil for nullable fields.
func setValue(rv reflect.Value, v any) error {
	if v == nil {
		return nil
	}
	if rv.Kind() == reflect.Ptr {
		rv.Set(reflect.New(rv.Type().Elem()))
		rv = rv.Elem()
	}
	switch rv.Type() {
	case reflect.TypeOf(schema.MicrosInstant{}):
		var seconds, micros int64
		for _, f := range v.(yaml.MapSlice) {
			switch f.Key.(string) {
			case "seconds":
				seconds = int64(f.Value.(int))
			case "micros":
				micros = int64(f.Value.(int))
			}
		}
		rv.Set(reflect.ValueOf(schema.MicrosInstant(time.Unix(seconds, micros*1000).UTC())))
		return nil
	}
	switch rv.Kind() {
	case reflect.String:
		rv.SetString(v.(string))
	case reflect.Int16, reflect.Int32, reflect.Int64:
		rv.SetInt(int64(v.(int)))
	case reflect.Float32:
		c, err := strconv.ParseFloat(v.(string), 32)
		if err != nil {
			return err
		}
		rv.SetFloat(c)
	case reflect.Float64:
		c, err := strconv.ParseFloat(v.(string), 64)
		if err != nil {
			return err
		}
		rv.SetFloat(c)
	case reflect.Bool:
		rv.SetBool(v.(bool))
	case reflect.Slice:
		if rv.Type() == reflectx.ByteSlice {
			rv.SetBytes([]byte(v.(string)))
			return nil
		}
		items := v.([]any)
		sv := reflect.MakeSlice(rv.Type(), len(items), len(items))
		for i, item := range items {
			if err := setValue(sv.Index(i), item); err != nil {
				return err
			}
		}
		rv.Set(sv)
	case reflect.Map:
		mv := reflect.MakeMap(rv.Type())
		for _, entry := range v.(yaml.MapSlice) {
			key := reflect.New(rv.Type().Key()).Elem()
			if err := setValue(key, entry.Key); err != nil {
				return err
			}
			val := reflect.New(rv.Type().Elem()).Elem()
			if err := setValue(val, entry.Value); err != nil {
				return err
			}
			mv.SetMapIndex(key, val)
		}
		rv.Set(mv)
	case reflect.Struct:
		return setStruct(rv, v.(yaml.MapSlice))
	default:
		return fmt.Errorf("unsupported field type %v", rv.Type())
	}
	return nil
}

func (s *Spec) parseCoder(c Coder) string {
	id := s.nextID()
	var compIDs []string
	for _, comp := range c.Components {
		compIDs = append(compIDs, s.parseCoder(comp))
	}
	payload, err := latin1Bytes(c.Payload)
	if err != nil {
		panic(fmt.Sprintf("invalid coder payload %q: %v", c.Payload, err))
	}
	s.coderPBs[id] = &pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn:     c.Urn,
			Payload: payload,
		},
		ComponentCoderIds: compIDs,
	}
	return id
}

// Simple logger to run as main program.
type logLogger struct{}

func (*logLogger) Errorf(format string, v ...any) {
	log.Printf(format, v...)
}

func (*logLogger) Logf(format string, v ...any) {
	log.Printf(format, v...)
}

const yamlPath = "../../../../../../model/fn-execution/src/main/resources/org/apache/beam/model/fnexecution/v1/standard_coders.yaml"

func main() {
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		log.Fatalf("Couldn't read %v: %v", yamlPath, err)
	}
	specs := bytes.Split(data, []byte("\n---\n"))
	var failures bool
	var l logLogger
	for _, data := range specs {
		cs := Spec{Log: &l}
		if err := yaml.Unmarshal(data, &cs); err != nil {
			failures = true
			l.Logf("unable to parse yaml: %v %q", err, data)
			continue
		}
		if err := cs.testStandardCoder(); err != nil {
			failures = true
			l.Logf("Failed \"%v\": %v", cs.Coder, err)
		}
	}
	if !failures {
		log.Println("PASS")
	}
}
