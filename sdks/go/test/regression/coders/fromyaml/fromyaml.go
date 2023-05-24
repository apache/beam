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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/text/encoding/charmap"
	yaml "gopkg.in/yaml.v2"
)

var unimplementedCoders = map[string]bool{
	"beam:coder:param_windowed_value:v1": true,
	"beam:coder:sharded_key:v1":          true,
	"beam:coder:custom_window:v1":        true,
}

var filteredCases = []struct{ filter, reason string }{
	{"logical", "BEAM-9615: Support logical types"},
	{"30ea5a25-dcd8-4cdb-abeb-5332d15ab4b9", "https://github.com/apache/beam/issues/21206: Support encoding position."},
	{"80be749a-5700-4ede-89d8-dd9a4433a3f8", "https://github.com/apache/beam/issues/19817: Support millis_instant."},
	{"800c44ae-a1b7-4def-bbf6-6217cca89ec4", "https://github.com/apache/beam/issues/19817: Support decimal."},
	{"f0ffb3a4-f46f-41ca-a942-85e3e939452a", "https://github.com/apache/beam/issues/23526: Support char/varchar, binary/varbinary."},
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
		fs := eg.Value.(yaml.MapSlice)
		var rfs []reflect.StructField
		// There are only 2 pointer examples, but they reuse field names,
		// so we key off the proto hash to know which example we're handling.
		ptrEg := strings.Contains(c.Payload, "51ace21c7393")
		for _, rf := range fs {
			name := rf.Key.(string)
			t := nameToType[name]
			if ptrEg {
				t = reflect.PtrTo(t)
			}
			rfs = append(rfs, reflect.StructField{
				Name: strings.ToUpper(name[:1]) + name[1:],
				Type: t,
				Tag:  reflect.StructTag(fmt.Sprintf("beam:\"%v\"", name)),
			})
		}
		rv := reflect.New(reflect.StructOf(rfs)).Elem()
		for i, rf := range fs {
			setField(rv, i, rf.Value)
		}

		got, want = elem.Elm, rv.Interface()
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

// standard_coders.yaml uses the name for type indication, except for nullability.
var nameToType = map[string]reflect.Type{
	"str":     reflectx.String,
	"i32":     reflectx.Int32,
	"f64":     reflectx.Float64,
	"arr":     reflect.SliceOf(reflectx.String),
	"f_bool":  reflectx.Bool,
	"f_bytes": reflect.PtrTo(reflectx.ByteSlice),
	"f_map":   reflect.MapOf(reflectx.String, reflect.PtrTo(reflectx.Int64)),
	"f_float": reflectx.Float32,
}

func setField(rv reflect.Value, i int, v any) {
	if v == nil {
		return
	}
	rf := rv.Field(i)
	if rf.Kind() == reflect.Ptr {
		// Ensure it's initialized.
		rf.Set(reflect.New(rf.Type().Elem()))
		rf = rf.Elem()
	}
	switch rf.Kind() {
	case reflect.String:
		rf.SetString(v.(string))
	case reflect.Int32:
		rf.SetInt(int64(v.(int)))
	case reflect.Float32:
		c, err := strconv.ParseFloat(v.(string), 32)
		if err != nil {
			panic(err)
		}
		rf.SetFloat(c)
	case reflect.Float64:
		c, err := strconv.ParseFloat(v.(string), 64)
		if err != nil {
			panic(err)
		}
		rf.SetFloat(c)
	case reflect.Slice:
		if rf.Type() == reflectx.ByteSlice {
			rf.Set(reflect.ValueOf([]byte(v.(string))))
			break
		}
		// Value is a []any with string values.
		var arr []string
		for _, a := range v.([]any) {
			arr = append(arr, a.(string))
		}
		rf.Set(reflect.ValueOf(arr))
	case reflect.Bool:
		rf.SetBool(v.(bool))
	case reflect.Map:
		// only f_map presently, which is always map[string]*int64
		rm := reflect.MakeMap(rf.Type())
		for _, a := range v.(yaml.MapSlice) {
			rk := reflect.ValueOf(a.Key.(string))
			rv := reflect.Zero(rf.Type().Elem())
			if a.Value != nil {
				rv = reflect.New(reflectx.Int64)
				rv.Elem().SetInt(int64(a.Value.(int)))
			}
			rm.SetMapIndex(rk, rv)
		}
		rf.Set(rm)

	}
}

func (s *Spec) parseCoder(c Coder) string {
	id := s.nextID()
	var compIDs []string
	for _, comp := range c.Components {
		compIDs = append(compIDs, s.parseCoder(comp))
	}
	s.coderPBs[id] = &pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn:     c.Urn,
			Payload: []byte(c.Payload),
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
