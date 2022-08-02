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

package errors

import (
	"fmt"
	"testing"
)

func TestNew(t *testing.T) {
	const want string = "error message"
	err := New(want)
	if err.Error() != want {
		t.Errorf("Error msg does not match original. Want: %q, Got: %q", want, err.Error())
	}
}

func TestErrorf(t *testing.T) {
	want := fmt.Sprintf("%s %d", "ten", 10)
	err := Errorf("%s %d", "ten", 10)
	if err.Error() != want {
		t.Errorf("Incorrect formatting. Want: %q, Got: %q", want, err.Error())
	}
}

const (
	base string = "base"
	msg1 string = "message 1"
	msg2 string = "message 2"
	ctx1 string = "context 1"
	ctx2 string = "context 2"
	top1 string = "top level message 1"
	top2 string = "top level message 2"
)

func TestWrap(t *testing.T) {
	tests := []struct {
		err  error
		want errorStructure
	}{
		{
			err:  Wrap(New(base), msg1),
			want: errorStructure{{ERROR, msg1}, {ERROR, base}},
		}, {
			err:  Wrap(Wrap(New(base), msg1), msg2),
			want: errorStructure{{ERROR, msg2}, {ERROR, msg1}, {ERROR, base}},
		}, {
			err:  Wrap(nil, msg1),
			want: nil,
		},
	}
	for _, test := range tests {
		got := getStructure(test.err)
		if !equalStructure(got, test.want) {
			t.Errorf("Incorrect structure. Want: %+v, Got: %+v", test.want, got)
		}
	}
}

func TestWrapf(t *testing.T) {
	want := fmt.Sprintf("%s %d", "ten", 10)
	err := Wrapf(New(base), "%s %d", "ten", 10)
	got := getStructure(err)[0].msg
	if got != want {
		t.Errorf("Incorrect formatting. Want: %q, Got: %q", want, got)
	}
}

func TestWrapf_NilErr(t *testing.T) {
	err := Wrapf(nil, "%s %d", "ten", 10)
	if err != nil {
		t.Errorf(`Wrapf(nil, "%%s %%d", "ten", 10). Want: nil, Got: %q`, err)
	}
}

func TestContext(t *testing.T) {
	tests := []struct {
		err  error
		want errorStructure
	}{
		{
			err:  WithContext(New(base), ctx1),
			want: errorStructure{{CONTEXT, ctx1}, {ERROR, base}},
		}, {
			err:  WithContext(Wrap(WithContext(New(base), ctx1), msg1), ctx2),
			want: errorStructure{{CONTEXT, ctx2}, {ERROR, msg1}, {CONTEXT, ctx1}, {ERROR, base}},
		}, {
			err:  Wrap(WithContext(WithContext(Wrap(New(base), msg1), ctx1), ctx2), msg2),
			want: errorStructure{{ERROR, msg2}, {CONTEXT, ctx2}, {CONTEXT, ctx1}, {ERROR, msg1}, {ERROR, base}},
		}, {
			err:  WithContext(nil, ctx1),
			want: nil,
		},
	}
	for _, test := range tests {
		got := getStructure(test.err)
		if !equalStructure(got, test.want) {
			t.Errorf("Incorrect structure. Want: %+v, Got: %+v", test.want, got)
		}
	}
}

func TestWithContextf_NilErr(t *testing.T) {
	err := WithContextf(nil, "%s %d", "ten", 10)
	if err != nil {
		t.Errorf(`WithContextf(nil, "%%s %%d", "ten", 10). Want: nil, Got: %q`, err)
	}
}

func TestWithContextf(t *testing.T) {
	want := fmt.Sprintf("%s %d", "ten", 10)
	err := WithContextf(New(base), "%s %d", "ten", 10)
	got := getStructure(err)[0].msg
	if got != want {
		t.Errorf("Incorrect formatting. Want: %q, Got: %q", want, got)
	}
}

func TestTopLevelMsg(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{
			err:  New(base),
			want: "",
		}, {
			err:  Wrap(WithContext(New(base), ctx1), msg1),
			want: "",
		}, {
			err:  SetTopLevelMsg(New(base), top1),
			want: top1,
		}, {
			err:  Wrap(SetTopLevelMsg(WithContext(New(base), ctx1), top1), msg1),
			want: top1,
		}, {
			err:  Wrap(SetTopLevelMsg(WithContext(SetTopLevelMsg(New(base), top1), ctx1), top2), msg1),
			want: top2,
		}, {
			err:  SetTopLevelMsg(nil, top1),
			want: "",
		},
	}
	for _, test := range tests {
		got := getTop(test.err)
		if got != test.want {
			t.Errorf("Incorrect top-level message. Want: %+v, Got: %+v", test.want, got)
		}
	}
}

func TestSetTopLevelMsgf(t *testing.T) {
	want := fmt.Sprintf("%s %d", "ten", 10)
	err := SetTopLevelMsgf(New(base), "%s %d", "ten", 10)
	if getTop(err) != want {
		t.Errorf("Incorrect formatting. Want: %q, Got: %q", want, getTop(err))
	}
}

func TestSetTopLevelMsgf_NilErr(t *testing.T) {
	want := ""
	err := SetTopLevelMsgf(nil, "%s %d", "ten", 10)
	if getTop(err) != want {
		t.Errorf("Incorrect formatting. Want: %q, Got: %q", want, getTop(err))
	}
}

func TestError(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{
			err:  Wrap(New(base), msg1),
			want: "message 1\n\tcaused by:\nbase",
		},
		{
			err:  SetTopLevelMsg(New(base), top1),
			want: "top level message 1\nFull error:\nbase",
		},
		{
			err:  SetTopLevelMsg(Wrap(Wrap(New(base), msg1), msg2), top1),
			want: "top level message 1\nFull error:\nmessage 2\n\tcaused by:\nmessage 1\n\tcaused by:\nbase",
		},
	}

	for _, test := range tests {
		if be, ok := test.err.(*beamError); ok {
			if got, want := be.Error(), test.want; got != want {
				t.Errorf("Incorrect formatting. Want: %q, Got: %q", want, got)
			}
		} else {
			t.Errorf("Error should be type *beamError, got: %q", test.err)
		}
	}
}

// getStructure extracts the structure of an error, outputting a slice that
// represents the nested messages in that error in the order they are output
// and with the type of message (context or error) described.
func getStructure(e error) errorStructure {
	if e == nil {
		return nil
	}
	var structure errorStructure

	for {
		if be, ok := e.(*beamError); ok {
			if be.context != "" {
				structure = append(structure, entry{CONTEXT, be.context})
			}
			if be.msg != "" {
				structure = append(structure, entry{ERROR, be.msg})
			}
			// Continue by iterating into the cause of the error.
			if be.cause != nil {
				e = be.cause
			} else {
				return structure
			}
		} else { // Not a beamError.
			structure = append(structure, entry{ERROR, e.Error()})
			return structure
		}
	}
}

func equalStructure(left errorStructure, right errorStructure) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}

	if len(left) != len(right) {
		return false
	}

	for i := 0; i < len(left); i++ {
		if left[i].t != right[i].t || left[i].msg != right[i].msg {
			return false
		}
	}

	return true
}

type msgType int

const (
	ERROR msgType = iota
	CONTEXT
)

type entry struct {
	t   msgType
	msg string
}

type errorStructure []entry
