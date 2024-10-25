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

package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/ioutilx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"google.golang.org/protobuf/encoding/prototext"
)

// leafCoders lists coder urns the runner knows how to manipulate.
// In particular, ones that won't be a problem to parse, in general
// because they have a known total size.
var leafCoders = map[string]struct{}{
	urns.CoderBytes:          {},
	urns.CoderStringUTF8:     {},
	urns.CoderLengthPrefix:   {},
	urns.CoderVarInt:         {},
	urns.CoderDouble:         {},
	urns.CoderBool:           {},
	urns.CoderGlobalWindow:   {},
	urns.CoderIntervalWindow: {},
}

func isLeafCoder(c *pipepb.Coder) bool {
	_, ok := leafCoders[c.GetSpec().GetUrn()]
	return ok
}

// makeWindowedValueCoder gets the coder for the PCollection, renders it safe, and adds it to the coders map.
//
// PCollection coders are not inherently WindowValueCoder wrapped, and they are added by the runner
// for crossing the FnAPI boundary at data sources and data sinks.
func makeWindowedValueCoder(pID string, comps *pipepb.Components, coders map[string]*pipepb.Coder) (string, error) {
	col := comps.GetPcollections()[pID]
	cID, err := lpUnknownCoders(col.GetCoderId(), coders, comps.GetCoders())
	if err != nil {
		return "", fmt.Errorf("makeWindowedValueCoder: couldn't process coder for pcollection %q %v: %w", pID, prototext.Format(col), err)
	}
	wcID := comps.GetWindowingStrategies()[col.GetWindowingStrategyId()].GetWindowCoderId()

	// The runner needs to be defensive, and tell the SDK to Length Prefix
	// any coders that it doesn't understand.
	// So here, we look at the coder and its components, and produce
	// new coders that we know how to deal with.

	// Produce ID for the Windowed Value Coder
	wvcID := "cwv_" + pID
	wInC := &pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn: urns.CoderWindowedValue,
		},
		ComponentCoderIds: []string{cID, wcID},
	}
	// Populate the coders to send with the new windowed value coder.
	coders[wvcID] = wInC
	return wvcID, nil
}

// makeWindowCoders categorizes and provides the encoder, decoder pair for the type of window.
func makeWindowCoders(wc *pipepb.Coder) (engine.WinCoderType, exec.WindowDecoder, exec.WindowEncoder) {
	var cwc *coder.WindowCoder
	var winCoder engine.WinCoderType
	switch wc.GetSpec().GetUrn() {
	case urns.CoderGlobalWindow:
		winCoder = engine.WinGlobal
		cwc = coder.NewGlobalWindow()
	case urns.CoderIntervalWindow:
		winCoder = engine.WinInterval
		cwc = coder.NewIntervalWindow()
	default:
		// TODO(https://github.com/apache/beam/issues/31921): Support custom windowfns instead of panicking here.
		winCoder = engine.WinCustom
		slog.LogAttrs(context.TODO(), slog.LevelError, "makeWindowCoders: unknown urn", slog.String("urn", wc.GetSpec().GetUrn()))
		panic(fmt.Sprintf("makeWindowCoders, unknown urn: %v", prototext.Format(wc)))
	}
	return winCoder, exec.MakeWindowDecoder(cwc), exec.MakeWindowEncoder(cwc)
}

// lpUnknownCoders takes a coder, and populates coders with any new coders
// coders that the runner needs to be safe, and speedy.
// It returns either the passed in coder id, or the new safe coder id.
func lpUnknownCoders(cID string, bundle, base map[string]*pipepb.Coder) (string, error) {
	// First check if we've already added the LP version of this coder to coders already.
	lpcID := cID + "_lp"
	// Check if we've done this one before.
	if _, ok := bundle[lpcID]; ok {
		return lpcID, nil
	}
	// All coders in the coders map have been processed.
	if _, ok := bundle[cID]; ok {
		return cID, nil
	}
	// Look up the canonical location.
	c, ok := base[cID]
	if !ok {
		// We messed up somewhere.
		return "", fmt.Errorf("lpUnknownCoders: coder %q not present in base map", cID)
	}
	// Add the original coder to the coders map.
	bundle[cID] = c
	// If we don't know this coder, and it has no sub components,
	// we must LP it, and we return the LP'd version.
	leaf := isLeafCoder(c)
	if len(c.GetComponentCoderIds()) == 0 && !leaf {
		lpc := &pipepb.Coder{
			Spec: &pipepb.FunctionSpec{
				Urn: urns.CoderLengthPrefix,
			},
			ComponentCoderIds: []string{cID},
		}
		bundle[lpcID] = lpc
		return lpcID, nil
	}
	// We know we have a composite, so if we count this as a leaf, move everything to
	// the coders map.
	if leaf {
		// Copy the components from the base.
		for _, cc := range c.GetComponentCoderIds() {
			bundle[cc] = base[cc]
		}
		return cID, nil
	}
	var needNewComposite bool
	var comps []string
	for i, cc := range c.GetComponentCoderIds() {
		rcc, err := lpUnknownCoders(cc, bundle, base)
		if err != nil {
			return "", fmt.Errorf("lpUnknownCoders: couldn't handle component %d %q of %q %v:\n%w", i, cc, cID, prototext.Format(c), err)
		}
		if cc != rcc {
			needNewComposite = true
		}
		comps = append(comps, rcc)
	}
	if needNewComposite {
		lpc := &pipepb.Coder{
			Spec:              c.GetSpec(),
			ComponentCoderIds: comps,
		}
		bundle[lpcID] = lpc
		return lpcID, nil
	}
	return cID, nil
}

// reconcileCoders ensures that the bundle coders are primed with initial coders from
// the base pipeline components.
func reconcileCoders(bundle, base map[string]*pipepb.Coder) {
	for {
		var comps []string
		for _, c := range bundle {
			for _, ccid := range c.GetComponentCoderIds() {
				if _, ok := bundle[ccid]; !ok {
					// We don't have the coder yet, so in we go.
					comps = append(comps, ccid)
				}
			}
		}
		if len(comps) == 0 {
			return
		}
		for _, ccid := range comps {
			c, ok := base[ccid]
			if !ok {
				panic(fmt.Sprintf("unknown coder id during reconciliation: %v", ccid))
			}
			bundle[ccid] = c
		}
	}
}

// pullDecoder return a function that will extract the bytes
// for the associated coder. Uses a buffer and a TeeReader to extract the original
// bytes from when decoding elements.
func pullDecoder(c *pipepb.Coder, coders map[string]*pipepb.Coder) func(io.Reader) []byte {
	dec := pullDecoderNoAlloc(c, coders)
	return func(r io.Reader) []byte {
		var buf bytes.Buffer
		tr := io.TeeReader(r, &buf)
		dec(tr)
		return buf.Bytes()
	}
}

// pullDecoderNoAlloc returns a function that decodes a single element of the given coder.
// Intended to only be used as an internal function for pullDecoder, which will use a io.TeeReader
// to extract the bytes.
func pullDecoderNoAlloc(c *pipepb.Coder, coders map[string]*pipepb.Coder) func(io.Reader) {
	urn := c.GetSpec().GetUrn()
	switch urn {
	// Anything length prefixed can be treated as opaque.
	case urns.CoderBytes, urns.CoderStringUTF8, urns.CoderLengthPrefix:
		return func(r io.Reader) {
			l, _ := coder.DecodeVarInt(r)
			ioutilx.ReadN(r, int(l))
		}
	case urns.CoderNullable:
		return func(r io.Reader) {
			b, _ := ioutilx.ReadN(r, 1)
			if len(b) == 0 {
				return
			}
			// Nullable coder is prefixed with 0 or 1 to indicate whether there exists remaining data.
			prefix := b[0]
			if prefix == 0 {
				return
			}
			l, _ := coder.DecodeVarInt(r)
			ioutilx.ReadN(r, int(l))
		}
	case urns.CoderVarInt:
		return func(r io.Reader) {
			coder.DecodeVarInt(r)
		}
	case urns.CoderBool:
		return func(r io.Reader) {
			coder.DecodeBool(r)
		}
	case urns.CoderDouble:
		return func(r io.Reader) {
			coder.DecodeDouble(r)
		}
	case urns.CoderIterable:
		ccids := c.GetComponentCoderIds()
		ed := pullDecoderNoAlloc(coders[ccids[0]], coders)
		return func(r io.Reader) {
			l, _ := coder.DecodeInt32(r)
			for i := int32(0); i < l; i++ {
				ed(r)
			}
		}

	case urns.CoderKV:
		ccids := c.GetComponentCoderIds()
		if len(ccids) != 2 {
			panic(fmt.Sprintf("KV coder with more than 2 components: %s", prototext.Format(c)))
		}
		kd := pullDecoderNoAlloc(coders[ccids[0]], coders)
		vd := pullDecoderNoAlloc(coders[ccids[1]], coders)
		return func(r io.Reader) {
			kd(r)
			vd(r)
		}
	case urns.CoderRow:
		panic(fmt.Sprintf("Runner forgot to LP this Row Coder. %v", prototext.Format(c)))
	default:
		panic(fmt.Sprintf("unknown coder urn key: %v", urn))
	}
}

// debugCoder is developer code to get the structure of a proto coder visible when
// debugging coder errors in prism. It may sometimes be unused, so we do this to avoid
// linting errors.
var _ = debugCoder

func debugCoder(cid string, coders map[string]*pipepb.Coder) string {
	var b strings.Builder
	b.WriteString(cid)
	b.WriteRune('\n')
	c := coders[cid]
	if len(c.ComponentCoderIds) > 0 {
		b.WriteRune('\t')
		b.WriteString(strings.Join(c.ComponentCoderIds, ", "))
		b.WriteRune('\n')
		for _, ccid := range c.GetComponentCoderIds() {
			b.WriteString(debugCoder(ccid, coders))
		}
	}
	return b.String()
}
