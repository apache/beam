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

package exec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"google.golang.org/protobuf/encoding/protowire"
)

type stateProvider struct {
	ctx        context.Context
	sr         StateReader
	SID        StreamID
	elementKey []byte
	window     []byte

	transactionsByKey        map[string][]state.Transaction
	initialValueByKey        map[string]any
	initialBagByKey          map[string][]any
	blindBagWriteCountsByKey map[string]int // Tracks blind writes to bags before a read.
	initialMapValuesByKey    map[string]map[string]any
	initialMapKeysByKey      map[string][]any
	initialOrderedListByKey  map[string][]any
	readersByKey             map[string]io.ReadCloser
	appendersByKey           map[string]io.Writer
	clearersByKey            map[string]io.Writer
	codersByKey              map[string]*coder.Coder
	keyCodersByID            map[string]*coder.Coder
	combineFnsByKey          map[string]*graph.CombineFn
}

// ReadValueState reads a value state from the State API
func (s *stateProvider) ReadValueState(userStateID string) (any, []state.Transaction, error) {
	initialValue, ok := s.initialValueByKey[userStateID]
	if !ok {
		rw, err := s.getBagReader(userStateID)
		if err != nil {
			return nil, nil, err
		}
		dec := MakeElementDecoder(coder.SkipW(s.codersByKey[userStateID]))
		resp, err := dec.Decode(rw)
		if err != nil && err != io.EOF {
			return nil, nil, err
		}
		if resp == nil {
			return nil, []state.Transaction{}, nil
		}
		initialValue = resp.Elm
		s.initialValueByKey[userStateID] = initialValue
	}

	transactions, ok := s.transactionsByKey[userStateID]
	if !ok {
		transactions = []state.Transaction{}
	}

	return initialValue, transactions, nil
}

// WriteValueState writes a value state to the State API
// For value states, this is done by clearing a bag state and writing a value to it.
func (s *stateProvider) WriteValueState(val state.Transaction) error {
	cl, err := s.getBagClearer(val.Key)
	if err != nil {
		return err
	}
	_, err = cl.Write([]byte{})
	if err != nil {
		return err
	}

	ap, err := s.getBagAppender(val.Key)
	if err != nil {
		return err
	}
	fv := FullValue{Elm: val.Val}
	enc := MakeElementEncoder(coder.SkipW(s.codersByKey[val.Key]))
	err = enc.Encode(&fv, ap)
	if err != nil {
		return err
	}

	// Any transactions before a set don't matter
	s.transactionsByKey[val.Key] = []state.Transaction{val}

	return nil
}

// ClearValueState clears a value state from the State API.
func (s *stateProvider) ClearValueState(val state.Transaction) error {
	cl, err := s.getBagClearer(val.Key)
	if err != nil {
		return err
	}
	_, err = cl.Write([]byte{})
	if err != nil {
		return err
	}

	// Any transactions before a clear don't matter
	s.transactionsByKey[val.Key] = []state.Transaction{val}

	return nil
}

// ReadBagState reads a bag state from the State API
func (s *stateProvider) ReadBagState(userStateID string) ([]any, []state.Transaction, error) {
	initialValue, ok := s.initialBagByKey[userStateID]
	if !ok {
		initialValue = []any{}
		rw, err := s.getBagReader(userStateID)
		if err != nil {
			return nil, nil, err
		}
		dec := MakeElementDecoder(coder.SkipW(s.codersByKey[userStateID]))
		for err == nil {
			var resp *FullValue
			resp, err = dec.Decode(rw)
			if err == nil {
				initialValue = append(initialValue, resp.Elm)
			} else if err != io.EOF {
				return nil, nil, err
			}
		}
		s.initialBagByKey[userStateID] = initialValue
	}

	transactions, ok := s.transactionsByKey[userStateID]
	if !ok {
		transactions = []state.Transaction{}
	}
	// If there were blind writes before this read, trim the transactions.
	// These don't need to be reset, unless a clear happens.
	if s.blindBagWriteCountsByKey[userStateID] > 0 {
		// Trim blind writes from the transaction queue, to avoid re-applying them.
		transactions = transactions[s.blindBagWriteCountsByKey[userStateID]:]
	}

	return initialValue, transactions, nil
}

// ClearBagState clears a bag state from the State API
func (s *stateProvider) ClearBagState(val state.Transaction) error {
	cl, err := s.getBagClearer(val.Key)
	if err != nil {
		return err
	}
	_, err = cl.Write([]byte{})
	if err != nil {
		return err
	}

	// Any transactions before a clear don't matter
	s.transactionsByKey[val.Key] = []state.Transaction{val}
	s.blindBagWriteCountsByKey[val.Key] = 1 // To account for the clear.

	return nil
}

// WriteBagState writes a bag state to the State API
func (s *stateProvider) WriteBagState(val state.Transaction) error {
	_, ok := s.initialBagByKey[val.Key]
	if !ok {
		s.blindBagWriteCountsByKey[val.Key]++
	}
	ap, err := s.getBagAppender(val.Key)
	if err != nil {
		return err
	}
	fv := FullValue{Elm: val.Val}
	enc := MakeElementEncoder(coder.SkipW(s.codersByKey[val.Key]))
	err = enc.Encode(&fv, ap)
	if err != nil {
		return err
	}

	if transactions, ok := s.transactionsByKey[val.Key]; ok {
		transactions = append(transactions, val)
		s.transactionsByKey[val.Key] = transactions
	} else {
		s.transactionsByKey[val.Key] = []state.Transaction{val}
	}

	return nil
}

// ReadMapStateValue reads a value from the map state for a given key.
func (s *stateProvider) ReadMapStateValue(userStateID string, key any) (any, []state.Transaction, error) {
	_, ok := s.initialMapValuesByKey[userStateID]
	if !ok {
		s.initialMapValuesByKey[userStateID] = make(map[string]any)
	}
	b, err := s.encodeKey(userStateID, key)
	if err != nil {
		return nil, nil, err
	}
	initialValue, ok := s.initialMapValuesByKey[userStateID][string(b)]
	if !ok {
		rw, err := s.getMultiMapReader(userStateID, key)
		if err != nil {
			return nil, nil, err
		}
		dec := MakeElementDecoder(coder.SkipW(s.codersByKey[userStateID]))
		resp, err := dec.Decode(rw)
		if err != nil && err != io.EOF {
			return nil, nil, err
		}
		if resp == nil {
			return nil, []state.Transaction{}, nil
		}
		initialValue = resp.Elm
		s.initialValueByKey[userStateID] = initialValue
	}

	transactions, ok := s.transactionsByKey[userStateID]
	if !ok {
		transactions = []state.Transaction{}
	}

	return initialValue, transactions, nil
}

// ReadMapStateKeys reads all the keys in a map state.
func (s *stateProvider) ReadMapStateKeys(userStateID string) ([]any, []state.Transaction, error) {
	initialValue, ok := s.initialMapKeysByKey[userStateID]
	if !ok {
		initialValue = []any{}
		rw, err := s.getMultiMapKeyReader(userStateID)
		if err != nil {
			return nil, nil, err
		}
		dec := MakeElementDecoder(coder.SkipW(s.keyCodersByID[userStateID]))
		for err == nil {
			var resp *FullValue
			resp, err = dec.Decode(rw)
			if err == nil {
				initialValue = append(initialValue, resp.Elm)
			} else if err != io.EOF {
				return nil, nil, err
			}
		}
		s.initialMapKeysByKey[userStateID] = initialValue
	}

	transactions, ok := s.transactionsByKey[userStateID]
	if !ok {
		transactions = []state.Transaction{}
	}

	return initialValue, transactions, nil
}

// WriteMapState writes a key value pair to the global map state.
func (s *stateProvider) WriteMapState(val state.Transaction) error {
	cl, err := s.getMultiMapKeyClearer(val.Key, val.MapKey)
	if err != nil {
		return err
	}
	_, err = cl.Write([]byte{})
	if err != nil {
		return err
	}

	ap, err := s.getMultiMapAppender(val.Key, val.MapKey)
	if err != nil {
		return err
	}
	fv := FullValue{Elm: val.Val}
	enc := MakeElementEncoder(coder.SkipW(s.codersByKey[val.Key]))
	err = enc.Encode(&fv, ap)
	if err != nil {
		return err
	}

	if transactions, ok := s.transactionsByKey[val.Key]; ok {
		transactions = append(transactions, val)
		s.transactionsByKey[val.Key] = transactions
	} else {
		s.transactionsByKey[val.Key] = []state.Transaction{val}
	}

	return nil
}

// ClearMapStateKey deletes a key value pair from the global map state.
func (s *stateProvider) ClearMapStateKey(val state.Transaction) error {
	cl, err := s.getMultiMapKeyClearer(val.Key, val.MapKey)
	if err != nil {
		return err
	}
	_, err = cl.Write([]byte{})
	if err != nil {
		return err
	}

	if transactions, ok := s.transactionsByKey[val.Key]; ok {
		transactions = append(transactions, val)
		s.transactionsByKey[val.Key] = transactions
	} else {
		s.transactionsByKey[val.Key] = []state.Transaction{val}
	}

	return nil
}

// ClearMapState deletes all key value pairs from the global map state.
func (s *stateProvider) ClearMapState(val state.Transaction) error {
	cl, err := s.getMultiMapClearer(val.Key)
	if err != nil {
		return err
	}
	_, err = cl.Write([]byte{})
	if err != nil {
		return err
	}

	// Any transactions before a clear don't matter
	s.transactionsByKey[val.Key] = []state.Transaction{val}

	return nil
}

func (s *stateProvider) CreateAccumulatorFn(userStateID string) reflectx.Func {
	a := s.combineFnsByKey[userStateID]
	if ca := a.CreateAccumulatorFn(); ca != nil {
		return ca.Fn
	}
	return nil
}

func (s *stateProvider) AddInputFn(userStateID string) reflectx.Func {
	a := s.combineFnsByKey[userStateID]
	if ai := a.AddInputFn(); ai != nil {
		return ai.Fn
	}

	return nil
}

func (s *stateProvider) MergeAccumulatorsFn(userStateID string) reflectx.Func {
	a := s.combineFnsByKey[userStateID]
	if ma := a.MergeAccumulatorsFn(); ma != nil {
		return ma.Fn
	}

	return nil
}

func (s *stateProvider) ExtractOutputFn(userStateID string) reflectx.Func {
	a := s.combineFnsByKey[userStateID]
	if eo := a.ExtractOutputFn(); eo != nil {
		return eo.Fn
	}
	return nil
}

func (s *stateProvider) getBagReader(userStateID string) (io.ReadCloser, error) {
	if r, ok := s.readersByKey[userStateID]; ok {
		return r, nil
	}
	r, err := s.sr.OpenBagUserStateReader(s.ctx, s.SID, userStateID, s.elementKey, s.window)
	if err != nil {
		return nil, err
	}
	s.readersByKey[userStateID] = r
	return s.readersByKey[userStateID], nil
}

func (s *stateProvider) getBagAppender(userStateID string) (io.Writer, error) {
	if w, ok := s.appendersByKey[userStateID]; ok {
		return w, nil
	}
	w, err := s.sr.OpenBagUserStateAppender(s.ctx, s.SID, userStateID, s.elementKey, s.window)
	if err != nil {
		return nil, err
	}
	s.appendersByKey[userStateID] = w
	return s.appendersByKey[userStateID], nil
}

func (s *stateProvider) getBagClearer(userStateID string) (io.Writer, error) {
	if w, ok := s.clearersByKey[userStateID]; ok {
		return w, nil
	}
	w, err := s.sr.OpenBagUserStateClearer(s.ctx, s.SID, userStateID, s.elementKey, s.window)
	if err != nil {
		return nil, err
	}
	s.clearersByKey[userStateID] = w
	return s.clearersByKey[userStateID], nil
}

func (s *stateProvider) getMultiMapReader(userStateID string, key any) (io.ReadCloser, error) {
	ek, err := s.encodeKey(userStateID, key)
	if err != nil {
		return nil, err
	}
	r, err := s.sr.OpenMultimapUserStateReader(s.ctx, s.SID, userStateID, s.elementKey, s.window, ek)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s *stateProvider) getMultiMapAppender(userStateID string, key any) (io.Writer, error) {
	ek, err := s.encodeKey(userStateID, key)
	if err != nil {
		return nil, err
	}
	w, err := s.sr.OpenMultimapUserStateAppender(s.ctx, s.SID, userStateID, s.elementKey, s.window, ek)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *stateProvider) getMultiMapKeyClearer(userStateID string, key any) (io.Writer, error) {
	ek, err := s.encodeKey(userStateID, key)
	if err != nil {
		return nil, err
	}
	w, err := s.sr.OpenMultimapUserStateClearer(s.ctx, s.SID, userStateID, s.elementKey, s.window, ek)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *stateProvider) getMultiMapClearer(userStateID string) (io.Writer, error) {
	w, err := s.sr.OpenMultimapKeysUserStateClearer(s.ctx, s.SID, userStateID, s.elementKey, s.window)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *stateProvider) getMultiMapKeyReader(userStateID string) (io.ReadCloser, error) {
	if r, ok := s.readersByKey[userStateID]; ok {
		return r, nil
	}
	r, err := s.sr.OpenMultimapKeysUserStateReader(s.ctx, s.SID, userStateID, s.elementKey, s.window)
	if err != nil {
		return nil, err
	}
	s.readersByKey[userStateID] = r
	return s.readersByKey[userStateID], nil
}

// ReadOrderedListState reads an ordered list state from the State API.
// It fetches the full range on first access and caches the result.
func (s *stateProvider) ReadOrderedListState(userStateID string) ([]any, []state.Transaction, error) {
	initialValue, ok := s.initialOrderedListByKey[userStateID]
	if !ok {
		initialValue = []any{}
		rw, err := s.getOrderedListReader(userStateID, math.MinInt64, math.MaxInt64)
		if err != nil {
			return nil, nil, err
		}
		for {
			entry, err := decodeOrderedListEntry(rw, s.codersByKey[userStateID])
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, nil, err
			}
			initialValue = append(initialValue, entry)
		}
		s.initialOrderedListByKey[userStateID] = initialValue
	}

	transactions, ok := s.transactionsByKey[userStateID]
	if !ok {
		transactions = []state.Transaction{}
	}

	return initialValue, transactions, nil
}

// WriteOrderedListState writes a single entry to the ordered list state.
// The wire format is: varint(sortKey) || coder_encoded(value).
func (s *stateProvider) WriteOrderedListState(val state.Transaction) error {
	ap, err := s.getOrderedListAppender(val.Key)
	if err != nil {
		return err
	}

	sortKey := val.MapKey.(int64)
	if err := encodeOrderedListEntry(sortKey, val.Val, ap, s.codersByKey[val.Key]); err != nil {
		return err
	}

	if transactions, ok := s.transactionsByKey[val.Key]; ok {
		s.transactionsByKey[val.Key] = append(transactions, val)
	} else {
		s.transactionsByKey[val.Key] = []state.Transaction{val}
	}

	return nil
}

// ClearOrderedListState clears entries in a range from the ordered list state.
func (s *stateProvider) ClearOrderedListState(val state.Transaction) error {
	r := val.MapKey.([2]int64)
	cl, err := s.getOrderedListClearer(val.Key, r[0], r[1])
	if err != nil {
		return err
	}
	_, err = cl.Write([]byte{})
	if err != nil {
		return err
	}

	if transactions, ok := s.transactionsByKey[val.Key]; ok {
		s.transactionsByKey[val.Key] = append(transactions, val)
	} else {
		s.transactionsByKey[val.Key] = []state.Transaction{val}
	}

	return nil
}

func (s *stateProvider) getOrderedListReader(userStateID string, start, end int64) (io.ReadCloser, error) {
	r, err := s.sr.OpenOrderedListUserStateReader(s.ctx, s.SID, userStateID, s.elementKey, s.window, start, end)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s *stateProvider) getOrderedListAppender(userStateID string) (io.Writer, error) {
	w, err := s.sr.OpenOrderedListUserStateAppender(s.ctx, s.SID, userStateID, s.elementKey, s.window)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *stateProvider) getOrderedListClearer(userStateID string, start, end int64) (io.Writer, error) {
	w, err := s.sr.OpenOrderedListUserStateClearer(s.ctx, s.SID, userStateID, s.elementKey, s.window, start, end)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// encodeOrderedListEntry writes varint(uint64(sortKey)) || coder_encoded(value) to w.
// The entire entry is buffered before writing so that each w.Write call
// delivers a complete entry (important when w is a stateKeyWriter that
// sends each Write as a separate gRPC Append request).
func encodeOrderedListEntry(sortKey int64, val any, w io.Writer, c *coder.Coder) error {
	var buf bytes.Buffer
	b := protowire.AppendVarint(nil, uint64(sortKey))
	buf.Write(b)
	fv := FullValue{Elm: val}
	enc := MakeElementEncoder(coder.SkipW(c))
	if err := enc.Encode(&fv, &buf); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}

// decodeOrderedListEntry reads varint(sortKey) || coder_encoded(value) from r.
func decodeOrderedListEntry(r io.Reader, c *coder.Coder) (state.OrderedListEntry, error) {
	// Read varint byte-by-byte.
	var buf [10]byte // max varint size
	var n int
	for n = 0; n < len(buf); n++ {
		_, err := r.Read(buf[n : n+1])
		if err != nil {
			if n == 0 {
				return state.OrderedListEntry{}, err
			}
			return state.OrderedListEntry{}, fmt.Errorf("unexpected error reading varint: %w", err)
		}
		if buf[n]&0x80 == 0 {
			n++
			break
		}
	}
	sortKey, consumed := protowire.ConsumeVarint(buf[:n])
	if consumed < 0 {
		return state.OrderedListEntry{}, fmt.Errorf("invalid varint in ordered list entry")
	}

	dec := MakeElementDecoder(coder.SkipW(c))
	fv, err := dec.Decode(r)
	if err != nil {
		return state.OrderedListEntry{}, err
	}
	return state.OrderedListEntry{SortKey: int64(sortKey), Value: fv.Elm}, nil
}

func (s *stateProvider) encodeKey(userStateID string, key any) ([]byte, error) {
	fv := FullValue{Elm: key}
	enc := MakeElementEncoder(coder.SkipW(s.keyCodersByID[userStateID]))
	var b bytes.Buffer
	err := enc.Encode(&fv, &b)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// UserStateAdapter provides a state provider to be used for user state.
type UserStateAdapter interface {
	NewStateProvider(ctx context.Context, reader StateReader, w typex.Window, element any) (stateProvider, error)
}

type userStateAdapter struct {
	sid                StreamID
	wc                 WindowEncoder
	kc                 ElementEncoder
	stateIDToCoder     map[string]*coder.Coder
	stateIDToKeyCoder  map[string]*coder.Coder
	stateIDToCombineFn map[string]*graph.CombineFn
	c                  *coder.Coder
}

// NewUserStateAdapter returns a user state adapter for the given StreamID and coder.
// It expects a W<V> or W<KV<K,V>> coder, because the protocol requires windowing information.
func NewUserStateAdapter(sid StreamID, c *coder.Coder, stateIDToCoder map[string]*coder.Coder, stateIDToKeyCoder map[string]*coder.Coder, stateIDToCombineFn map[string]*graph.CombineFn) UserStateAdapter {
	if !coder.IsW(c) {
		panic(fmt.Sprintf("expected WV coder for user state %v: %v", sid, c))
	}

	wc := MakeWindowEncoder(c.Window)
	var kc ElementEncoder
	if coder.IsKV(coder.SkipW(c)) {
		kc = MakeElementEncoder(coder.SkipW(c).Components[0])
	}
	return &userStateAdapter{sid: sid, wc: wc, kc: kc, c: c, stateIDToCoder: stateIDToCoder, stateIDToKeyCoder: stateIDToKeyCoder, stateIDToCombineFn: stateIDToCombineFn}
}

// NewStateProvider creates a stateProvider with the ability to talk to the state API.
func (s *userStateAdapter) NewStateProvider(ctx context.Context, reader StateReader, w typex.Window, element any) (stateProvider, error) {
	if s.kc == nil {
		return stateProvider{}, fmt.Errorf("cannot make a state provider for an unkeyed input %v", element)
	}
	elementKey, err := EncodeElement(s.kc, element.(*MainInput).Key.Elm)
	if err != nil {
		return stateProvider{}, err
	}

	win, err := EncodeWindow(s.wc, w)
	if err != nil {
		return stateProvider{}, err
	}
	sp := stateProvider{
		ctx:                      ctx,
		sr:                       reader,
		SID:                      s.sid,
		elementKey:               elementKey,
		window:                   win,
		transactionsByKey:        make(map[string][]state.Transaction),
		initialValueByKey:        make(map[string]any),
		initialBagByKey:          make(map[string][]any),
		blindBagWriteCountsByKey: make(map[string]int),
		initialMapValuesByKey:    make(map[string]map[string]any),
		initialMapKeysByKey:      make(map[string][]any),
		initialOrderedListByKey:  make(map[string][]any),
		readersByKey:             make(map[string]io.ReadCloser),
		appendersByKey:           make(map[string]io.Writer),
		clearersByKey:            make(map[string]io.Writer),
		combineFnsByKey:          s.stateIDToCombineFn,
		codersByKey:              s.stateIDToCoder,
		keyCodersByID:            s.stateIDToKeyCoder,
	}

	return sp, nil
}
