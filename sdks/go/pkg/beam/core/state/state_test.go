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

package state

import (
	"errors"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

var (
	errFake = errors.New("fake error")
)

type fakeProvider struct {
	initialState      map[string]any
	initialBagState   map[string][]any
	initialMapState   map[string]map[string]any
	transactions      map[string][]Transaction
	err               map[string]error
	createAccumForKey map[string]bool
	addInputForKey    map[string]bool
	mergeAccumForKey  map[string]bool
	extractOutForKey  map[string]bool
}

func (s *fakeProvider) ReadValueState(userStateID string) (any, []Transaction, error) {
	if err, ok := s.err[userStateID]; ok {
		return nil, nil, err
	}
	base := s.initialState[userStateID]
	trans, ok := s.transactions[userStateID]
	if !ok {
		trans = []Transaction{}
	}
	return base, trans, nil
}

func (s *fakeProvider) WriteValueState(val Transaction) error {
	s.transactions[val.Key] = []Transaction{val}
	return nil
}

func (s *fakeProvider) ClearValueState(val Transaction) error {
	s.transactions[val.Key] = []Transaction{val}
	return nil
}

func (s *fakeProvider) ReadBagState(userStateID string) ([]any, []Transaction, error) {
	if err, ok := s.err[userStateID]; ok {
		return nil, nil, err
	}
	base := s.initialBagState[userStateID]
	trans, ok := s.transactions[userStateID]
	if !ok {
		trans = []Transaction{}
	}
	return base, trans, nil
}

func (s *fakeProvider) WriteBagState(val Transaction) error {
	if transactions, ok := s.transactions[val.Key]; ok {
		s.transactions[val.Key] = append(transactions, val)
	} else {
		s.transactions[val.Key] = []Transaction{val}
	}
	return nil
}

func (s *fakeProvider) ClearBagState(val Transaction) error {
	s.transactions[val.Key] = []Transaction{val}
	return nil
}

func (s *fakeProvider) CreateAccumulatorFn(userStateID string) reflectx.Func {
	if s.createAccumForKey[userStateID] {
		return reflectx.MakeFunc0x1(func() int {
			return 1
		})
	}

	return nil
}
func (s *fakeProvider) AddInputFn(userStateID string) reflectx.Func {
	if s.addInputForKey[userStateID] {
		return reflectx.MakeFunc2x1(func(a, b int) int {
			return a + b
		})
	}

	return nil
}
func (s *fakeProvider) MergeAccumulatorsFn(userStateID string) reflectx.Func {
	if s.mergeAccumForKey[userStateID] {
		return reflectx.MakeFunc2x1(func(a, b int) int {
			return a + b
		})
	}

	return nil
}
func (s *fakeProvider) ExtractOutputFn(userStateID string) reflectx.Func {
	if s.extractOutForKey[userStateID] {
		return reflectx.MakeFunc1x1(func(a int) int {
			return a * 100
		})
	}

	return nil
}

func (s *fakeProvider) ReadMapStateValue(userStateID string, key any) (any, []Transaction, error) {
	keyString := key.(string)
	if err, ok := s.err[userStateID]; ok {
		return nil, nil, err
	}
	base := s.initialMapState[userStateID][keyString]
	trans, ok := s.transactions[userStateID]
	if !ok {
		trans = []Transaction{}
	}
	return base, trans, nil
}

func (s *fakeProvider) ReadMapStateKeys(userStateID string) ([]any, []Transaction, error) {
	if err, ok := s.err[userStateID]; ok {
		return nil, nil, err
	}
	base := s.initialMapState[userStateID]
	keys := make([]any, len(base))
	i := 0
	for k := range base {
		keys[i] = k
		i++
	}
	trans, ok := s.transactions[userStateID]
	if !ok {
		trans = []Transaction{}
	}
	return keys, trans, nil
}

func (s *fakeProvider) WriteMapState(val Transaction) error {
	if transactions, ok := s.transactions[val.Key]; ok {
		s.transactions[val.Key] = append(transactions, val)
	} else {
		s.transactions[val.Key] = []Transaction{val}
	}
	return nil
}

func (s *fakeProvider) ClearMapStateKey(val Transaction) error {
	if transactions, ok := s.transactions[val.Key]; ok {
		s.transactions[val.Key] = append(transactions, val)
	} else {
		s.transactions[val.Key] = []Transaction{val}
	}
	return nil
}

func (s *fakeProvider) ClearMapState(val Transaction) error {
	s.transactions[val.Key] = []Transaction{val}
	return nil
}

func TestValueRead(t *testing.T) {
	is := make(map[string]any)
	ts := make(map[string][]Transaction)
	es := make(map[string]error)
	is["no_transactions"] = 1
	ts["no_transactions"] = nil
	is["basic_set"] = 1
	ts["basic_set"] = []Transaction{{Key: "basic_set", Type: TransactionTypeSet, Val: 3}}
	is["basic_clear"] = 1
	ts["basic_clear"] = []Transaction{{Key: "basic_clear", Type: TransactionTypeClear, Val: nil}}
	is["set_then_clear"] = 1
	ts["set_then_clear"] = []Transaction{{Key: "set_then_clear", Type: TransactionTypeSet, Val: 3}, {Key: "set_then_clear", Type: TransactionTypeClear, Val: nil}}
	is["set_then_clear_then_set"] = 1
	ts["set_then_clear_then_set"] = []Transaction{{Key: "set_then_clear_then_set", Type: TransactionTypeSet, Val: 3}, {Key: "set_then_clear_then_set", Type: TransactionTypeClear, Val: nil}, {Key: "set_then_clear_then_set", Type: TransactionTypeSet, Val: 4}}
	is["err"] = 1
	ts["err"] = []Transaction{{Key: "err", Type: TransactionTypeSet, Val: 3}}
	es["err"] = errFake

	f := fakeProvider{
		initialState: is,
		transactions: ts,
		err:          es,
	}

	var tests = []struct {
		vs  Value[int]
		val int
		ok  bool
		err error
	}{
		{MakeValueState[int]("no_transactions"), 1, true, nil},
		{MakeValueState[int]("basic_set"), 3, true, nil},
		{MakeValueState[int]("basic_clear"), 0, false, nil},
		{MakeValueState[int]("set_then_clear"), 0, false, nil},
		{MakeValueState[int]("set_then_clear_then_set"), 4, true, nil},
		{MakeValueState[int]("err"), 0, false, errFake},
	}

	for _, tt := range tests {
		val, ok, err := tt.vs.Read(&f)
		if err != nil && tt.err == nil {
			t.Errorf("Value.Read() returned error %v for state key %v when it shouldn't have", err, tt.vs.Key)
		} else if err == nil && tt.err != nil {
			t.Errorf("Value.Read() returned no error for state key %v when it should have returned %v", tt.vs.Key, err)
		} else if ok && !tt.ok {
			t.Errorf("Value.Read() returned a value %v for state key %v when it shouldn't have", val, tt.vs.Key)
		} else if !ok && tt.ok {
			t.Errorf("Value.Read() didn't return a value for state key %v when it should have returned %v", tt.vs.Key, tt.val)
		} else if val != tt.val {
			t.Errorf("Value.Read()=%v, want %v for state key %v", val, tt.val, tt.vs.Key)
		}
	}
}

func TestValueWrite(t *testing.T) {
	var tests = []struct {
		writes []int
		val    int
		ok     bool
	}{
		{[]int{}, 0, false},
		{[]int{3}, 3, true},
		{[]int{1, 5}, 5, true},
	}

	for _, tt := range tests {
		f := fakeProvider{
			initialState: make(map[string]any),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeValueState[int]("vs")
		for _, val := range tt.writes {
			vs.Write(&f, val)
		}
		val, ok, err := vs.Read(&f)
		if err != nil {
			t.Errorf("Value.Write() returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if ok && !tt.ok {
			t.Errorf("Value.Write() returned a value %v when it shouldn't have after writing: %v", val, tt.writes)
		} else if !ok && tt.ok {
			t.Errorf("Value.Write() didn't return a value when it should have returned %v after writing: %v", tt.val, tt.writes)
		} else if val != tt.val {
			t.Errorf("Value.Write()=%v, want %v after writing: %v", val, tt.val, tt.writes)
		}
	}
}

func TestValueClear(t *testing.T) {
	var tests = []struct {
		writes []int
		clears int
	}{
		{[]int{}, 1},
		{[]int{3}, 1},
		{[]int{1, 5}, 1},
		{[]int{}, 2},
		{[]int{3}, 2},
		{[]int{1, 5}, 2},
	}

	for _, tt := range tests {
		f := fakeProvider{
			initialState: make(map[string]any),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeValueState[int]("vs")
		for _, val := range tt.writes {
			vs.Write(&f, val)
		}
		for i := 0; i < tt.clears; i++ {
			err := vs.Clear(&f)
			if err != nil {
				t.Errorf("Value.Clear() attempt %v returned error %v", i, err)
			}
		}
		_, ok, err := vs.Read(&f)
		if err != nil {
			t.Errorf("Value.Read() returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if ok {
			t.Errorf("Value.Read() returned a value when it shouldn't have after writing %v and performing %v clears", tt.writes, tt.clears)
		}
	}
}

func TestBagRead(t *testing.T) {
	is := make(map[string][]any)
	ts := make(map[string][]Transaction)
	es := make(map[string]error)
	is["no_transactions"] = []any{1}
	ts["no_transactions"] = nil
	is["basic_append"] = []any{}
	ts["basic_append"] = []Transaction{{Key: "basic_append", Type: TransactionTypeAppend, Val: 3}}
	is["multi_append"] = []any{}
	ts["multi_append"] = []Transaction{{Key: "multi_append", Type: TransactionTypeAppend, Val: 3}, {Key: "multi_append", Type: TransactionTypeAppend, Val: 2}}
	is["basic_clear"] = []any{1}
	ts["basic_clear"] = []Transaction{{Key: "basic_clear", Type: TransactionTypeClear, Val: nil}}
	is["append_then_clear"] = []any{1}
	ts["append_then_clear"] = []Transaction{{Key: "append_then_clear", Type: TransactionTypeAppend, Val: 3}, {Key: "append_then_clear", Type: TransactionTypeClear, Val: nil}}
	is["append_then_clear_then_append"] = []any{1}
	ts["append_then_clear_then_append"] = []Transaction{{Key: "append_then_clear_then_append", Type: TransactionTypeAppend, Val: 3}, {Key: "append_then_clear_then_append", Type: TransactionTypeClear, Val: nil}, {Key: "append_then_clear_then_append", Type: TransactionTypeAppend, Val: 4}}
	is["err"] = []any{1}
	ts["err"] = []Transaction{{Key: "err", Type: TransactionTypeAppend, Val: 3}}
	es["err"] = errFake

	f := fakeProvider{
		initialBagState: is,
		transactions:    ts,
		err:             es,
	}

	var tests = []struct {
		vs  Bag[int]
		val []int
		ok  bool
		err error
	}{
		{MakeBagState[int]("no_transactions"), []int{1}, true, nil},
		{MakeBagState[int]("basic_append"), []int{3}, true, nil},
		{MakeBagState[int]("multi_append"), []int{3, 2}, true, nil},
		{MakeBagState[int]("basic_clear"), []int{}, false, nil},
		{MakeBagState[int]("append_then_clear"), []int{}, false, nil},
		{MakeBagState[int]("append_then_clear_then_append"), []int{4}, true, nil},
		{MakeBagState[int]("err"), []int{}, false, errFake},
	}

	for _, tt := range tests {
		val, ok, err := tt.vs.Read(&f)
		if err != nil && tt.err == nil {
			t.Errorf("Bag.Read() returned error %v for state key %v when it shouldn't have", err, tt.vs.Key)
		} else if err == nil && tt.err != nil {
			t.Errorf("Bag.Read() returned no error for state key %v when it should have returned %v", tt.vs.Key, err)
		} else if ok && !tt.ok {
			t.Errorf("Bag.Read() returned a value %v for state key %v when it shouldn't have", val, tt.vs.Key)
		} else if !ok && tt.ok {
			t.Errorf("Bag.Read() didn't return a value for state key %v when it should have returned %v", tt.vs.Key, tt.val)
		} else if len(val) != len(tt.val) {
			t.Errorf("Bag.Read()=%v, want %v for state key %v", val, tt.val, tt.vs.Key)
		} else {
			eq := true
			for idx, v := range val {
				if v != tt.val[idx] {
					eq = false
				}
			}
			if !eq {
				t.Errorf("Bag.Read()=%v, want %v for state key %v", val, tt.val, tt.vs.Key)
			}
		}
	}
}

func TestBagAdd(t *testing.T) {
	var tests = []struct {
		writes []int
		val    []int
		ok     bool
	}{
		{[]int{}, []int{}, false},
		{[]int{3}, []int{3}, true},
		{[]int{1, 5}, []int{1, 5}, true},
	}

	for _, tt := range tests {
		f := fakeProvider{
			initialState: make(map[string]any),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeBagState[int]("vs")
		for _, val := range tt.writes {
			vs.Add(&f, val)
		}
		val, ok, err := vs.Read(&f)
		if err != nil {
			t.Errorf("Bag.Read() returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if ok && !tt.ok {
			t.Errorf("Bag.Read() returned a value %v when it shouldn't have after writing: %v", val, tt.writes)
		} else if !ok && tt.ok {
			t.Errorf("Bag.Red() didn't return a value when it should have returned %v after writing: %v", tt.val, tt.writes)
		} else if len(val) != len(tt.val) {
			t.Errorf("Bag.Read()=%v, want %v after writing: %v", val, tt.val, tt.writes)
		} else {
			eq := true
			for idx, v := range val {
				if v != tt.val[idx] {
					eq = false
				}
			}
			if !eq {
				t.Errorf("Bag.Read()=%v, want %v after writing: %v", val, tt.val, tt.writes)
			}
		}
	}
}

func TestBagClear(t *testing.T) {
	var tests = []struct {
		writes []int
		clears int
	}{
		{[]int{}, 1},
		{[]int{3}, 1},
		{[]int{1, 5}, 1},
		{[]int{}, 2},
		{[]int{3}, 2},
		{[]int{1, 5}, 2},
	}

	for _, tt := range tests {
		f := fakeProvider{
			initialState: make(map[string]any),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeBagState[int]("vs")
		for _, val := range tt.writes {
			vs.Add(&f, val)
		}
		for i := 0; i < tt.clears; i++ {
			err := vs.Clear(&f)
			if err != nil {
				t.Errorf("Bag.Clear() attempt %v returned error %v", i, err)
			}
		}
		_, ok, err := vs.Read(&f)
		if err != nil {
			t.Errorf("Bag.Read() returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if ok {
			t.Errorf("Bag.Read() returned a value when it shouldn't have after writing %v and performing %v clears", tt.writes, tt.clears)
		}
	}
}

func TestCombiningRead(t *testing.T) {
	is := make(map[string]any)
	ts := make(map[string][]Transaction)
	es := make(map[string]error)
	ca := make(map[string]bool)
	eo := make(map[string]bool)
	ts["no_transactions"] = nil
	ts["no_transactions_initial_accum"] = nil
	ca["no_transactions_initial_accum"] = true
	ts["no_transactions_initial_accum_extract_out"] = nil
	ca["no_transactions_initial_accum_extract_out"] = true
	eo["no_transactions_initial_accum_extract_out"] = true
	is["basic_set"] = 1
	ts["basic_set"] = []Transaction{{Key: "basic_set", Type: TransactionTypeSet, Val: 3}}
	is["basic_clear"] = 1
	ts["basic_clear"] = []Transaction{{Key: "basic_clear", Type: TransactionTypeClear, Val: nil}}
	is["set_then_clear"] = 1
	ts["set_then_clear"] = []Transaction{{Key: "set_then_clear", Type: TransactionTypeSet, Val: 3}, {Key: "set_then_clear", Type: TransactionTypeClear, Val: nil}}
	is["set_then_clear_then_set"] = 1
	ts["set_then_clear_then_set"] = []Transaction{{Key: "set_then_clear_then_set", Type: TransactionTypeSet, Val: 3}, {Key: "set_then_clear_then_set", Type: TransactionTypeClear, Val: nil}, {Key: "set_then_clear_then_set", Type: TransactionTypeSet, Val: 4}}
	is["err"] = 1
	ts["err"] = []Transaction{{Key: "err", Type: TransactionTypeSet, Val: 3}}
	es["err"] = errFake

	f := fakeProvider{
		initialState:      is,
		transactions:      ts,
		err:               es,
		createAccumForKey: ca,
		extractOutForKey:  eo,
	}

	var tests = []struct {
		vs  Combining[int, int, int]
		val int
		ok  bool
		err error
	}{
		{MakeCombiningState[int, int, int]("no_transactions", func(a, b int) int {
			return a + b
		}), 0, false, nil},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum", func(a, b int) int {
			return a + b
		}), 1, true, nil},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum_extract_out", func(a, b int) int {
			return a + b
		}), 100, true, nil},
		{MakeCombiningState[int, int, int]("basic_set", func(a, b int) int {
			return a + b
		}), 3, true, nil},
		{MakeCombiningState[int, int, int]("basic_clear", func(a, b int) int {
			return a + b
		}), 0, false, nil},
		{MakeCombiningState[int, int, int]("set_then_clear", func(a, b int) int {
			return a + b
		}), 0, false, nil},
		{MakeCombiningState[int, int, int]("set_then_clear_then_set", func(a, b int) int {
			return a + b
		}), 4, true, nil},
		{MakeCombiningState[int, int, int]("err", func(a, b int) int {
			return a + b
		}), 0, false, errFake},
	}

	for _, tt := range tests {
		val, ok, err := tt.vs.Read(&f)
		if err != nil && tt.err == nil {
			t.Errorf("Combining.Read() returned error %v for state key %v when it shouldn't have", err, tt.vs.Key)
		} else if err == nil && tt.err != nil {
			t.Errorf("Combining.Read() returned no error for state key %v when it should have returned %v", tt.vs.Key, err)
		} else if ok && !tt.ok {
			t.Errorf("Combining.Read() returned a value %v for state key %v when it shouldn't have", val, tt.vs.Key)
		} else if !ok && tt.ok {
			t.Errorf("Combining.Read() didn't return a value for state key %v when it should have returned %v", tt.vs.Key, tt.val)
		} else if val != tt.val {
			t.Errorf("Combining.Read()=%v, want %v for state key %v", val, tt.val, tt.vs.Key)
		}
	}
}

func TestCombiningClear(t *testing.T) {
	var tests = []struct {
		vs     Combining[int, int, int]
		writes []int
		val    int
		clears int
		ok     bool
	}{
		{MakeCombiningState[int, int, int]("no_transactions", func(a, b int) int {
			return a + b
		}), []int{}, 0, 1, false},
		{MakeCombiningState[int, int, int]("no_transactions", func(a, b int) int {
			return a + b
		}), []int{2}, 0, 1, false},
		{MakeCombiningState[int, int, int]("no_transactions", func(a, b int) int {
			return a + b
		}), []int{7, 8, 9}, 0, 1, false},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum", func(a, b int) int {
			return a + b
		}), []int{}, 1, 1, true},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum", func(a, b int) int {
			return a + b
		}), []int{1}, 1, 1, true},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum", func(a, b int) int {
			return a + b
		}), []int{3, 4}, 1, 1, true},
	}

	for _, tt := range tests {
		is := make(map[string]any)
		ts := make(map[string][]Transaction)
		es := make(map[string]error)
		ca := make(map[string]bool)
		eo := make(map[string]bool)
		ma := make(map[string]bool)
		ai := make(map[string]bool)
		ts["no_transactions"] = nil
		ma["no_transactions"] = true
		ts["no_transactions_initial_accum"] = nil
		ca["no_transactions_initial_accum"] = true
		ma["no_transactions_initial_accum"] = true

		f := fakeProvider{
			initialState:      is,
			transactions:      ts,
			err:               es,
			createAccumForKey: ca,
			extractOutForKey:  eo,
			mergeAccumForKey:  ma,
			addInputForKey:    ai,
		}

		for _, val := range tt.writes {
			tt.vs.Add(&f, val)
		}
		for i := 0; i < tt.clears; i++ {
			err := tt.vs.Clear(&f)
			if err != nil {
				t.Errorf("Combining.Clear() attempt %v returned error %v", i, err)
			}
		}
		val, ok, err := tt.vs.Read(&f)
		if err != nil {
			t.Errorf("Combining.Read() returned error %v when it shouldn't have after writing %v and performing %v clears for key %v", err, tt.writes, tt.clears, tt.vs.StateKey())
		} else if ok && !tt.ok {
			t.Errorf("Combining.Read() returned a value when it shouldn't have after writing %v and performing %v clears for key %v", tt.writes, tt.clears, tt.vs.StateKey())
		} else if !ok && tt.ok {
			t.Errorf("Combining.Read() returned no value when it should have returned %v after writing %v and performing %v clears for key %v", tt.val, tt.writes, tt.clears, tt.vs.StateKey())
		} else if tt.val != val {
			t.Errorf("Combining.Read()=%v, want %v after writing %v and performing %v clears for key %v", val, tt.val, tt.writes, tt.clears, tt.vs.StateKey())
		}
	}
}

func TestCombiningAdd(t *testing.T) {
	var tests = []struct {
		vs     Combining[int, int, int]
		writes []int
		val    int
		ok     bool
	}{
		{MakeCombiningState[int, int, int]("no_transactions", func(a, b int) int {
			return a + b
		}), []int{}, 0, false},
		{MakeCombiningState[int, int, int]("no_transactions", func(a, b int) int {
			return a + b
		}), []int{2}, 2, true},
		{MakeCombiningState[int, int, int]("no_transactions", func(a, b int) int {
			return a + b
		}), []int{7, 8, 9}, 24, true},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum", func(a, b int) int {
			return a + b
		}), []int{}, 1, true},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum", func(a, b int) int {
			return a + b
		}), []int{1}, 2, true},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum", func(a, b int) int {
			return a + b
		}), []int{3, 4}, 8, true},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum_extract_out", func(a, b int) int {
			return a + b
		}), []int{}, 100, true},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum_extract_out", func(a, b int) int {
			return a + b
		}), []int{1}, 200, true},
		{MakeCombiningState[int, int, int]("no_transactions_initial_accum_extract_out", func(a, b int) int {
			return a + b
		}), []int{1, 2}, 400, true},
	}

	for _, tt := range tests {
		is := make(map[string]any)
		ts := make(map[string][]Transaction)
		es := make(map[string]error)
		ca := make(map[string]bool)
		eo := make(map[string]bool)
		ma := make(map[string]bool)
		ai := make(map[string]bool)
		ts["no_transactions"] = nil
		ma["no_transactions"] = true
		ts["no_transactions_initial_accum"] = nil
		ca["no_transactions_initial_accum"] = true
		ma["no_transactions_initial_accum"] = true
		ts["no_transactions_initial_accum_extract_out"] = nil
		ca["no_transactions_initial_accum_extract_out"] = true
		eo["no_transactions_initial_accum_extract_out"] = true
		ai["no_transactions_initial_accum_extract_out"] = true

		f := fakeProvider{
			initialState:      is,
			transactions:      ts,
			err:               es,
			createAccumForKey: ca,
			extractOutForKey:  eo,
			mergeAccumForKey:  ma,
			addInputForKey:    ai,
		}

		for _, val := range tt.writes {
			tt.vs.Add(&f, val)
		}

		val, ok, err := tt.vs.Read(&f)
		if err != nil {
			t.Errorf("Bag.Read() returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if ok && !tt.ok {
			t.Errorf("Bag.Read() returned a value %v when it shouldn't have after writing: %v", val, tt.writes)
		} else if !ok && tt.ok {
			t.Errorf("Bag.Read() didn't return a value when it should have returned %v after writing: %v", tt.val, tt.writes)
		} else if val != tt.val {
			t.Errorf("Bag.Read()=%v, want %v after writing: %v", val, tt.val, tt.writes)
		}
	}
}

func TestMapGet(t *testing.T) {
	is := make(map[string]any)
	im := make(map[string]map[string]any)
	ts := make(map[string][]Transaction)
	es := make(map[string]error)
	ca := make(map[string]bool)
	eo := make(map[string]bool)
	ts["no_transactions"] = nil
	im["basic_set"] = map[string]any{"foo": 2}
	ts["basic_set"] = []Transaction{{Key: "basic_set", Type: TransactionTypeSet, Val: 3, MapKey: "foo"}, {Key: "basic_set", Type: TransactionTypeSet, Val: 1, MapKey: "bar"}}
	im["basic_clear"] = map[string]any{"foo": 2, "bar": 1}
	ts["basic_clear"] = []Transaction{{Key: "basic_clear", Type: TransactionTypeClear, Val: nil, MapKey: "foo"}}
	im["set_then_clear"] = map[string]any{"foo": 2, "bar": 1}
	ts["set_then_clear"] = []Transaction{{Key: "set_then_clear", Type: TransactionTypeSet, Val: 3, MapKey: "foo"}, {Key: "set_then_clear", Type: TransactionTypeClear, Val: nil, MapKey: "foo"}}
	im["err"] = map[string]any{"foo": 2}
	ts["err"] = []Transaction{{Key: "err", Type: TransactionTypeSet, Val: 3}}
	es["err"] = errFake

	f := fakeProvider{
		initialMapState:   im,
		initialState:      is,
		transactions:      ts,
		err:               es,
		createAccumForKey: ca,
		extractOutForKey:  eo,
	}

	var tests = []struct {
		vs    Map[string, int]
		foo   int
		bar   int
		fooOk bool
		barOk bool
		err   error
	}{
		{MakeMapState[string, int]("no_transactions"), 0, 0, false, false, nil},
		{MakeMapState[string, int]("basic_set"), 3, 1, true, true, nil},
		{MakeMapState[string, int]("basic_clear"), 0, 1, false, true, nil},
		{MakeMapState[string, int]("set_then_clear"), 0, 1, false, true, nil},
		{MakeMapState[string, int]("err"), 0, 0, false, false, errFake},
	}

	for _, tt := range tests {
		val, ok, err := tt.vs.Get(&f, "foo")
		if err != nil && tt.err == nil {
			t.Errorf("Map.Get() returned error %v for state key %v and map key foo when it shouldn't have", err, tt.vs.Key)
		} else if err == nil && tt.err != nil {
			t.Errorf("Map.Get() returned no error for state key %v and map key foo when it should have returned %v", tt.vs.Key, err)
		} else if ok && !tt.fooOk {
			t.Errorf("Map.Get() returned a value %v for state key %v and map key foo when it shouldn't have", val, tt.vs.Key)
		} else if !ok && tt.fooOk {
			t.Errorf("Map.Get() didn't return a value for state key %v and map key foo when it should have returned %v", tt.vs.Key, tt.foo)
		} else if val != tt.foo {
			t.Errorf("Map.Get()=%v, want %v for state key %v and map key foo", val, tt.foo, tt.vs.Key)
		}
		val, ok, err = tt.vs.Get(&f, "bar")
		if err != nil && tt.err == nil {
			t.Errorf("Map.Get() returned error %v for state key %v and map key bar when it shouldn't have", err, tt.vs.Key)
		} else if err == nil && tt.err != nil {
			t.Errorf("Map.Get() returned no error for state key %v and map key bar when it should have returned %v", tt.vs.Key, err)
		} else if ok && !tt.barOk {
			t.Errorf("Map.Get() returned a value %v for state key %v and map key bar when it shouldn't have", val, tt.vs.Key)
		} else if !ok && tt.barOk {
			t.Errorf("Map.Get() didn't return a value for state key %v and map key bar when it should have returned %v", tt.vs.Key, tt.bar)
		} else if val != tt.bar {
			t.Errorf("Map.Get()=%v, want %v for state key %v and map key bar", val, tt.bar, tt.vs.Key)
		}
	}
}

func TestMapKeys(t *testing.T) {
	is := make(map[string]any)
	im := make(map[string]map[string]any)
	ts := make(map[string][]Transaction)
	es := make(map[string]error)
	ca := make(map[string]bool)
	eo := make(map[string]bool)
	ts["no_transactions"] = nil
	im["basic_set"] = map[string]any{"foo": 2}
	ts["basic_set"] = []Transaction{{Key: "basic_set", Type: TransactionTypeSet, Val: 3, MapKey: "foo"}, {Key: "basic_set", Type: TransactionTypeSet, Val: 1, MapKey: "bar"}}
	im["basic_clear"] = map[string]any{"foo": 2, "bar": 1}
	ts["basic_clear"] = []Transaction{{Key: "basic_clear", Type: TransactionTypeClear, Val: nil, MapKey: "foo"}}
	im["set_then_clear"] = map[string]any{"foo": 2, "bar": 1}
	ts["set_then_clear"] = []Transaction{{Key: "set_then_clear", Type: TransactionTypeSet, Val: 3, MapKey: "foo"}, {Key: "set_then_clear", Type: TransactionTypeClear, Val: nil, MapKey: "foo"}}
	im["err"] = map[string]any{"foo": 2}
	ts["err"] = []Transaction{{Key: "err", Type: TransactionTypeSet, Val: 3}}
	es["err"] = errFake

	f := fakeProvider{
		initialMapState:   im,
		initialState:      is,
		transactions:      ts,
		err:               es,
		createAccumForKey: ca,
		extractOutForKey:  eo,
	}

	var tests = []struct {
		vs   Map[string, int]
		keys []string
		ok   bool
		err  error
	}{
		{MakeMapState[string, int]("no_transactions"), []string{}, false, nil},
		{MakeMapState[string, int]("basic_set"), []string{"foo", "bar"}, true, nil},
		{MakeMapState[string, int]("basic_clear"), []string{"bar"}, true, nil},
		{MakeMapState[string, int]("set_then_clear"), []string{"bar"}, true, nil},
		{MakeMapState[string, int]("err"), []string{}, false, errFake},
	}

	for _, tt := range tests {
		val, ok, err := tt.vs.Keys(&f)
		if err != nil && tt.err == nil {
			t.Errorf("Map.Keys() returned error %v for state key %v when it shouldn't have", err, tt.vs.Key)
		} else if err == nil && tt.err != nil {
			t.Errorf("Map.Keys() returned no error for state key %v when it should have returned %v", tt.vs.Key, err)
		} else if ok && !tt.ok {
			t.Errorf("Map.Keys() returned a value %v for state key %v when it shouldn't have", val, tt.vs.Key)
		} else if len(val) != len(tt.keys) {
			t.Errorf("Map.Keys()=%v, want %v for state key %v", val, tt.keys, tt.vs.Key)
		} else {
			eq := true
			for idx, v := range val {
				if v != tt.keys[idx] {
					eq = false
				}
			}
			if !eq {
				t.Errorf("Map.Keys()=%v, want %v for state key %v", val, tt.keys, tt.vs.Key)
			}
		}
	}
}

func TestMapPut(t *testing.T) {
	var tests = []struct {
		writes []int
		val    int
		ok     bool
	}{
		{[]int{}, 0, false},
		{[]int{3}, 3, true},
		{[]int{1, 5}, 5, true},
	}

	for _, tt := range tests {
		f := fakeProvider{
			initialState: make(map[string]any),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeMapState[string, int]("vs")
		for _, val := range tt.writes {
			vs.Put(&f, "foo", val)
		}
		val, ok, err := vs.Get(&f, "foo")
		if err != nil {
			t.Errorf("Map.Get(\"foo\") returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if ok && !tt.ok {
			t.Errorf("Map.Get(\"foo\") returned a value %v when it shouldn't have after writing: %v", val, tt.writes)
		} else if !ok && tt.ok {
			t.Errorf("Map.Get(\"foo\") didn't return a value when it should have returned %v after writing: %v", tt.val, tt.writes)
		} else if val != tt.val {
			t.Errorf("Map.Get(\"foo\")=%v, want %v after writing: %v", val, tt.val, tt.writes)
		}
	}
}

func TestMapRemove(t *testing.T) {
	var tests = []struct {
		writes  [][]string
		removes []string
		keys    []string
	}{
		{[][]string{}, []string{}, []string{}},
		{[][]string{{"foo", "bar"}, {"foo2", "bar2"}, {"foo3", "bar3"}}, []string{"foo", "foo2"}, []string{"foo3"}},
		{[][]string{{"foo", "bar"}, {"foo2", "bar2"}, {"foo3", "bar3"}}, []string{"foo", "foo2", "foo"}, []string{"foo3"}},
		{[][]string{{"foo", "bar"}, {"foo2", "bar2"}, {"foo3", "bar3"}}, []string{"foo", "foo2", "foo3"}, []string{}},
	}

	for _, tt := range tests {
		f := fakeProvider{
			initialState: make(map[string]any),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeMapState[string, string]("vs")
		for _, val := range tt.writes {
			vs.Put(&f, val[0], val[1])
		}
		for _, val := range tt.removes {
			err := vs.Remove(&f, val)
			if err != nil {
				t.Errorf("vs.Remove(%v) returned error %v", val, err)
			}
		}
		val, _, err := vs.Keys(&f)
		if err != nil {
			t.Errorf("Map.Keys() returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if len(val) != len(tt.keys) {
			t.Errorf("Map.Keys()=%v, want %v for state key %v", val, tt.keys, vs.Key)
		} else {
			eq := true
			for idx, v := range val {
				if v != tt.keys[idx] {
					eq = false
				}
			}
			if !eq {
				t.Errorf("Map.Keys()=%v, want %v for state key %v", val, tt.keys, vs.Key)
			}
		}
	}
}

func TestMapClear(t *testing.T) {
	var tests = []struct {
		writes [][]string
		keys   []string
	}{
		{[][]string{}, []string{}},
		{[][]string{{"foo", "bar"}, {"foo2", "bar2"}, {"foo3", "bar3"}}, []string{}},
	}

	for _, tt := range tests {
		f := fakeProvider{
			initialState: make(map[string]any),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeMapState[string, string]("vs")
		for _, val := range tt.writes {
			vs.Put(&f, val[0], val[1])
		}
		err := vs.Clear(&f)
		if err != nil {
			t.Errorf("vs.Clear() returned error %v", err)
		}
		val, _, err := vs.Keys(&f)
		if err != nil {
			t.Errorf("Map.Keys() returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if len(val) != len(tt.keys) {
			t.Errorf("Map.Keys()=%v, want %v for state key %v", val, tt.keys, vs.Key)
		} else {
			eq := true
			for idx, v := range val {
				if v != tt.keys[idx] {
					eq = false
				}
			}
			if !eq {
				t.Errorf("Map.Keys()=%v, want %v for state key %v", val, tt.keys, vs.Key)
			}
		}
	}
}

func TestSetContains(t *testing.T) {
	is := make(map[string]any)
	im := make(map[string]map[string]any)
	ts := make(map[string][]Transaction)
	es := make(map[string]error)
	ca := make(map[string]bool)
	eo := make(map[string]bool)
	ts["no_transactions"] = nil
	im["basic_set"] = map[string]any{}
	ts["basic_set"] = []Transaction{{Key: "basic_set", Type: TransactionTypeSet, Val: true, MapKey: "foo"}, {Key: "basic_set", Type: TransactionTypeSet, Val: true, MapKey: "bar"}}
	im["basic_clear"] = map[string]any{"foo": true, "bar": true}
	ts["basic_clear"] = []Transaction{{Key: "basic_clear", Type: TransactionTypeClear, Val: nil, MapKey: "foo"}}
	im["set_then_clear"] = map[string]any{"foo": true, "bar": true}
	ts["set_then_clear"] = []Transaction{{Key: "set_then_clear", Type: TransactionTypeSet, Val: true, MapKey: "foo"}, {Key: "set_then_clear", Type: TransactionTypeClear, Val: nil, MapKey: "foo"}}
	im["err"] = map[string]any{"foo": true}
	ts["err"] = []Transaction{{Key: "err", Type: TransactionTypeSet, Val: true}}
	es["err"] = errFake

	f := fakeProvider{
		initialMapState:   im,
		initialState:      is,
		transactions:      ts,
		err:               es,
		createAccumForKey: ca,
		extractOutForKey:  eo,
	}

	var tests = []struct {
		vs    Set[string]
		fooOk bool
		barOk bool
		err   error
	}{
		{MakeSetState[string]("no_transactions"), false, false, nil},
		{MakeSetState[string]("basic_set"), true, true, nil},
		{MakeSetState[string]("basic_clear"), false, true, nil},
		{MakeSetState[string]("set_then_clear"), false, true, nil},
		{MakeSetState[string]("err"), false, false, errFake},
	}

	for _, tt := range tests {
		ok, err := tt.vs.Contains(&f, "foo")
		if err != nil && tt.err == nil {
			t.Errorf("Set.Contains() returned error %v for state key %v entry foo when it shouldn't have", err, tt.vs.Key)
		} else if err == nil && tt.err != nil {
			t.Errorf("Set.Contains() returned no error for state key %v entry foo when it should have returned %v", tt.vs.Key, err)
		} else if ok && !tt.fooOk {
			t.Errorf("Set.Contains() returned true for state key %v entry foo when it shouldn't have", tt.vs.Key)
		} else if !ok && tt.fooOk {
			t.Errorf("Set.Contains() returned false for state key %v entry foo when it should have returned true", tt.vs.Key)
		}
		ok, err = tt.vs.Contains(&f, "bar")
		if err != nil && tt.err == nil {
			t.Errorf("Set.Contains() returned error %v for state key %v entry bar when it shouldn't have", err, tt.vs.Key)
		} else if err == nil && tt.err != nil {
			t.Errorf("Set.Contains() returned no error for state key %v entry bar when it should have returned %v", tt.vs.Key, err)
		} else if ok && !tt.barOk {
			t.Errorf("Set.Contains() returned true for state key %v entry bar when it shouldn't have", tt.vs.Key)
		} else if !ok && tt.barOk {
			t.Errorf("Set.Contains() returned false for state key %v entry bar when it should have returned true", tt.vs.Key)
		}
	}
}

func TestSetKeys(t *testing.T) {
	is := make(map[string]any)
	im := make(map[string]map[string]any)
	ts := make(map[string][]Transaction)
	es := make(map[string]error)
	ca := make(map[string]bool)
	eo := make(map[string]bool)
	ts["no_transactions"] = nil
	im["basic_set"] = map[string]any{"foo": true}
	ts["basic_set"] = []Transaction{{Key: "basic_set", Type: TransactionTypeSet, Val: true, MapKey: "foo"}, {Key: "basic_set", Type: TransactionTypeSet, Val: true, MapKey: "bar"}}
	im["basic_clear"] = map[string]any{"foo": true, "bar": true}
	ts["basic_clear"] = []Transaction{{Key: "basic_clear", Type: TransactionTypeClear, Val: nil, MapKey: "foo"}}
	im["set_then_clear"] = map[string]any{"foo": true, "bar": true}
	ts["set_then_clear"] = []Transaction{{Key: "set_then_clear", Type: TransactionTypeSet, Val: true, MapKey: "foo"}, {Key: "set_then_clear", Type: TransactionTypeClear, Val: nil, MapKey: "foo"}}
	im["err"] = map[string]any{"foo": true}
	ts["err"] = []Transaction{{Key: "err", Type: TransactionTypeSet, Val: true}}
	es["err"] = errFake

	f := fakeProvider{
		initialMapState:   im,
		initialState:      is,
		transactions:      ts,
		err:               es,
		createAccumForKey: ca,
		extractOutForKey:  eo,
	}

	var tests = []struct {
		vs   Set[string]
		keys []string
		ok   bool
		err  error
	}{
		{MakeSetState[string]("no_transactions"), []string{}, false, nil},
		{MakeSetState[string]("basic_set"), []string{"foo", "bar"}, true, nil},
		{MakeSetState[string]("basic_clear"), []string{"bar"}, true, nil},
		{MakeSetState[string]("set_then_clear"), []string{"bar"}, true, nil},
		{MakeSetState[string]("err"), []string{}, false, errFake},
	}

	for _, tt := range tests {
		val, ok, err := tt.vs.Keys(&f)
		if err != nil && tt.err == nil {
			t.Errorf("Set.Keys() returned error %v for state key %v when it shouldn't have", err, tt.vs.Key)
		} else if err == nil && tt.err != nil {
			t.Errorf("Set.Keys() returned no error for state key %v when it should have returned %v", tt.vs.Key, err)
		} else if ok && !tt.ok {
			t.Errorf("Set.Keys() returned a value %v for state key %v when it shouldn't have", val, tt.vs.Key)
		} else if len(val) != len(tt.keys) {
			t.Errorf("Set.Keys()=%v, want %v for state key %v", val, tt.keys, tt.vs.Key)
		} else {
			eq := true
			for idx, v := range val {
				if v != tt.keys[idx] {
					eq = false
				}
			}
			if !eq {
				t.Errorf("Set.Keys()=%v, want %v for state key %v", val, tt.keys, tt.vs.Key)
			}
		}
	}
}

func TestSetAdd(t *testing.T) {
	var tests = []struct {
		writes []string
		okFoo  bool
		okBar  bool
	}{
		{[]string{}, false, false},
		{[]string{"foo"}, true, false},
		{[]string{"foo", "bar"}, true, true},
	}

	for _, tt := range tests {
		f := fakeProvider{
			initialState: make(map[string]any),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeSetState[string]("vs")
		for _, val := range tt.writes {
			vs.Add(&f, val)
		}
		ok, err := vs.Contains(&f, "foo")
		if err != nil {
			t.Errorf("Set.Contains(\"foo\") returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if ok && !tt.okFoo {
			t.Errorf("Set.Contains(\"foo\") returned true when it should have returned false after writing: %v", tt.writes)
		} else if !ok && tt.okFoo {
			t.Errorf("Set.Contains(\"foo\") returned false when it should have returned true after writing: %v", tt.writes)
		}
		ok, err = vs.Contains(&f, "bar")
		if err != nil {
			t.Errorf("Set.Contains(\"bar\") returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if ok && !tt.okBar {
			t.Errorf("Set.Contains(\"bar\") returned true when it should have returned false after writing: %v", tt.writes)
		} else if !ok && tt.okBar {
			t.Errorf("Set.Contains(\"bar\") returned false when it should have returned true after writing: %v", tt.writes)
		}
	}
}

func TestSetRemove(t *testing.T) {
	var tests = []struct {
		writes  []string
		removes []string
		keys    []string
	}{
		{[]string{}, []string{}, []string{}},
		{[]string{"foo", "foo2", "foo3"}, []string{"foo", "foo2"}, []string{"foo3"}},
		{[]string{"foo", "foo2", "foo3"}, []string{"foo", "foo2", "foo"}, []string{"foo3"}},
		{[]string{"foo", "foo2", "foo3"}, []string{"foo", "foo2", "foo3"}, []string{}},
	}

	for _, tt := range tests {
		f := fakeProvider{
			initialState: make(map[string]any),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeSetState[string]("vs")
		for _, val := range tt.writes {
			vs.Add(&f, val)
		}
		for _, val := range tt.removes {
			err := vs.Remove(&f, val)
			if err != nil {
				t.Errorf("vs.Remove(%v) returned error %v", val, err)
			}
		}
		val, _, err := vs.Keys(&f)
		if err != nil {
			t.Errorf("Set.Keys() returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if len(val) != len(tt.keys) {
			t.Errorf("Set.Keys()=%v, want %v for state key %v", val, tt.keys, vs.Key)
		} else {
			eq := true
			for idx, v := range val {
				if v != tt.keys[idx] {
					eq = false
				}
			}
			if !eq {
				t.Errorf("Set.Keys()=%v, want %v for state key %v", val, tt.keys, vs.Key)
			}
		}
	}
}

func TestSetClear(t *testing.T) {
	var tests = []struct {
		writes []string
		keys   []string
	}{
		{[]string{}, []string{}},
		{[]string{"foo", "foo2", "foo3"}, []string{}},
	}

	for _, tt := range tests {
		f := fakeProvider{
			initialState: make(map[string]any),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeSetState[string]("vs")
		for _, val := range tt.writes {
			vs.Add(&f, val)
		}
		err := vs.Clear(&f)
		if err != nil {
			t.Errorf("vs.Clear() returned error %v", err)
		}
		val, _, err := vs.Keys(&f)
		if err != nil {
			t.Errorf("Set.Keys() returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if len(val) != len(tt.keys) {
			t.Errorf("Set.Keys()=%v, want %v for state key %v", val, tt.keys, vs.Key)
		} else {
			eq := true
			for idx, v := range val {
				if v != tt.keys[idx] {
					eq = false
				}
			}
			if !eq {
				t.Errorf("Set.Keys()=%v, want %v for state key %v", val, tt.keys, vs.Key)
			}
		}
	}
}
