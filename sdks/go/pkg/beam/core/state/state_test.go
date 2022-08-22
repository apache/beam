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
)

var (
	errFake = errors.New("fake error")
)

type fakeProvider struct {
	initialState    map[string]interface{}
	initialBagState map[string][]interface{}
	transactions    map[string][]Transaction
	err             map[string]error
}

func (s *fakeProvider) ReadValueState(userStateID string) (interface{}, []Transaction, error) {
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
	if transactions, ok := s.transactions[val.Key]; ok {
		s.transactions[val.Key] = append(transactions, val)
	} else {
		s.transactions[val.Key] = []Transaction{val}
	}
	return nil
}

func (s *fakeProvider) ReadBagState(userStateID string) ([]interface{}, []Transaction, error) {
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

func TestValueRead(t *testing.T) {
	is := make(map[string]interface{})
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
			initialState: make(map[string]interface{}),
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

func TestBagRead(t *testing.T) {
	is := make(map[string][]interface{})
	ts := make(map[string][]Transaction)
	es := make(map[string]error)
	is["no_transactions"] = []interface{}{1}
	ts["no_transactions"] = nil
	is["basic_append"] = []interface{}{}
	ts["basic_append"] = []Transaction{{Key: "basic_append", Type: TransactionTypeAppend, Val: 3}}
	is["multi_append"] = []interface{}{}
	ts["multi_append"] = []Transaction{{Key: "multi_append", Type: TransactionTypeAppend, Val: 3}, {Key: "multi_append", Type: TransactionTypeAppend, Val: 2}}
	is["basic_clear"] = []interface{}{1}
	ts["basic_clear"] = []Transaction{{Key: "basic_clear", Type: TransactionTypeClear, Val: nil}}
	is["append_then_clear"] = []interface{}{1}
	ts["append_then_clear"] = []Transaction{{Key: "append_then_clear", Type: TransactionTypeAppend, Val: 3}, {Key: "append_then_clear", Type: TransactionTypeClear, Val: nil}}
	is["append_then_clear_then_append"] = []interface{}{1}
	ts["append_then_clear_then_append"] = []Transaction{{Key: "append_then_clear_then_append", Type: TransactionTypeAppend, Val: 3}, {Key: "append_then_clear_then_append", Type: TransactionTypeClear, Val: nil}, {Key: "append_then_clear_then_append", Type: TransactionTypeAppend, Val: 4}}
	is["err"] = []interface{}{1}
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

func TestBagWrite(t *testing.T) {
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
			initialState: make(map[string]interface{}),
			transactions: make(map[string][]Transaction),
			err:          make(map[string]error),
		}
		vs := MakeBagState[int]("vs")
		for _, val := range tt.writes {
			vs.Add(&f, val)
		}
		val, ok, err := vs.Read(&f)
		if err != nil {
			t.Errorf("Bag.Write() returned error %v when it shouldn't have after writing: %v", err, tt.writes)
		} else if ok && !tt.ok {
			t.Errorf("Bag.Write() returned a value %v when it shouldn't have after writing: %v", val, tt.writes)
		} else if !ok && tt.ok {
			t.Errorf("Bag.Write() didn't return a value when it should have returned %v after writing: %v", tt.val, tt.writes)
		} else if len(val) != len(tt.val) {
			t.Errorf("Bag.Write()=%v, want %v after writing: %v", val, tt.val, tt.writes)
		} else {
			eq := true
			for idx, v := range val {
				if v != tt.val[idx] {
					eq = false
				}
			}
			if !eq {
				t.Errorf("Bag.Write()=%v, want %v after writing: %v", val, tt.val, tt.writes)
			}
		}
	}
}
