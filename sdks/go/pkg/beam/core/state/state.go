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

// Package state contains structs for reading and manipulating pipeline state.
package state

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

// TransactionTypeEnum represents the type of state transaction (e.g. set, clear)
type TransactionTypeEnum int32

// TypeEnum represents the type of a state instance (e.g. value, bag, etc...)
type TypeEnum int32

const (
	// TransactionTypeSet is the set transaction type
	TransactionTypeSet TransactionTypeEnum = 0
	// TransactionTypeClear is the set transaction type
	TransactionTypeClear TransactionTypeEnum = 1
	// TransactionTypeAppend is the append transaction type
	TransactionTypeAppend TransactionTypeEnum = 2
	// TypeValue represents a value state
	TypeValue TypeEnum = 0
	// TypeBag represents a bag state
	TypeBag TypeEnum = 1
	// TypeCombining represents a combining state
	TypeCombining TypeEnum = 2
	// TypeMap represents a map state
	TypeMap TypeEnum = 3
	// TypeSet represents a set state
	TypeSet TypeEnum = 4
)

var (
	// ProviderType is the state provider type
	ProviderType = reflect.TypeOf((*Provider)(nil)).Elem()
)

// TODO(#20510) - add other forms of state (MapState, BagState, CombiningState), prefetch, and clear.

// Transaction is used to represent a pending state transaction. This should not be manipulated directly;
// it is primarily used for implementations of the Provider interface to talk to the various State objects.
type Transaction struct {
	Key    string
	Type   TransactionTypeEnum
	MapKey any
	Val    any
}

// Provider represents the DoFn parameter used to get and manipulate pipeline state
// stored as key value pairs (https://beam.apache.org/documentation/programming-guide/#state-and-timers).
// This should not be manipulated directly. Instead it should be used as a parameter
// to functions on State objects like state.Value.
type Provider interface {
	ReadValueState(id string) (any, []Transaction, error)
	WriteValueState(val Transaction) error
	ClearValueState(val Transaction) error
	ReadBagState(id string) ([]any, []Transaction, error)
	WriteBagState(val Transaction) error
	ClearBagState(val Transaction) error
	CreateAccumulatorFn(userStateID string) reflectx.Func
	AddInputFn(userStateID string) reflectx.Func
	MergeAccumulatorsFn(userStateID string) reflectx.Func
	ExtractOutputFn(userStateID string) reflectx.Func
	ReadMapStateValue(userStateID string, key any) (any, []Transaction, error)
	ReadMapStateKeys(userStateID string) ([]any, []Transaction, error)
	WriteMapState(val Transaction) error
	ClearMapStateKey(val Transaction) error
	ClearMapState(val Transaction) error
}

// PipelineState is an interface representing different kinds of PipelineState (currently just state.Value).
// It is primarily meant for Beam packages to use and is probably not useful for most pipeline authors.
type PipelineState interface {
	StateKey() string
	KeyCoderType() reflect.Type
	CoderType() reflect.Type
	StateType() TypeEnum
}

// CombiningPipelineState is an interface representing combining pipeline state.
// It is primarily meant for Beam packages to use and is probably not useful for most pipeline authors.
type CombiningPipelineState interface {
	GetCombineFn() any
}

// Value is used to read and write global pipeline state representing a single value.
// Key represents the key used to lookup this state.
type Value[T any] struct {
	Key string
}

// Write is used to write this instance of global pipeline state representing a single value.
func (s *Value[T]) Write(p Provider, val T) error {
	return p.WriteValueState(Transaction{
		Key:  s.Key,
		Type: TransactionTypeSet,
		Val:  val,
	})
}

// Read is used to read this instance of global pipeline state representing a single value.
// When a value is not found, returns the 0 value and false.
func (s *Value[T]) Read(p Provider) (T, bool, error) {
	// This replays any writes that have happened to this value since we last read
	// For more detail, see "State Transactionality" below for buffered transactions
	cur, bufferedTransactions, err := p.ReadValueState(s.Key)
	if err != nil {
		var val T
		return val, false, err
	}
	for _, t := range bufferedTransactions {
		switch t.Type {
		case TransactionTypeSet:
			cur = t.Val
		case TransactionTypeClear:
			cur = nil
		}
	}
	if cur == nil {
		var val T
		return val, false, nil
	}
	return cur.(T), true, nil
}

// Clear is used to clear this instance of global pipeline state representing a single value.
func (s *Value[T]) Clear(p Provider) error {
	return p.ClearValueState(Transaction{
		Key:  s.Key,
		Type: TransactionTypeClear,
	})
}

// StateKey returns the key for this pipeline state entry.
func (s Value[T]) StateKey() string {
	return s.Key
}

// KeyCoderType returns nil since Value types aren't keyed.
func (s Value[T]) KeyCoderType() reflect.Type {
	return nil
}

// CoderType returns the type of the value state which should be used for a coder.
func (s Value[T]) CoderType() reflect.Type {
	var t T
	return reflect.TypeOf(t)
}

// StateType returns the type of the state (in this case always Value).
func (s Value[T]) StateType() TypeEnum {
	return TypeValue
}

// MakeValueState is a factory function to create an instance of ValueState with the given key.
func MakeValueState[T any](k string) Value[T] {
	return Value[T]{
		Key: k,
	}
}

// Bag is used to read and write global pipeline state representing a collection of values.
// Key represents the key used to lookup this state.
type Bag[T any] struct {
	Key string
}

// Add is used to write append to the bag pipeline state.
func (s *Bag[T]) Add(p Provider, val T) error {
	return p.WriteBagState(Transaction{
		Key:  s.Key,
		Type: TransactionTypeAppend,
		Val:  val,
	})
}

// Read is used to read this instance of global pipeline state representing a bag.
// When a value is not found, returns an empty list and false.
func (s *Bag[T]) Read(p Provider) ([]T, bool, error) {
	// This replays any writes that have happened to this value since we last read
	// For more detail, see "State Transactionality" below for buffered transactions
	initialValue, bufferedTransactions, err := p.ReadBagState(s.Key)
	if err != nil {
		var val []T
		return val, false, err
	}
	cur := []T{}
	for _, v := range initialValue {
		cur = append(cur, v.(T))
	}
	for _, t := range bufferedTransactions {
		switch t.Type {
		case TransactionTypeAppend:
			cur = append(cur, t.Val.(T))
		case TransactionTypeClear:
			cur = []T{}
		}
	}
	if len(cur) == 0 {
		return cur, false, nil
	}
	return cur, true, nil
}

// Clear is used to clear this instance of global pipeline state representing a bag.
func (s *Bag[T]) Clear(p Provider) error {
	return p.ClearBagState(Transaction{
		Key:  s.Key,
		Type: TransactionTypeClear,
	})
}

// StateKey returns the key for this pipeline state entry.
func (s Bag[T]) StateKey() string {
	return s.Key
}

// KeyCoderType returns nil since Bag types aren't keyed.
func (s Bag[T]) KeyCoderType() reflect.Type {
	return nil
}

// CoderType returns the type of the bag state which should be used for a coder.
func (s Bag[T]) CoderType() reflect.Type {
	var t T
	return reflect.TypeOf(t)
}

// StateType returns the type of the state (in this case always Bag).
func (s Bag[T]) StateType() TypeEnum {
	return TypeBag
}

// MakeBagState is a factory function to create an instance of BagState with the given key.
func MakeBagState[T any](k string) Bag[T] {
	return Bag[T]{
		Key: k,
	}
}

// Combining is used to read and write global pipeline state representing a single combined value.
// It uses 3 generic values, [T1, T2, T3], to represent the accumulator, input, and output types respectively.
// Key represents the key used to lookup this state.
type Combining[T1, T2, T3 any] struct {
	Key       string
	combineFn any
}

// Add is used to write add an element to the combining pipeline state.
func (s *Combining[T1, T2, T3]) Add(p Provider, val T2) error {
	// We will always maintain a single accumulated value as a value state.
	// Therefore, when we add we must first read the current accumulator so that we can add to it.
	acc, ok, err := s.readAccumulator(p)
	if err != nil {
		return err
	}
	if !ok {
		// If no accumulator, that means that the CreateAccumulator function doesn't exist
		// and our value is our initial accumulator.
		return p.WriteValueState(Transaction{
			Key:  s.Key,
			Type: TransactionTypeSet,
			Val:  val,
		})
	}

	if ai := p.AddInputFn(s.Key); ai != nil {
		var newVal any
		if f, ok := ai.(reflectx.Func2x1); ok {
			newVal = f.Call2x1(acc, val)
		} else {
			newVal = f.Call([]any{acc, val})[0]
		}
		return p.WriteValueState(Transaction{
			Key:  s.Key,
			Type: TransactionTypeSet,
			Val:  newVal,
		})
	}
	// If AddInput isn't defined, that means we must just have one accumulator type identical to the input type.
	if ma := p.MergeAccumulatorsFn(s.Key); ma != nil {
		var newVal any
		if f, ok := ma.(reflectx.Func2x1); ok {
			newVal = f.Call2x1(acc, val)
		} else {
			newVal = f.Call([]any{acc, val})[0]
		}
		return p.WriteValueState(Transaction{
			Key:  s.Key,
			Type: TransactionTypeSet,
			Val:  newVal,
		})
	}

	// Should be taken care of by previous validation
	panic(fmt.Sprintf("MergeAccumulators must be defined on accumulator %v", s))
}

// Read is used to read this instance of global pipeline state representing a combiner.
// When a value is not found, returns an empty list and false.
func (s *Combining[T1, T2, T3]) Read(p Provider) (T3, bool, error) {
	acc, ok, err := s.readAccumulator(p)
	if !ok || err != nil {
		var val T3
		return val, ok, err
	}

	if eo := p.ExtractOutputFn(s.Key); eo != nil {
		f, ok := eo.(reflectx.Func1x1)
		if ok {
			return f.Call1x1(acc).(T3), true, nil
		}
		return f.Call([]any{acc})[0].(T3), true, nil
	}

	return acc.(T3), true, nil
}

// Clear is used to clear this instance of global pipeline state representing a combiner.
func (s *Combining[T1, T2, T3]) Clear(p Provider) error {
	return p.ClearValueState(Transaction{
		Key:  s.Key,
		Type: TransactionTypeClear,
	})
}

func (s *Combining[T1, T2, T3]) readAccumulator(p Provider) (any, bool, error) {
	// This replays any writes that have happened to this value since we last read
	// For more detail, see "State Transactionality" below for buffered transactions
	cur, bufferedTransactions, err := p.ReadValueState(s.Key)
	if err != nil {
		var val T1
		return val, false, err
	}
	for _, t := range bufferedTransactions {
		switch t.Type {
		case TransactionTypeSet:
			cur = t.Val
		case TransactionTypeClear:
			cur = nil
		}
	}
	if cur == nil {
		if ca := p.CreateAccumulatorFn(s.Key); ca != nil {
			f, ok := ca.(reflectx.Func0x1)
			if ok {
				return f.Call0x1(), true, nil
			}
			return f.Call([]any{})[0], true, nil
		}
		var val T1
		return val, false, nil
	}

	return cur, true, nil
}

// StateKey returns the key for this pipeline state entry.
func (s Combining[T1, T2, T3]) StateKey() string {
	return s.Key
}

// KeyCoderType returns nil since combining state types aren't keyed.
func (s Combining[T1, T2, T3]) KeyCoderType() reflect.Type {
	return nil
}

// CoderType returns the type of the bag state which should be used for a coder.
func (s Combining[T1, T2, T3]) CoderType() reflect.Type {
	var t T1
	return reflect.TypeOf(t)
}

// StateType returns the type of the state (in this case always Bag).
func (s Combining[T1, T2, T3]) StateType() TypeEnum {
	return TypeCombining
}

// GetCombineFn returns this state instance's CombineFn
func (s Combining[T1, T2, T3]) GetCombineFn() any {
	return s.combineFn
}

// MakeCombiningState is a factory function to create an instance of Combining state with the given key and combiner
// when the combiner may have different types for its accumulator, input, and output.
// Takes 3 generic constraints [T1, T2, T3 any] representing the accumulator/input/output types respectively.
// If no accumulator or output types are defined, use the input type.
func MakeCombiningState[T1, T2, T3 any](k string, combiner any) Combining[T1, T2, T3] {
	return Combining[T1, T2, T3]{
		Key:       k,
		combineFn: combiner,
	}
}

// Map is used to read and write global pipeline state representing a map.
// Key represents the key used to lookup this state (not the key of map entries).
type Map[K comparable, V any] struct {
	Key string
}

// Put is used to write a key/value pair to this instance of global map state.
func (s *Map[K, V]) Put(p Provider, key K, val V) error {
	return p.WriteMapState(Transaction{
		Key:    s.Key,
		Type:   TransactionTypeSet,
		MapKey: key,
		Val:    val,
	})
}

// Keys is used to read the keys of this map state.
// When a value is not found, returns an empty list and false.
func (s *Map[K, V]) Keys(p Provider) ([]K, bool, error) {
	// This replays any writes that have happened to this value since we last read
	// For more detail, see "State Transactionality" below for buffered transactions
	initialValue, bufferedTransactions, err := p.ReadMapStateKeys(s.Key)
	if err != nil {
		return []K{}, false, err
	}
	cur := []K{}
	for _, v := range initialValue {
		cur = append(cur, v.(K))
	}
	for _, t := range bufferedTransactions {
		switch t.Type {
		case TransactionTypeSet:
			seen := false
			mk := t.MapKey.(K)
			for _, k := range cur {
				if k == mk {
					seen = true
				}
			}
			if !seen {
				cur = append(cur, mk)
			}
		case TransactionTypeClear:
			if t.MapKey == nil {
				cur = []K{}
			} else {
				k := t.MapKey.(K)
				for idx, v := range cur {
					if v == k {
						// Remove this key since its been cleared
						cur[idx] = cur[len(cur)-1]
						cur = cur[:len(cur)-1]
						break
					}
				}
			}
		}
	}
	if len(cur) == 0 {
		return cur, false, nil
	}
	return cur, true, nil
}

// Get is used to read a value given a key.
// When a value is not found, returns the 0 value and false.
func (s *Map[K, V]) Get(p Provider, key K) (V, bool, error) {
	// This replays any writes that have happened to this value since we last read
	// For more detail, see "State Transactionality" below for buffered transactions
	cur, bufferedTransactions, err := p.ReadMapStateValue(s.Key, key)
	if err != nil {
		var val V
		return val, false, err
	}
	for _, t := range bufferedTransactions {
		switch t.Type {
		case TransactionTypeSet:
			if t.MapKey.(K) == key {
				cur = t.Val
			}
		case TransactionTypeClear:
			if t.MapKey == nil || t.MapKey.(K) == key {
				cur = nil
			}
		}
	}
	if cur == nil {
		var val V
		return val, false, nil
	}
	return cur.(V), true, nil
}

// Remove deletes an entry from this instance of map state.
func (s *Map[K, V]) Remove(p Provider, key K) error {
	return p.ClearMapStateKey(Transaction{
		Key:    s.Key,
		Type:   TransactionTypeClear,
		MapKey: key,
	})
}

// Clear deletes all entries from this instance of map state.
func (s *Map[K, V]) Clear(p Provider) error {
	return p.ClearMapState(Transaction{
		Key:  s.Key,
		Type: TransactionTypeClear,
	})
}

// StateKey returns the key for this pipeline state entry.
func (s Map[K, V]) StateKey() string {
	return s.Key
}

// KeyCoderType returns the type of the value state which should be used for a coder for map keys.
func (s Map[K, V]) KeyCoderType() reflect.Type {
	var k K
	return reflect.TypeOf(k)
}

// CoderType returns the type of the value state which should be used for a coder for map values.
func (s Map[K, V]) CoderType() reflect.Type {
	var v V
	return reflect.TypeOf(v)
}

// StateType returns the type of the state (in this case always Map).
func (s Map[K, V]) StateType() TypeEnum {
	return TypeMap
}

// MakeMapState is a factory function to create an instance of MapState with the given key.
func MakeMapState[K comparable, V any](k string) Map[K, V] {
	return Map[K, V]{
		Key: k,
	}
}

// Set is used to read and write global pipeline state representing a Set.
// Key represents the key used to lookup this state (not the key of Set entries).
type Set[K comparable] struct {
	Key string
}

// Add is used to write a key to this instance of global Set state.
func (s *Set[K]) Add(p Provider, key K) error {
	return p.WriteMapState(Transaction{
		Key:    s.Key,
		Type:   TransactionTypeSet,
		MapKey: key,
		Val:    true,
	})
}

// Keys is used to read the keys of this set state.
// When a value is not found, returns an empty list and false.
func (s *Set[K]) Keys(p Provider) ([]K, bool, error) {
	// This replays any writes that have happened to this value since we last read
	// For more detail, see "State Transactionality" below for buffered transactions
	initialValue, bufferedTransactions, err := p.ReadMapStateKeys(s.Key)
	if err != nil {
		return []K{}, false, err
	}
	cur := []K{}
	for _, v := range initialValue {
		cur = append(cur, v.(K))
	}
	for _, t := range bufferedTransactions {
		switch t.Type {
		case TransactionTypeSet:
			seen := false
			mk := t.MapKey.(K)
			for _, k := range cur {
				if k == mk {
					seen = true
				}
			}
			if !seen {
				cur = append(cur, mk)
			}
		case TransactionTypeClear:
			if t.MapKey == nil {
				cur = []K{}
			} else {
				k := t.MapKey.(K)
				for idx, v := range cur {
					if v == k {
						// Remove this key since its been cleared
						cur[idx] = cur[len(cur)-1]
						cur = cur[:len(cur)-1]
						break
					}
				}
			}
		}
	}
	if len(cur) == 0 {
		return cur, false, nil
	}
	return cur, true, nil
}

// Contains is used to determine if a given a key exists in the set.
func (s *Set[K]) Contains(p Provider, key K) (bool, error) {
	// This replays any writes that have happened to this value since we last read
	// For more detail, see "State Transactionality" below for buffered transactions
	cur, bufferedTransactions, err := p.ReadMapStateValue(s.Key, key)
	if err != nil {
		return false, err
	}
	for _, t := range bufferedTransactions {
		switch t.Type {
		case TransactionTypeSet:
			if t.MapKey.(K) == key {
				cur = t.Val
			}
		case TransactionTypeClear:
			if t.MapKey == nil || t.MapKey.(K) == key {
				cur = nil
			}
		}
	}
	if cur == nil {
		return false, nil
	}
	return true, nil
}

// Remove deletes an entry from this instance of set state.
func (s Set[K]) Remove(p Provider, key K) error {
	return p.ClearMapStateKey(Transaction{
		Key:    s.Key,
		Type:   TransactionTypeClear,
		MapKey: key,
	})
}

// Clear deletes all entries from this instance of set state.
func (s Set[K]) Clear(p Provider) error {
	return p.ClearMapState(Transaction{
		Key:  s.Key,
		Type: TransactionTypeClear,
	})
}

// StateKey returns the key for this pipeline state entry.
func (s Set[K]) StateKey() string {
	return s.Key
}

// KeyCoderType returns the type of the value state which should be used for a coder for set keys.
func (s Set[K]) KeyCoderType() reflect.Type {
	var k K
	return reflect.TypeOf(k)
}

// CoderType returns the type of the coder used for values, in this case nil since there are no values associated with a set.
func (s Set[K]) CoderType() reflect.Type {
	// A bool coder is used later, but it does not need to be passed around or visible to users.
	return nil
}

// StateType returns the type of the state (in this case always Set).
func (s Set[K]) StateType() TypeEnum {
	return TypeSet
}

// MakeSetState is a factory function to create an instance of SetState with the given key.
func MakeSetState[K comparable](k string) Set[K] {
	return Set[K]{
		Key: k,
	}
}
