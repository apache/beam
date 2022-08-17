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
	"reflect"
)

type TransactionType_Enum int32

const (
	TransactionType_Set   TransactionType_Enum = 0
	TransactionType_Clear TransactionType_Enum = 1
)

var (
	ProviderType = reflect.TypeOf((*Provider)(nil)).Elem()
)

// TODO(#20510) - add other forms of state (MapState, BagState, CombiningState), prefetch, and clear.

// Transaction is used to represent a pending state transaction. This should not be manipulated directly;
// it is primarily used for implementations of the Provider interface to talk to the various State objects.
type Transaction struct {
	Key  string
	Type TransactionType_Enum
	Val  interface{}
}

// Provider represents the DoFn parameter used to get and manipulate pipeline state
// stored as key value pairs (https://beam.apache.org/documentation/programming-guide/#state-and-timers).
// This should not be manipulated directly. Instead it should be used as a parameter
// to functions on State objects like state.Value.
type Provider interface {
	ReadValueState(id string) (interface{}, []Transaction, error)
	WriteValueState(val Transaction) error
}

type PipelineState interface {
	StateKey() string
	CoderType() reflect.Type
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
		Type: TransactionType_Set,
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
		case TransactionType_Set:
			cur = t.Val
		case TransactionType_Clear:
			cur = nil
		}
	}
	if cur == nil {
		var val T
		return val, false, nil
	}
	return cur.(T), true, nil
}

// StateKey returns the key for this pipeline state entry.
func (s Value[T]) StateKey() string {
	if s.Key == "" {
		// TODO(#22736) - infer the state from the member variable name, either here or during pipeline construction.
		panic("Value state exists on struct but has not been initialized with a key.")
	}
	return s.Key
}

func (s Value[T]) CoderType() reflect.Type {
	var t T
	return reflect.TypeOf(t)
}

// MakeValueState is a factory function to create an instance of ValueState with the given key.
func MakeValueState[T any](k string) Value[T] {
	return Value[T]{
		Key: k,
	}
}
