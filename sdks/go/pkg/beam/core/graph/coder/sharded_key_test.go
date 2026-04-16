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

package coder

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func TestNewSK(t *testing.T) {
	// Register the ShardedKey[string] instantiation so NewSK can build a
	// FullType with the correct concrete struct.
	skStrType := reflect.TypeOf(typex.ShardedKey[string]{})
	typex.RegisterShardedKeyType(reflect.TypeOf(""), skStrType)

	t.Run("nilKeyCoder_panics", func(t *testing.T) {
		defer func() {
			if p := recover(); p == nil {
				t.Fatal("expected panic on nil keyCoder, got none")
			}
		}()
		NewSK(skStrType, nil)
	})

	t.Run("nonShardedKeyType_panics", func(t *testing.T) {
		defer func() {
			if p := recover(); p == nil {
				t.Fatal("expected panic on non-ShardedKey type, got none")
			}
		}()
		NewSK(reflect.TypeOf(""), NewString())
	})

	t.Run("mismatchedKeyType_panics", func(t *testing.T) {
		defer func() {
			if p := recover(); p == nil {
				t.Fatal("expected panic on mismatched key type, got none")
			}
		}()
		// ShardedKey[string] but key coder encodes bytes — inconsistent
		NewSK(skStrType, NewBytes())
	})

	t.Run("valid", func(t *testing.T) {
		sk := NewSK(skStrType, NewString())
		if sk.Kind != ShardedKey {
			t.Fatalf("Kind = %v, want %v", sk.Kind, ShardedKey)
		}
		if !IsSK(sk) {
			t.Fatalf("IsSK(%v) = false, want true", sk)
		}
		if len(sk.Components) != 1 {
			t.Fatalf("Components = %d, want 1", len(sk.Components))
		}
		if sk.Components[0].Kind != String {
			t.Fatalf("Components[0].Kind = %v, want %v", sk.Components[0].Kind, String)
		}
	})
}

func TestSK_IsDeterministic(t *testing.T) {
	skStrType := reflect.TypeOf(typex.ShardedKey[string]{})
	typex.RegisterShardedKeyType(reflect.TypeOf(""), skStrType)

	detSK := NewSK(skStrType, NewString())
	if !detSK.IsDeterministic() {
		t.Errorf("ShardedKey<string>.IsDeterministic() = false, want true")
	}

	// Build a ShardedKey whose key coder is a non-deterministic custom coder.
	nonDet, err := NewCustomCoder("nonDet", reflect.TypeOf(""),
		func(string) []byte { return nil }, func([]byte) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	nonDetC := &Coder{Kind: Custom, Custom: nonDet, T: typex.New(reflect.TypeOf(""))}
	nonDetSK := NewSK(skStrType, nonDetC)
	if nonDetSK.IsDeterministic() {
		t.Errorf("ShardedKey<nonDet>.IsDeterministic() = true, want false")
	}
}
