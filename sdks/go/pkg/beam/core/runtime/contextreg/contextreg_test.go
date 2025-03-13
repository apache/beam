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

package contextreg

import (
	"context"
	"testing"
)

func TestPTransformExtractor(t *testing.T) {
	reg := &Registry{}

	type keyType string
	key1 := keyType("annotation1")
	key2 := keyType("annotation2")
	key3 := keyType("annotation3")

	reg.TransformExtractor(func(ctx context.Context) TransformMetadata {
		v := ctx.Value(key1).(string)
		return TransformMetadata{
			Annotations: map[string][]byte{
				"beam:test:annotation": []byte(v),
			},
		}
	})
	reg.TransformExtractor(func(ctx context.Context) TransformMetadata {
		v := ctx.Value(key2).(string)
		return TransformMetadata{
			Annotations: map[string][]byte{
				"beam:test:annotation2": []byte(v),
			},
		}
	})
	// Override the extaction for result annotation to use the last set version.
	reg.TransformExtractor(func(ctx context.Context) TransformMetadata {
		v := ctx.Value(key3).(string)
		return TransformMetadata{
			Annotations: map[string][]byte{
				"beam:test:annotation": []byte(v),
			},
		}
	})

	ctx := context.Background()
	// Set all 3 distinct context values.
	ctx = context.WithValue(ctx, key1, "never seen")
	want2 := "want_value2"
	ctx = context.WithValue(ctx, key2, want2)
	want3 := "want_value3"
	ctx = context.WithValue(ctx, key3, want3)

	ptrans := reg.ExtractTransformMetadata(ctx)

	key := "beam:test:annotation"
	if got, want := string(ptrans.Annotations[key]), want3; got != want {
		t.Errorf("extracted annotation %q = %q, want %q", key, got, want)
	}
	key = "beam:test:annotation2"
	if got, want := string(ptrans.Annotations[key]), want2; got != want {
		t.Errorf("extracted annotation %q = %q, want %q", key, got, want)
	}
	if got, want := len(ptrans.Annotations), 2; got != want {
		t.Errorf("extracted annotation %q = %q, want %q - have %v", key, got, want, ptrans)
	}
}

func TestHintExtractor(t *testing.T) {
	reg := &Registry{}

	type keyType string
	hintKey := keyType("hint")

	reg.EnvExtrator(func(ctx context.Context) EnvironmentMetadata {
		v := ctx.Value(hintKey).(string)
		return EnvironmentMetadata{
			ResourceHints: map[string][]byte{
				"beam:test:hint": []byte(v),
			},
		}
	})

	ctx := context.Background()
	wantedHint := "hint"
	ctx = context.WithValue(ctx, hintKey, wantedHint)

	env := reg.ExtractEnvironmentMetadata(ctx)

	key := "beam:test:hint"
	if got, want := string(env.ResourceHints[key]), wantedHint; got != want {
		t.Errorf("extracted annotation %q = %q, want %q", key, got, want)
	}
	if got, want := len(env.ResourceHints), 1; got != want {
		t.Errorf("extracted annotation %q = %q, want %q - have %v", key, got, want, env)
	}
}
