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

package pubsubio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"testing"
)

func TestRead_BothTopicAndSubscriptionPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when both topic and subscription are set")
		}
	}()

	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	opts := ReadOptions{
		Topic:        "topic",
		Subscription: "sub",
	}
	Read(s, "test-project", opts)
}

func TestRead_NeitherTopicNorSubscriptionPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when neither topic nor subscription is set")
		}
	}()

	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	opts := ReadOptions{}
	Read(s, "test-project", opts)
}
