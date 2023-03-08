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

// Package pubsubio provides access to Pub/Sub on Dataflow streaming.
//
// This implementation only functions on the Dataflow runner.
//
// See https://cloud.google.com/dataflow/docs/concepts/streaming-with-cloud-pubsub
// for details on using Pub/Sub with Dataflow.
package pubsubio

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/pubsubx"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
	"google.golang.org/protobuf/proto"
)

var (
	readURN  = "beam:transform:pubsub_read:v1"
	writeURN = "beam:transform:pubsub_write:v1"
)

func init() {
	register.Function2x1(unmarshalMessageFn)
	register.Function2x1(marshalMessageFn)
	register.Function2x0(wrapInMessage)
	register.Function2x0(wrapInMessage)
	register.Emitter1[[]byte]()
	register.Emitter1[*pb.PubsubMessage]()
}

// ReadOptions represents options for reading from PubSub.
type ReadOptions struct {
	Subscription       string
	IDAttribute        string
	TimestampAttribute string
	WithAttributes     bool
}

// Read reads an unbounded number of PubSubMessages from the given
// pubsub topic. It produces an unbounded PCollecton<*PubSubMessage>,
// if WithAttributes is set, or an unbounded PCollection<[]byte>.
func Read(s beam.Scope, project, topic string, opts *ReadOptions) beam.PCollection {
	s = s.Scope("pubsubio.Read")

	payload := &pipepb.PubSubReadPayload{
		Topic: pubsubx.MakeQualifiedTopicName(project, topic),
	}
	if opts != nil {
		payload.IdAttribute = opts.IDAttribute
		payload.TimestampAttribute = opts.TimestampAttribute
		if opts.Subscription != "" {
			payload.Subscription = pubsubx.MakeQualifiedSubscriptionName(project, opts.Subscription)
		}
		payload.WithAttributes = opts.WithAttributes
	}

	out := beam.External(s, readURN, protox.MustEncode(payload), nil, []beam.FullType{typex.New(reflectx.ByteSlice)}, false)
	if opts != nil && opts.WithAttributes {
		return beam.ParDo(s, unmarshalMessageFn, out[0])
	}
	return out[0]
}

func unmarshalMessageFn(raw []byte, emit func(*pb.PubsubMessage)) error {
	var msg pb.PubsubMessage
	if err := proto.Unmarshal(raw, &msg); err != nil {
		return err
	}
	emit(&msg)
	return nil
}

func wrapInMessage(raw []byte, emit func(*pb.PubsubMessage)) {
	emit(&pb.PubsubMessage{
		Data: raw,
	})
}

func marshalMessageFn(in *pb.PubsubMessage, emit func([]byte)) error {
	out, err := proto.Marshal(in)
	if err != nil {
		return err
	}
	emit(out)
	return nil
}

var pubSubMessageT = reflect.TypeOf((*pb.PubsubMessage)(nil))

// Write writes PubSubMessages or []bytes to the given pubsub topic.
// Panics if the input pcollection type is not one of those two types.
//
// When given []bytes, they are first wrapped in PubSubMessages.
//
// Note: Doesn't function in batch pipelines.
func Write(s beam.Scope, project, topic string, col beam.PCollection) {
	s = s.Scope("pubsubio.Write")

	payload := &pipepb.PubSubWritePayload{
		Topic: pubsubx.MakeQualifiedTopicName(project, topic),
	}

	out := col
	if col.Type().Type() == reflectx.ByteSlice {
		out = beam.ParDo(s, wrapInMessage, col)
	}
	if out.Type().Type() != pubSubMessageT {
		panic(fmt.Sprintf("pubsubio.Write only accepts PCollections of %v and %v, received %v", pubSubMessageT, reflectx.ByteSlice, col.Type().Type()))
	}
	marshaled := beam.ParDo(s, marshalMessageFn, out)
	beam.External(s, writeURN, protox.MustEncode(payload), []beam.PCollection{marshaled}, nil, false)
}
