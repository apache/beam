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

// Package pubsubio provides access to PubSub on Dataflow streaming.
// Experimental.
package pubsubio

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/io/pubsubio/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/pubsubx"
	"github.com/golang/protobuf/proto"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*pb.PubsubMessage)(nil)).Elem())
	beam.RegisterFunction(unmarshalMessageFn)
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

	payload := &v1.PubSubPayload{
		Op:    v1.PubSubPayload_READ,
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

	out := beam.External(s, v1.PubSubPayloadURN, protox.MustEncode(payload), nil, []beam.FullType{typex.New(reflectx.ByteSlice)})
	if opts.WithAttributes {
		return beam.ParDo(s, unmarshalMessageFn, out[0])
	}
	return out[0]
}

func unmarshalMessageFn(raw []byte) (*pb.PubsubMessage, error) {
	var msg pb.PubsubMessage
	if err := proto.Unmarshal(raw, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// Write writes PubSubMessages or bytes to the given pubsub topic.
func Write(s beam.Scope, project, topic string, col beam.PCollection) {
	s = s.Scope("pubsubio.Write")

	payload := &v1.PubSubPayload{
		Op:    v1.PubSubPayload_WRITE,
		Topic: pubsubx.MakeQualifiedTopicName(project, topic),
	}

	out := col
	if col.Type().Type() != reflectx.ByteSlice {
		out = beam.ParDo(s, proto.Marshal, col)
		payload.WithAttributes = true
	}
	beam.External(s, v1.PubSubPayloadURN, protox.MustEncode(payload), []beam.PCollection{out}, nil)
}
