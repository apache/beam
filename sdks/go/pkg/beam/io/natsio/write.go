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

package natsio

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func init() {
	register.DoFn3x1[context.Context, ProducerMessage, func(ack PublishAck), error](&writeFn{})
	register.Emitter1[PublishAck]()

	beam.RegisterType(reflect.TypeOf((*ProducerMessage)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*PublishAck)(nil)).Elem())
}

// ProducerMessage represents a message to be published to NATS.
type ProducerMessage struct {
	Subject string
	ID      string
	Headers map[string][]string
	Data    []byte
}

// PublishAck represents an acknowledgement from NATS after publishing a message.
type PublishAck struct {
	Stream    string
	Subject   string
	ID        string
	Sequence  uint64
	Duplicate bool
}

// Write writes a PCollection<ProducerMessage> to NATS JetStream and returns a
// PCollection<PublishAck> of the acknowledged messages. The ID field can be set in the
// ProducerMessage to utilize JetStream's support for deduplication of messages.
// Write takes a variable number of WriteOptionFn to configure the write operation:
//   - UserCredentials: path to the user credentials file. Defaults to empty.
func Write(s beam.Scope, uri string, col beam.PCollection, opts ...WriteOptionFn) beam.PCollection {
	s = s.Scope("natsio.Write")

	option := &writeOption{}
	for _, opt := range opts {
		opt(option)
	}

	return beam.ParDo(s, newWriteFn(uri, option), col)
}

type writeFn struct {
	natsFn
}

func newWriteFn(uri string, option *writeOption) *writeFn {
	return &writeFn{
		natsFn: natsFn{
			URI:       uri,
			CredsFile: option.CredsFile,
		},
	}
}

func (fn *writeFn) ProcessElement(
	ctx context.Context,
	elem ProducerMessage,
	emit func(PublishAck),
) error {
	msg := &nats.Msg{
		Subject: elem.Subject,
		Data:    elem.Data,
		Header:  elem.Headers,
	}

	var opts []jetstream.PublishOpt
	if elem.ID != "" {
		opts = append(opts, jetstream.WithMsgID(elem.ID))
	}

	ack, err := fn.js.PublishMsg(ctx, msg, opts...)
	if err != nil {
		return fmt.Errorf("error publishing message: %v", err)
	}

	pubAck := PublishAck{
		Stream:    ack.Stream,
		Subject:   elem.Subject,
		ID:        elem.ID,
		Sequence:  ack.Sequence,
		Duplicate: ack.Duplicate,
	}
	emit(pubAck)

	return nil
}
