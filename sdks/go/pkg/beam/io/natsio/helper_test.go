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
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func newServer(t *testing.T) *server.Server {
	t.Helper()

	opts := &test.DefaultTestOptions
	opts.Port = server.RANDOM_PORT
	opts.JetStream = true

	srv := test.RunServer(opts)
	t.Cleanup(srv.Shutdown)

	return srv
}

func newConn(t *testing.T, uri string) *nats.Conn {
	t.Helper()

	conn, err := nats.Connect(uri)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	t.Cleanup(conn.Close)

	return conn
}

func newJetStream(t *testing.T, conn *nats.Conn) jetstream.JetStream {
	t.Helper()

	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatalf("Failed to create JetStream instance: %v", err)
	}

	return js
}

func createStream(
	t *testing.T,
	ctx context.Context,
	js jetstream.JetStream,
	stream string,
	subjects []string,
) jetstream.Stream {
	t.Helper()

	cfg := jetstream.StreamConfig{
		Name:     stream,
		Subjects: subjects,
	}
	str, err := js.CreateStream(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	t.Cleanup(func() {
		if err := js.DeleteStream(ctx, stream); err != nil {
			t.Fatalf("Failed to delete stream: %v", err)
		}
	})

	return str
}

func createConsumer(
	t *testing.T,
	ctx context.Context,
	js jetstream.JetStream,
	stream string,
	subjects []string,
) jetstream.Consumer {
	t.Helper()

	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects: subjects,
	}
	cons, err := js.OrderedConsumer(ctx, stream, cfg)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	return cons
}

func fetchMessages(t *testing.T, cons jetstream.Consumer, size int) []jetstream.Msg {
	t.Helper()

	msgs, err := cons.FetchNoWait(size)
	if err != nil {
		t.Fatalf("Failed to fetch messages: %v", err)
	}

	var result []jetstream.Msg

	for msg := range msgs.Messages() {
		if err := msg.Ack(); err != nil {
			t.Fatalf("Failed to ack message: %v", err)
		}

		result = append(result, msg)
	}

	return result
}
