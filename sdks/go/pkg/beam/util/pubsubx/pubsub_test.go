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

package pubsubx

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestMakeQualifiedTopicName(t *testing.T) {
	project, topic := "foo", "bar"
	got := MakeQualifiedTopicName(project, topic)
	want := "projects/foo/topics/bar"
	if got != want {
		t.Errorf("MakeQualifiedTopicName(%q, %q) = %v, want %v", project, topic, got, want)
	}
}

func TestMakeQualifiedSubscriptionName(t *testing.T) {
	project, topic := "foo", "bar"
	got := MakeQualifiedSubscriptionName(project, topic)
	want := "projects/foo/subscriptions/bar"
	if got != want {
		t.Errorf("MakeQualifiedSubscriptionName(%q, %q) = %v, want %v", project, topic, got, want)
	}
}

func createFakeServerClient(ctx context.Context, project string) (*pubsub.Client, func(), error) {
	// Go with a ~1 MB buffer for the in memory connection.
	srv := pstest.NewServer()
	closer := func() {
		srv.Close()
	}

	// Connect to the server using it.
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		closer()
		return nil, nil, fmt.Errorf("unable to connect to bufconn: %w", err)
	}

	// Use the connection when creating a pubsub client.
	client, err := pubsub.NewClient(ctx, project, option.WithGRPCConn(conn))
	if err != nil {
		closer()
		return nil, nil, fmt.Errorf("unable to create pubsub client with bufconn: %w", err)
	}
	return client, closer, nil
}

func initTestServer(t *testing.T) (context.Context, *pubsub.Client) {
	t.Helper()
	ctx := context.Background()
	client, closer, err := createFakeServerClient(ctx, "project")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closer)
	return ctx, client
}

func TestEnsureTopic(t *testing.T) {
	ctx, client := initTestServer(t)

	topicName := "test_topic"

	if _, err := EnsureTopic(ctx, client, topicName); err != nil {
		t.Fatalf("EnsureTopic(%q) failed: unable to create topic: %v", topicName, err)
	}
	topic := client.Topic(topicName)
	if ok, err := topic.Exists(ctx); err != nil {
		t.Fatalf("topic.Exist() failed: %v", err)
	} else if !ok {
		t.Fatalf("topic.Exist() = false, want true: EnsureTopic(%q) didn't create a topic", topicName)
	}

	// Now cover the exists already path
	if _, err := EnsureTopic(ctx, client, topicName); err != nil {
		t.Fatalf("EnsureTopic(%q) failed: unable to find existing topic: %v", topicName, err)
	}

	if ok, err := topic.Exists(ctx); err != nil {
		t.Fatalf("topic.Exist() failed: %v", err)
	} else if !ok {
		t.Fatalf("topic.Exist() = false, want true: EnsureTopic(%q) didn't return existing topic", topicName)
	}
}

func TestCleanupTopic(t *testing.T) {
	ctx, client := initTestServer(t)
	topicName := "test_topic"
	topic, err := client.CreateTopic(ctx, topicName)
	if err != nil {
		t.Fatalf("client.CreateTopic(%q) failed %v", topicName, err)
	}

	cleanupTopic(ctx, client, topicName)

	if ok, err := topic.Exists(ctx); err != nil {
		t.Fatalf("topic.Exist() failed: %v", err)
	} else if ok {
		t.Fatalf("topic.Exist() = true, want false: CleanupTopic(%q) didn't delete topic", topicName)
	}
}

func TestEnsureSubscription(t *testing.T) {
	ctx, client := initTestServer(t)

	topicName, subName := "test_topic", "test_sub"

	if _, err := EnsureSubscription(ctx, client, topicName, subName); grpc.Code(err) != codes.NotFound {
		t.Fatalf("EnsureSubscription(%q,%q) failed: expected NotFound error (topic doesn't exist): %v", topicName, subName, err)
	}

	if _, err := client.CreateTopic(ctx, topicName); err != nil {
		t.Fatalf("client.CreateTopic(%q) failed %v", topicName, err)
	}

	if _, err := EnsureSubscription(ctx, client, topicName, subName); err != nil {
		t.Fatalf("EnsureSubscription(%q,%q) failed: unable error to create subscription: %v", topicName, subName, err)
	}

	sub := client.Subscription(subName)
	if ok, err := sub.Exists(ctx); err != nil {
		t.Fatalf("sub.Exist() failed: %v", err)
	} else if !ok {
		t.Fatalf("sub.Exist() = false, want true: EnsureTopic(%q) didn't create a topic", topicName)
	}

	// // Now cover the exists already path
	if _, err := EnsureSubscription(ctx, client, topicName, subName); err != nil {
		t.Fatalf("EnsureSubscription(%q,%q) failed: unable error to find existing subscription: %v", topicName, subName, err)
	}
	sub = client.Subscription(subName)
	if ok, err := sub.Exists(ctx); err != nil {
		t.Fatalf("sub.Exist() failed: %v", err)
	} else if !ok {
		t.Fatalf("sub.Exist() = false, want true: EnsureTopic(%q) didn't create a topic", topicName)
	}
}

func TestPublish(t *testing.T) {
	ctx, client := initTestServer(t)

	topicName := "test_topic"

	want := []string{"apple", "banana", "carrot"}
	sub, err := publish(ctx, client, topicName, want...)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	var got []string
	sub.Receive(cctx, func(_ context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, string(msg.Data))
		msg.Ack()
		if len(got) >= len(want) {
			cancel()
		}
	})
	sort.Strings(got)
	if d := cmp.Diff(want, got); d != "" {
		t.Fatalf("publish failed: diff\n%v", d)
	}
}
