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

// Package pubsubx contains utilities for working with Google PubSub.
package pubsubx

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

// MakeQualifiedTopicName returns a fully-qualified topic name for
// the given project and topic.
func MakeQualifiedTopicName(project, topic string) string {
	return fmt.Sprintf("projects/%s/topics/%s", project, topic)
}

// MakeQualifiedSubscriptionName returns a fully-qualified subscription name for
// the given project and subscription id.
func MakeQualifiedSubscriptionName(project, subscription string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", project, subscription)
}

// EnsureTopic creates a new topic, if it doesn't exist.
func EnsureTopic(ctx context.Context, client *pubsub.Client, topic string) (*pubsub.Topic, error) {
	ret := client.Topic(topic)

	exists, err := ret.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return client.CreateTopic(ctx, topic)
	}
	return ret, nil
}

// EnsureSubscription creates a new subscription with the given name, if it doesn't exist.
func EnsureSubscription(ctx context.Context, client *pubsub.Client, topic, id string) (*pubsub.Subscription, error) {
	ret := client.Subscription(id)
	exists, err := ret.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		cfg := pubsub.SubscriptionConfig{
			Topic: client.Topic(topic),
		}
		return client.CreateSubscription(ctx, id, cfg)
	}
	return ret, nil
}

// CleanupTopic deletes a topic with all subscriptions and logs any
// error. Useful for defer.
func CleanupTopic(ctx context.Context, project, topic string) {
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		log.Errorf(ctx, "Failed to delete topic %v: %v", topic, err)
	}
	defer client.Close()
	cleanupTopic(ctx, client, topic)
}

func cleanupTopic(ctx context.Context, client *pubsub.Client, topic string) {
	if err := client.Topic(topic).Delete(ctx); err != nil {
		log.Errorf(ctx, "Failed to delete topic %v: %v", topic, err)
	}
}

// Publish is a simple utility for publishing a set of string messages
// serially to a pubsub topic. Small scale use only.
func Publish(ctx context.Context, project, topic string, messages ...string) (*pubsub.Subscription, error) {
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	return publish(ctx, client, topic, messages...)
}

func publish(ctx context.Context, client *pubsub.Client, topic string, messages ...string) (*pubsub.Subscription, error) {
	t, err := EnsureTopic(ctx, client, topic)
	if err != nil {
		return nil, err
	}
	sub, err := EnsureSubscription(ctx, client, topic, fmt.Sprintf("%v.sub.%v", topic, time.Now().Unix()))
	if err != nil {
		return nil, err
	}

	for _, msg := range messages {
		m := &pubsub.Message{
			Data: ([]byte)(msg),
		}
		id, err := t.Publish(ctx, m).Get(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to publish '%v'", msg)
		}
		log.Infof(ctx, "Published %v with id: %v", msg, id)
	}
	return sub, nil
}
