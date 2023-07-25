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

// Package nativepubsubio contains a Golang implementation of streaming reads
// and writes to PubSub. This is not as fully featured as the cross-language
// pubsubio package present in the Beam Go repository and should not be used
// in place of it.
package nativepubsubio

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/pubsubx"
)

func init() {
	register.DoFn5x2[context.Context, beam.BundleFinalization, *sdf.LockRTracker, []byte, func(beam.EventTime, []byte), sdf.ProcessContinuation, error](&pubSubRead{})
	register.DoFn2x1[context.Context, []byte, error](&pubSubWrite{})
	register.Emitter2[beam.EventTime, []byte]()
}

// pubSubRead is a structural DoFn representing a read from a given subscription ID.
type pubSubRead struct {
	ProjectID         string
	Subscription      string
	client            *pubsub.Client
	processedMessages []*pubsub.Message
}

// newPubSubRead inserts an unbounded read from a PubSub topic into the pipeline. If an existing subscription
// is provided, the DoFn will read using that subscription; otherwise, a new subscription to the topic
// will be created using the provided subscription name.
func newPubSubRead(ctx context.Context, projectID, topic, subscription string) (*pubSubRead, error) {
	if topic == "" {
		return nil, errors.New("please provide either a topic to read from")
	}
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	top := client.Topic(topic)
	if ok, err := top.Exists(ctx); !ok || err != nil {
		return nil, fmt.Errorf("failed to get topic; exists: %v, error: %v", ok, err)
	}
	sub, err := pubsubx.EnsureSubscription(ctx, client, topic, subscription)
	if err != nil {
		return nil, err
	}
	return &pubSubRead{ProjectID: projectID, Subscription: sub.ID()}, nil
}

// CreateInitialRestriction() establishes the PubSub subscription ID as the
// initial restriction
func (r *pubSubRead) CreateInitialRestriction(_ []byte) string {
	return r.Subscription
}

// CreateTracker wraps the PubSub subscription ID in a StaticRTracker
// and applies a mutex via LockRTracker.
func (r *pubSubRead) CreateTracker(rest string) *sdf.LockRTracker {
	return sdf.NewLockRTracker(NewSubscriptionRTracker(rest))
}

// RestrictionSize always returns 1.0, as the restriction is always 1 subscription.
func (r *pubSubRead) RestrictionSize(_ []byte, rest string) float64 {
	return 1.0
}

// SplitRestriction is a no-op as the restriction cannot be split.
func (r *pubSubRead) SplitRestriction(_ []byte, rest string) []string {
	return []string{rest}
}

// Setup initializes a PubSub client if one has not been created already
func (r *pubSubRead) Setup(ctx context.Context) error {
	if r.client == nil {
		client, err := pubsub.NewClient(ctx, r.ProjectID)
		if err != nil {
			return err
		}
		r.client = client
	}
	return nil
}

var messageTimeout time.Duration = 5 * time.Second

// ProcessElement initializes a PubSub client if one has not been created already, reads from the PubSub subscription,
// and emits elements as it reads them. If no messages are available, the DoFn will schedule itself to resume processing
// later. If polling the subscription returns an error, the error will be logged and the DoFn will not reschedule itself.
func (r *pubSubRead) ProcessElement(ctx context.Context, bf beam.BundleFinalization, rt *sdf.LockRTracker, _ []byte, emit func(beam.EventTime, []byte)) (sdf.ProcessContinuation, error) {
	// Register finalization callback
	bf.RegisterCallback(5*time.Minute, func() error {
		for _, m := range r.processedMessages {
			m.Ack()
		}
		r.processedMessages = nil
		return nil
	})

	for {
		ok := rt.TryClaim(r.Subscription)
		if !ok {
			return sdf.ResumeProcessingIn(5 * time.Second), nil
		}
		sub := r.client.Subscription(r.Subscription)
		canCtx, cFn := context.WithCancel(ctx)

		// Because emitters are not thread safe and synchronous Receive() behavior
		// is deprecated, we have to collect messages in a goroutine and pipe them
		// out through a channel.
		messChan := make(chan *pubsub.Message, 1)
		go func(sendch chan<- *pubsub.Message) {
			err := sub.Receive(canCtx, func(ctx context.Context, m *pubsub.Message) {
				messChan <- m
			})
			if (err != nil) && (err != context.Canceled) {
				log.Errorf(ctx, "error reading from PubSub: %v, stopping processing", err)
				cFn()
				close(messChan)
			}
		}(messChan)

		timeout := time.NewTimer(messageTimeout)
		for {
			select {
			case m, ok := <-messChan:
				if !ok {
					log.Debug(ctx, "stopping bundle processing")
					return sdf.StopProcessing(), nil
				}
				r.processedMessages = append(r.processedMessages, m)
				emit(beam.EventTime(m.PublishTime.UnixMilli()), m.Data)
				if !timeout.Stop() {
					<-timeout.C
				}
				timeout.Reset(messageTimeout)
			case <-timeout.C:
				log.Debugf(ctx, "cancelling receive context, scheduling resumption")
				cFn()
				return sdf.ResumeProcessingIn(10 * time.Second), nil
			}
		}
	}
}

// Read reads messages from a PubSub topic in a streaming context, outputting
// received messages as a PCollection of byte slices. If the provided subscription
// name exists for the given topic, the DoFn will read from that subscription; otherwise,
// a new subscription with the given subscription name will be created and read from.
//
// This is an example and subject to change, including its behavior and function signature.
// Please use the cross-language implementation Read() instead.
func Read(ctx context.Context, s beam.Scope, project, topic, subscription string) beam.PCollection {
	s = s.Scope("pubsubio.NativeRead")

	psRead, err := newPubSubRead(ctx, project, topic, subscription)
	if err != nil {
		panic(err)
	}
	return beam.ParDo(s, psRead, beam.Impulse(s))
}

// pubSubWrite is a structural DoFn representing writes to a given PubSub topic.
type pubSubWrite struct {
	ProjectID string
	Topic     string
	client    *pubsub.Client
}

// Setup initializes a PubSub client if one has not been created already
func (r *pubSubWrite) Setup(ctx context.Context) error {
	if r.client == nil {
		client, err := pubsub.NewClient(ctx, r.ProjectID)
		if err != nil {
			return err
		}
		r.client = client
	}
	return nil
}

// ProcessElement takes a []byte element and publishes it to the provided PubSub
// topic.
func (w *pubSubWrite) ProcessElement(ctx context.Context, elm []byte) error {
	top := w.client.Topic(w.Topic)

	psMess := &pubsub.Message{Data: elm}
	result := top.Publish(ctx, psMess)
	if _, err := result.Get(ctx); err != nil {
		return err
	}
	return nil
}

// newPubSubWrite inserts a write to a PubSub topic into the pipeline.
func newPubSubWrite(ctx context.Context, projectID, topic string) (*pubSubWrite, error) {
	if topic == "" {
		return nil, errors.New("please provide a topic to write to")
	}
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	top := client.Topic(topic)
	if ok, err := top.Exists(ctx); !ok || err != nil {
		return nil, fmt.Errorf("failed to get topic; exists: %v, error: %v", ok, err)
	}
	return &pubSubWrite{ProjectID: projectID, Topic: top.ID()}, nil
}

// Write publishes elements from a PCollection of byte slices to a PubSub topic.
// If the topic does not exist at pipeline construction time, the function will panic.
//
// This is an example and subject to change, including its behavior and function signature.
// Please use the cross-language implementation Write() instead.
func Write(ctx context.Context, s beam.Scope, col beam.PCollection, project, topic string) {
	s = s.Scope("pubsubio.NativeWrite")

	psWrite, err := newPubSubWrite(ctx, project, topic)
	if err != nil {
		panic(err)
	}
	beam.ParDo0(s, psWrite, col)
}
