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
	"math"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func init() {
	register.DoFn5x2[
		context.Context, *watermarkEstimator, *sdf.LockRTracker, []byte,
		func(beam.EventTime, ConsumerMessage), sdf.ProcessContinuation, error,
	](
		&readFn{},
	)
	register.Emitter2[beam.EventTime, ConsumerMessage]()
	beam.RegisterType(reflect.TypeOf((*ConsumerMessage)(nil)).Elem())
}

const (
	defaultFetchSize  = 100
	defaultStartSeqNo = 1
	defaultEndSeqNo   = math.MaxInt64
	fetchTimeout      = 3 * time.Second
	assumedLag        = 1 * time.Second
	resumeDelay       = 5 * time.Second
)

type ConsumerMessage struct {
	Subject        string
	PublishingTime time.Time
	ID             string
	Headers        map[string][]string
	Data           []byte
}

// Read reads messages from NATS JetStream and returns a PCollection<ConsumerMessage>.
// Read takes a variable number of ReadOptionFn to configure the read operation:
//   - UserCredentials: path to the user credentials file. Defaults to empty.
//   - ProcessingTimePolicy: whether to use the pipeline processing time of the messages as the event
//     time. Defaults to true.
//   - PublishingTimePolicy: whether to use the publishing time of the messages as the event time.
//     Defaults to false.
//   - FetchSize: the maximum number of messages to retrieve at a time. Defaults to 100.
//   - StartSeqNo: the start sequence number of messages to read. Defaults to 1.
//   - EndSeqNo: the end sequence number of messages to read (exclusive). Defaults to math.MaxInt64.
func Read(
	s beam.Scope,
	uri string,
	stream string,
	subject string,
	opts ...ReadOptionFn,
) beam.PCollection {
	s = s.Scope("natsio.Read")

	option := &readOption{
		TimePolicy: processingTimePolicy,
		FetchSize:  defaultFetchSize,
		StartSeqNo: defaultStartSeqNo,
		EndSeqNo:   defaultEndSeqNo,
	}

	for _, opt := range opts {
		if err := opt(option); err != nil {
			panic(fmt.Sprintf("natsio.Read: invalid option: %v", err))
		}
	}

	imp := beam.Impulse(s)
	return beam.ParDo(s, newReadFn(uri, stream, subject, option), imp)
}

type readFn struct {
	natsFn
	Stream      string
	Subject     string
	TimePolicy  timePolicy
	FetchSize   int
	StartSeqNo  int64
	EndSeqNo    int64
	timestampFn timestampFn
}

func newReadFn(uri string, stream string, subject string, option *readOption) *readFn {
	return &readFn{
		natsFn: natsFn{
			URI:       uri,
			CredsFile: option.CredsFile,
		},
		Stream:     stream,
		Subject:    subject,
		TimePolicy: option.TimePolicy,
		FetchSize:  option.FetchSize,
		StartSeqNo: option.StartSeqNo,
		EndSeqNo:   option.EndSeqNo,
	}
}

func (fn *readFn) Setup() error {
	if err := fn.natsFn.Setup(); err != nil {
		return err
	}

	fn.timestampFn = fn.TimePolicy.TimestampFn()
	return nil
}

func (fn *readFn) CreateInitialRestriction(_ []byte) offsetrange.Restriction {
	return offsetrange.Restriction{
		Start: fn.StartSeqNo,
		End:   fn.EndSeqNo,
	}
}

func (fn *readFn) SplitRestriction(
	_ []byte,
	rest offsetrange.Restriction,
) []offsetrange.Restriction {
	return []offsetrange.Restriction{rest}
}

func (fn *readFn) RestrictionSize(_ []byte, rest offsetrange.Restriction) (float64, error) {
	if err := fn.natsFn.Setup(); err != nil {
		return -1, err
	}

	rt, err := fn.createRTracker(rest)
	if err != nil {
		return -1, err
	}

	_, remaining := rt.GetProgress()
	return remaining, nil
}

func (fn *readFn) CreateTracker(rest offsetrange.Restriction) (*sdf.LockRTracker, error) {
	rt, err := fn.createRTracker(rest)
	if err != nil {
		return nil, err
	}

	return sdf.NewLockRTracker(rt), nil
}

func (fn *readFn) TruncateRestriction(rt *sdf.LockRTracker, _ []byte) offsetrange.Restriction {
	start := rt.GetRestriction().(offsetrange.Restriction).Start
	return offsetrange.Restriction{
		Start: start,
		End:   start,
	}
}

func (fn *readFn) InitialWatermarkEstimatorState(
	et beam.EventTime,
	_ offsetrange.Restriction,
	_ []byte,
) int64 {
	return et.Milliseconds()
}

func (fn *readFn) CreateWatermarkEstimator(ms int64) *watermarkEstimator {
	return &watermarkEstimator{state: ms}
}

func (fn *readFn) WatermarkEstimatorState(we *watermarkEstimator) int64 {
	return we.state
}

func (fn *readFn) ProcessElement(
	ctx context.Context,
	we *watermarkEstimator,
	rt *sdf.LockRTracker,
	_ []byte,
	emit func(beam.EventTime, ConsumerMessage),
) (sdf.ProcessContinuation, error) {
	startSeqNo := rt.GetRestriction().(offsetrange.Restriction).Start
	cons, err := fn.createConsumer(ctx, startSeqNo)
	if err != nil {
		return sdf.StopProcessing(), err
	}

	for {
		msgs, err := cons.Fetch(fn.FetchSize, jetstream.FetchMaxWait(fetchTimeout))
		if err != nil {
			return nil, fmt.Errorf("error fetching messages: %v", err)
		}

		count := 0
		for msg := range msgs.Messages() {
			metadata, err := msg.Metadata()
			if err != nil {
				return sdf.StopProcessing(), fmt.Errorf("error retrieving metadata: %v", err)
			}

			seqNo := int64(metadata.Sequence.Stream)
			if !rt.TryClaim(seqNo) {
				return sdf.StopProcessing(), nil
			}

			et := fn.timestampFn(metadata.Timestamp)
			consMsg := createConsumerMessage(msg, metadata.Timestamp)
			emit(et, consMsg)

			count++
		}

		if err := msgs.Error(); err != nil {
			return sdf.StopProcessing(), fmt.Errorf("error in message batch: %v", err)
		}

		if count == 0 {
			fn.updateWatermarkManually(we)
			return sdf.ResumeProcessingIn(resumeDelay), nil
		}
	}
}

func (fn *readFn) createRTracker(rest offsetrange.Restriction) (sdf.RTracker, error) {
	if rest.End < math.MaxInt64 {
		return offsetrange.NewTracker(rest), nil
	}

	estimator := newEndEstimator(fn.js, fn.Stream, fn.Subject)
	rt, err := offsetrange.NewGrowableTracker(rest, estimator)
	if err != nil {
		return nil, fmt.Errorf("error creating growable tracker: %v", err)
	}

	return rt, nil
}

func (fn *readFn) createConsumer(
	ctx context.Context,
	startSeqNo int64,
) (jetstream.Consumer, error) {
	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects:   []string{fn.Subject},
		DeliverPolicy:    jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:      uint64(startSeqNo),
		MaxResetAttempts: 5,
	}

	cons, err := fn.js.OrderedConsumer(ctx, fn.Stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating consumer: %v", err)
	}

	return cons, nil
}

func createConsumerMessage(msg jetstream.Msg, publishingTime time.Time) ConsumerMessage {
	return ConsumerMessage{
		Subject:        msg.Subject(),
		PublishingTime: publishingTime,
		ID:             msg.Headers().Get(nats.MsgIdHdr),
		Headers:        msg.Headers(),
		Data:           msg.Data(),
	}
}

func (fn *readFn) updateWatermarkManually(we *watermarkEstimator) {
	t := time.Now().Add(-1 * assumedLag)
	et := fn.timestampFn(t)
	we.ObserveTimestamp(et.ToTime())
}
