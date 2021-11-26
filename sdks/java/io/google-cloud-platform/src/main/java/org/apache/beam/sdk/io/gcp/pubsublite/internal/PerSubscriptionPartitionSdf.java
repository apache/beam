/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.MonotonicallyIncreasing;
import org.joda.time.Duration;
import org.joda.time.Instant;

class PerSubscriptionPartitionSdf extends DoFn<SubscriptionPartition, SequencedMessage> {
  private final Duration maxSleepTime;
  private final ManagedBacklogReaderFactory backlogReaderFactory;
  private final SubscriptionPartitionProcessorFactory processorFactory;
  private final SerializableFunction<SubscriptionPartition, InitialOffsetReader>
      offsetReaderFactory;
  private final SerializableBiFunction<TopicBacklogReader, OffsetByteRange, TrackerWithProgress>
      trackerFactory;
  private final SerializableFunction<SubscriptionPartition, BlockingCommitter> committerFactory;

  PerSubscriptionPartitionSdf(
      Duration maxSleepTime,
      ManagedBacklogReaderFactory backlogReaderFactory,
      SerializableFunction<SubscriptionPartition, InitialOffsetReader> offsetReaderFactory,
      SerializableBiFunction<TopicBacklogReader, OffsetByteRange, TrackerWithProgress>
          trackerFactory,
      SubscriptionPartitionProcessorFactory processorFactory,
      SerializableFunction<SubscriptionPartition, BlockingCommitter> committerFactory) {
    this.maxSleepTime = maxSleepTime;
    this.backlogReaderFactory = backlogReaderFactory;
    this.processorFactory = processorFactory;
    this.offsetReaderFactory = offsetReaderFactory;
    this.trackerFactory = trackerFactory;
    this.committerFactory = committerFactory;
  }

  @Teardown
  public void teardown() {
    backlogReaderFactory.close();
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkState() {
    return Instant.EPOCH;
  }

  @NewWatermarkEstimator
  public MonotonicallyIncreasing newWatermarkEstimator(@WatermarkEstimatorState Instant state) {
    return new MonotonicallyIncreasing(state);
  }

  @ProcessElement
  public ProcessContinuation processElement(
      RestrictionTracker<OffsetByteRange, OffsetByteProgress> tracker,
      @Element SubscriptionPartition subscriptionPartition,
      OutputReceiver<SequencedMessage> receiver)
      throws Exception {
    SubscriptionPartitionProcessor processor =
        processorFactory.newProcessor(subscriptionPartition, tracker, receiver);
    ProcessContinuation result = processor.runFor(maxSleepTime);
    processor
        .lastClaimed()
        .ifPresent(
            lastClaimedOffset -> {
              Offset commitOffset = Offset.of(lastClaimedOffset.value() + 1);
              try {
                committerFactory.apply(subscriptionPartition).commitOffset(commitOffset);
              } catch (Exception e) {
                throw ExtractStatus.toCanonical(e).underlying;
              }
            });
    return result;
  }

  @GetInitialRestriction
  public OffsetByteRange getInitialRestriction(
      @Element SubscriptionPartition subscriptionPartition) {
    try (InitialOffsetReader reader = offsetReaderFactory.apply(subscriptionPartition)) {
      Offset offset = reader.read();
      return OffsetByteRange.of(
          new OffsetRange(offset.value(), Long.MAX_VALUE /* open interval */));
    }
  }

  @NewTracker
  public TrackerWithProgress newTracker(
      @Element SubscriptionPartition subscriptionPartition, @Restriction OffsetByteRange range) {
    return trackerFactory.apply(backlogReaderFactory.newReader(subscriptionPartition), range);
  }

  @GetSize
  public double getSize(
      @Element SubscriptionPartition subscriptionPartition,
      @Restriction OffsetByteRange restriction) {
    if (restriction.getRange().getTo() != Long.MAX_VALUE) {
      return restriction.getByteCount();
    }
    return newTracker(subscriptionPartition, restriction).getProgress().getWorkRemaining();
  }
}
