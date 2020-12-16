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
package org.apache.beam.sdk.io.gcp.pubsublite;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.internal.wire.Committer;
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
  private final SubscriptionPartitionProcessorFactory processorFactory;
  private final SerializableFunction<SubscriptionPartition, InitialOffsetReader>
      offsetReaderFactory;
  private final SerializableBiFunction<
          SubscriptionPartition, OffsetRange, RestrictionTracker<OffsetRange, OffsetByteProgress>>
      trackerFactory;
  private final SerializableFunction<SubscriptionPartition, Committer> committerFactory;

  PerSubscriptionPartitionSdf(
      Duration maxSleepTime,
      SerializableFunction<SubscriptionPartition, InitialOffsetReader> offsetReaderFactory,
      SerializableBiFunction<
              SubscriptionPartition,
              OffsetRange,
              RestrictionTracker<OffsetRange, OffsetByteProgress>>
          trackerFactory,
      SubscriptionPartitionProcessorFactory processorFactory,
      SerializableFunction<SubscriptionPartition, Committer> committerFactory) {
    this.maxSleepTime = maxSleepTime;
    this.processorFactory = processorFactory;
    this.offsetReaderFactory = offsetReaderFactory;
    this.trackerFactory = trackerFactory;
    this.committerFactory = committerFactory;
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
      RestrictionTracker<OffsetRange, OffsetByteProgress> tracker,
      @Element SubscriptionPartition subscriptionPartition,
      OutputReceiver<SequencedMessage> receiver,
      BundleFinalizer finalizer)
      throws Exception {
    try (SubscriptionPartitionProcessor processor =
        processorFactory.newProcessor(subscriptionPartition, tracker, receiver)) {
      processor.start();
      ProcessContinuation result = processor.waitForCompletion(maxSleepTime);
      processor
          .lastClaimed()
          .ifPresent(
              lastClaimedOffset ->
                  finalizer.afterBundleCommit(
                      Instant.ofEpochMilli(Long.MAX_VALUE),
                      () -> {
                        Committer committer = committerFactory.apply(subscriptionPartition);
                        committer.startAsync().awaitRunning();
                        // Commit the next-to-deliver offset.
                        committer.commitOffset(Offset.of(lastClaimedOffset.value() + 1)).get();
                        committer.stopAsync().awaitTerminated();
                      }));
      return result;
    }
  }

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element SubscriptionPartition subscriptionPartition) {
    try (InitialOffsetReader reader = offsetReaderFactory.apply(subscriptionPartition)) {
      Offset offset = reader.read();
      return new OffsetRange(offset.value(), Long.MAX_VALUE /* open interval */);
    }
  }

  @NewTracker
  public RestrictionTracker<OffsetRange, OffsetByteProgress> newTracker(
      @Element SubscriptionPartition subscriptionPartition, @Restriction OffsetRange range) {
    return trackerFactory.apply(subscriptionPartition, range);
  }
}
