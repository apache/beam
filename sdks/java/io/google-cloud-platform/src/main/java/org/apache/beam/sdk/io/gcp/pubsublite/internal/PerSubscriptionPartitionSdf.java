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
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PerSubscriptionPartitionSdf extends DoFn<SubscriptionPartition, SequencedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(PerSubscriptionPartitionSdf.class);
  private final ManagedFactory<TopicBacklogReader> backlogReaderFactory;
  private final ManagedFactory<BlockingCommitter> committerFactory;
  private final SubscriptionPartitionProcessorFactory processorFactory;
  private final SerializableFunction<SubscriptionPartition, InitialOffsetReader>
      offsetReaderFactory;
  private final SerializableBiFunction<TopicBacklogReader, OffsetByteRange, TrackerWithProgress>
      trackerFactory;

  PerSubscriptionPartitionSdf(
      ManagedFactory<TopicBacklogReader> backlogReaderFactory,
      ManagedFactory<BlockingCommitter> committerFactory,
      SerializableFunction<SubscriptionPartition, InitialOffsetReader> offsetReaderFactory,
      SerializableBiFunction<TopicBacklogReader, OffsetByteRange, TrackerWithProgress>
          trackerFactory,
      SubscriptionPartitionProcessorFactory processorFactory) {
    this.backlogReaderFactory = backlogReaderFactory;
    this.committerFactory = committerFactory;
    this.processorFactory = processorFactory;
    this.offsetReaderFactory = offsetReaderFactory;
    this.trackerFactory = trackerFactory;
  }

  @Teardown
  public void teardown() throws Exception {
    try (AutoCloseable c1 = committerFactory;
        AutoCloseable c2 = backlogReaderFactory) {}
  }

  /**
   * The initial watermark state is not allowed to return less than the element's input timestamp.
   *
   * <p>The polling logic for identifying new partitions will export all preexisting partitions with
   * very old (EPOCH) initial watermarks, and any new partitions with a recent watermark likely to
   * be before all messages that could exist on that partition given the polling delay.
   */
  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkState(@Timestamp Instant elementTimestamp) {
    return elementTimestamp;
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
    LOG.debug("Starting process for {} at {}", subscriptionPartition, Instant.now());
    SubscriptionPartitionProcessor processor =
        processorFactory.newProcessor(subscriptionPartition, tracker, receiver);
    ProcessContinuation result = processor.run();
    LOG.debug("Starting commit for {} at {}", subscriptionPartition, Instant.now());
    // TODO(dpcollins-google): Move commits to a bundle finalizer for drain correctness
    processor
        .lastClaimed()
        .ifPresent(
            lastClaimed -> {
              try {
                committerFactory
                    .create(subscriptionPartition)
                    .commitOffset(Offset.of(lastClaimed.value() + 1));
              } catch (Exception e) {
                throw ExtractStatus.toCanonical(e).underlying;
              }
            });
    LOG.debug("Finishing process for {} at {}", subscriptionPartition, Instant.now());
    return result;
  }

  @GetInitialRestriction
  public OffsetByteRange getInitialRestriction(
      @Element SubscriptionPartition subscriptionPartition) {
    Offset offset = offsetReaderFactory.apply(subscriptionPartition).read();
    return OffsetByteRange.of(new OffsetRange(offset.value(), Long.MAX_VALUE /* open interval */));
  }

  @NewTracker
  public TrackerWithProgress newTracker(
      @Element SubscriptionPartition subscriptionPartition, @Restriction OffsetByteRange range) {
    return trackerFactory.apply(backlogReaderFactory.create(subscriptionPartition), range);
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
