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
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.PullSubscriber;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.util.Timestamps;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.Duration;
import org.joda.time.Instant;

class PerPartitionSdf extends DoFn<Partition, SequencedMessage> {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final Duration maxSleepTime;
  private final SerializableBiFunction<Partition, Offset, PullSubscriber<SequencedMessage>>
      subscriberFactory;
  private final SerializableFunction<Partition, Committer> committerFactory;
  private final SerializableSupplier<Sleeper> sleeperSupplier;
  private final SerializableFunction<Partition, InitialOffsetReader> offsetReaderFactory;
  private final SerializableBiFunction<
          Partition, OffsetRange, RestrictionTracker<OffsetRange, OffsetByteProgress>>
      trackerFactory;

  Duration sleepTimeRemaining;

  PerPartitionSdf(
      Duration maxSleepTime,
      SerializableBiFunction<Partition, Offset, PullSubscriber<SequencedMessage>> subscriberFactory,
      SerializableFunction<Partition, Committer> committerFactory,
      SerializableSupplier<Sleeper> sleeperSupplier,
      SerializableFunction<Partition, InitialOffsetReader> offsetReaderFactory,
      SerializableBiFunction<
              Partition, OffsetRange, RestrictionTracker<OffsetRange, OffsetByteProgress>>
          trackerFactory) {
    this.maxSleepTime = maxSleepTime;
    this.sleepTimeRemaining = maxSleepTime;
    this.subscriberFactory = subscriberFactory;
    this.committerFactory = committerFactory;
    this.sleeperSupplier = sleeperSupplier;
    this.offsetReaderFactory = offsetReaderFactory;
    this.trackerFactory = trackerFactory;
  }

  private List<SequencedMessage> doPoll(PullSubscriber<SequencedMessage> subscriber)
      throws Exception {
    Sleeper sleeper = sleeperSupplier.get();
    while (sleepTimeRemaining.isLongerThan(Duration.ZERO)) {
      List<SequencedMessage> messages = subscriber.pull();
      if (!messages.isEmpty()) {
        return messages;
      }
      Duration sleepTime =
          Collections.min(ImmutableList.of(sleepTimeRemaining, Duration.millis(50)));
      sleepTimeRemaining = sleepTimeRemaining.minus(sleepTime);
      sleeper.sleep(sleepTime.getMillis());
    }
    return ImmutableList.of();
  }

  @ProcessElement
  public ProcessContinuation processElement(
      RestrictionTracker<OffsetRange, OffsetByteProgress> tracker,
      @Element Partition partition,
      OutputReceiver<SequencedMessage> receiver)
      throws Exception {
    logger.atInfo().log("Starting processing for partition " + partition);
    sleepTimeRemaining = maxSleepTime;
    Committer committer = committerFactory.apply(partition);
    committer.startAsync().awaitRunning();
    try (PullSubscriber<SequencedMessage> subscriber =
        subscriberFactory.apply(partition, Offset.of(tracker.currentRestriction().getFrom()))) {
      while (true) {
        List<SequencedMessage> messages = doPoll(subscriber);
        // We polled for as long as possible, yield to the runtime to allow it to reschedule us on
        // a new task.
        if (messages.isEmpty()) {
          logger.atInfo().log("Yielding due to timeout on partition " + partition);
          return ProcessContinuation.resume();
        }
        Offset lastOffset = Offset.of(Iterables.getLast(messages).getCursor().getOffset());
        long byteSize = messages.stream().mapToLong(SequencedMessage::getSizeBytes).sum();
        if (tracker.tryClaim(OffsetByteProgress.of(lastOffset, byteSize))) {
          messages.forEach(
              message ->
                  receiver.outputWithTimestamp(
                      message, new Instant(Timestamps.toMillis(message.getPublishTime()))));
          committer.commitOffset(Offset.of(lastOffset.value() + 1)).get();
        } else {
          logger.atInfo().log("Stopping partition " + partition);
          return ProcessContinuation.stop();
        }
      }
    } finally {
      committer.stopAsync().awaitTerminated();
    }
  }

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element Partition partition) {
    try (InitialOffsetReader reader = offsetReaderFactory.apply(partition)) {
      Offset offset = reader.read();
      return new OffsetRange(offset.value(), Long.MAX_VALUE /* open interval */);
    }
  }

  @NewTracker
  public RestrictionTracker<OffsetRange, OffsetByteProgress> newTracker(
      @Element Partition partition, @Restriction OffsetRange range) {
    return trackerFactory.apply(partition, range);
  }
}
