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
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.util.Timestamps;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SubscriptionPartitionProcessorImpl implements SubscriptionPartitionProcessor {
  private static final Logger LOG =
      LoggerFactory.getLogger(SubscriptionPartitionProcessorImpl.class);
  private final SubscriptionPartition subscriptionPartition;
  private final RestrictionTracker<OffsetByteRange, OffsetByteProgress> tracker;
  private final OutputReceiver<SequencedMessage> receiver;
  private final MemoryBufferedSubscriber subscriber;
  private Optional<Offset> lastClaimedOffset = Optional.empty();

  // getReadySubscriber doesn't reference the subscriber member.
  @SuppressWarnings("method.invocation.invalid")
  SubscriptionPartitionProcessorImpl(
      SubscriptionPartition subscriptionPartition,
      RestrictionTracker<OffsetByteRange, OffsetByteProgress> tracker,
      OutputReceiver<SequencedMessage> receiver,
      Supplier<MemoryBufferedSubscriber> subscriberFactory) {
    this.subscriptionPartition = subscriptionPartition;
    this.tracker = tracker;
    this.receiver = receiver;
    this.subscriber = getReadySubscriber(subscriberFactory);
  }

  @Override
  public ProcessContinuation run() {
    // Read any available data.
    for (Optional<SequencedMessage> next = subscriber.peek();
        next.isPresent();
        next = subscriber.peek()) {
      SequencedMessage message = next.get();
      Offset messageOffset = Offset.of(message.getCursor().getOffset());
      if (tracker.tryClaim(OffsetByteProgress.of(messageOffset, message.getSizeBytes()))) {
        subscriber.pop();
        lastClaimedOffset = Optional.of(messageOffset);
        receiver.outputWithTimestamp(
            message, new Instant(Timestamps.toMillis(message.getPublishTime())));
      } else {
        // Our claim failed, return stop()
        return ProcessContinuation.stop();
      }
    }
    // There is no more data available, yield to the runtime.
    return ProcessContinuation.resume();
  }

  @Override
  public Optional<Offset> lastClaimed() {
    return lastClaimedOffset;
  }

  private MemoryBufferedSubscriber getReadySubscriber(
      Supplier<MemoryBufferedSubscriber> getOrCreate) {
    Offset startOffset = Offset.of(tracker.currentRestriction().getRange().getFrom());
    while (true) {
      MemoryBufferedSubscriber subscriber = getOrCreate.get();
      Offset fetchOffset = subscriber.fetchOffset();
      if (startOffset.equals(fetchOffset)) {
        subscriber.rebuffer(); // TODO(dpcollins-google): Move this to a bundle finalizer
        return subscriber;
      }
      LOG.info(
          "Discarding subscriber due to mismatch, this should be rare. {}, start: {} fetch: {}",
          subscriptionPartition,
          startOffset,
          fetchOffset);
      try {
        subscriber.stopAsync().awaitTerminated();
      } catch (Exception ignored) {
      }
    }
  }
}
