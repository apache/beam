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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.util.Timestamps;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
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
  @SuppressWarnings("argument.type.incompatible")
  public ProcessContinuation runFor(Duration duration) {
    Instant maxReadTime = Instant.now().plus(duration);
    while (subscriber.isRunning()) {
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
      // Try waiting for new data.
      try {
        Duration readTime = new Duration(Instant.now(), maxReadTime);
        Future<Void> onData = subscriber.onData();
        checkArgumentNotNull(onData);
        onData.get(readTime.getMillis(), TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // Read timed out without us being cut off, yield to the runtime.
        return ProcessContinuation.resume();
      } catch (InterruptedException | ExecutionException e2) {
        // We should never be interrupted by beam, and onData should never return an error.
        throw new RuntimeException(e2);
      }
    }
    // Subscriber is no longer running, it has likely failed. Yield to the runtime to retry reading
    // with a new subscriber.
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
