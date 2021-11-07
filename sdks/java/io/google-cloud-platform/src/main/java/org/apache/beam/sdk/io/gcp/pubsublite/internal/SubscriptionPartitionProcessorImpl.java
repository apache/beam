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

import static com.google.cloud.pubsublite.internal.wire.ApiServiceUtils.blockingShutdown;

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.wire.Subscriber;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.cloud.pubsublite.proto.FlowControlRequest;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.SettableFuture;
import org.joda.time.Duration;
import org.joda.time.Instant;

class SubscriptionPartitionProcessorImpl extends Listener
    implements SubscriptionPartitionProcessor {
  private final RestrictionTracker<OffsetByteRange, OffsetByteProgress> tracker;
  private final OutputReceiver<SequencedMessage> receiver;
  private final Subscriber subscriber;
  private final SettableFuture<Void> completionFuture = SettableFuture.create();
  private final FlowControlSettings flowControlSettings;
  private Optional<Offset> lastClaimedOffset = Optional.empty();

  @SuppressWarnings("methodref.receiver.bound.invalid")
  SubscriptionPartitionProcessorImpl(
      RestrictionTracker<OffsetByteRange, OffsetByteProgress> tracker,
      OutputReceiver<SequencedMessage> receiver,
      Function<Consumer<List<SequencedMessage>>, Subscriber> subscriberFactory,
      FlowControlSettings flowControlSettings) {
    this.tracker = tracker;
    this.receiver = receiver;
    this.subscriber = subscriberFactory.apply(this::onMessages);
    this.flowControlSettings = flowControlSettings;
  }

  @Override
  @SuppressWarnings("argument.type.incompatible")
  public void start() throws CheckedApiException {
    this.subscriber.addListener(this, SystemExecutors.getFuturesExecutor());
    this.subscriber.startAsync();
    this.subscriber.awaitRunning();
    try {
      this.subscriber.allowFlow(
          FlowControlRequest.newBuilder()
              .setAllowedBytes(flowControlSettings.bytesOutstanding())
              .setAllowedMessages(flowControlSettings.messagesOutstanding())
              .build());
    } catch (Throwable t) {
      throw ExtractStatus.toCanonical(t);
    }
  }

  private void onMessages(List<SequencedMessage> messages) {
    if (completionFuture.isDone()) {
      return;
    }
    Offset lastOffset = Offset.of(Iterables.getLast(messages).getCursor().getOffset());
    long byteSize = messages.stream().mapToLong(SequencedMessage::getSizeBytes).sum();
    if (tracker.tryClaim(OffsetByteProgress.of(lastOffset, byteSize))) {
      lastClaimedOffset = Optional.of(lastOffset);
      messages.forEach(
          message ->
              receiver.outputWithTimestamp(
                  message, new Instant(Timestamps.toMillis(message.getPublishTime()))));
      try {
        subscriber.allowFlow(
            FlowControlRequest.newBuilder()
                .setAllowedBytes(byteSize)
                .setAllowedMessages(messages.size())
                .build());
      } catch (CheckedApiException e) {
        completionFuture.setException(e);
      }
    } else {
      completionFuture.set(null);
    }
  }

  @Override
  public void failed(State from, Throwable failure) {
    completionFuture.setException(ExtractStatus.toCanonical(failure));
  }

  @Override
  public void close() {
    blockingShutdown(subscriber);
  }

  @Override
  @SuppressWarnings("argument.type.incompatible")
  public ProcessContinuation waitForCompletion(Duration duration) {
    try {
      completionFuture.get(duration.getMillis(), TimeUnit.MILLISECONDS);
      // CompletionFuture set with null when tryClaim returned false.
      return ProcessContinuation.stop();
    } catch (TimeoutException ignored) {
      // Timed out waiting, yield to the runtime.
      return ProcessContinuation.resume();
    } catch (ExecutionException e) {
      throw ExtractStatus.toCanonical(e.getCause()).underlying;
    } catch (Throwable t) {
      throw ExtractStatus.toCanonical(t).underlying;
    }
  }

  @Override
  public Optional<Offset> lastClaimed() {
    return lastClaimedOffset;
  }
}
