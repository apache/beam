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

import com.google.api.core.ApiFuture;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.CloseableMonitor;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.ProxyService;
import com.google.cloud.pubsublite.internal.PullSubscriber;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;

/** A reader for Pub/Sub Lite that generates a stream of SequencedMessages. */
class PubsubLiteUnboundedReader extends UnboundedReader<SequencedMessage>
    implements OffsetFinalizer {
  private final UnboundedSource<SequencedMessage, ?> source;
  private final CloseableMonitor monitor = new CloseableMonitor();

  @GuardedBy("monitor.monitor")
  private final ImmutableMap<Partition, SubscriberState> subscriberMap;

  private final CommitterProxy committerProxy;

  @GuardedBy("monitor.monitor")
  private final Queue<PartitionedSequencedMessage> messages = new ArrayDeque<>();

  @GuardedBy("monitor.monitor")
  private Optional<StatusException> permanentError = Optional.empty();

  private static class CommitterProxy extends ProxyService {
    private final Consumer<StatusException> permanentErrorSetter;

    CommitterProxy(
        Collection<SubscriberState> states, Consumer<StatusException> permanentErrorSetter)
        throws StatusException {
      this.permanentErrorSetter = permanentErrorSetter;
      addServices(states.stream().map(state -> state.committer).collect(Collectors.toList()));
    }

    @Override
    protected void start() {}

    @Override
    protected void stop() {}

    @Override
    protected void handlePermanentError(StatusException error) {
      permanentErrorSetter.accept(error);
    }
  }

  public PubsubLiteUnboundedReader(
      UnboundedSource<SequencedMessage, ?> source, Map<Partition, SubscriberState> subscriberMap)
      throws StatusException {
    this.source = source;
    this.subscriberMap = ImmutableMap.copyOf(subscriberMap);
    this.committerProxy =
        new CommitterProxy(
            subscriberMap.values(),
            error -> {
              try (CloseableMonitor.Hold h = monitor.enter()) {
                permanentError = Optional.of(permanentError.orElse(error));
              }
            });
    this.committerProxy.startAsync().awaitRunning();
  }

  @Override
  public void finalizeOffsets(Map<Partition, Offset> offsets) throws StatusException {
    List<ApiFuture<Void>> commitFutures = new ArrayList<>();
    try (CloseableMonitor.Hold h = monitor.enter()) {
      for (Partition partition : offsets.keySet()) {
        if (!subscriberMap.containsKey(partition)) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Asked to finalize an offset for partition %s which was not managed by this"
                          + " reader.",
                      partition))
              .asException();
        }
        commitFutures.add(
            subscriberMap.get(partition).committer.commitOffset(offsets.get(partition)));
      }
    }
    // Add outside of monitor in case they are finished inline.
    commitFutures.forEach(
        commitFuture ->
            ExtractStatus.addFailureHandler(
                commitFuture,
                error -> {
                  try (CloseableMonitor.Hold h = monitor.enter()) {
                    if (!permanentError.isPresent()) {
                      permanentError = Optional.of(error);
                    }
                  }
                }));
  }

  static class SubscriberState {
    Instant lastDeliveredPublishTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    Optional<Offset> lastDelivered = Optional.empty();
    PullSubscriber<SequencedMessage> subscriber;
    Committer committer;
  }

  @AutoValue
  abstract static class PartitionedSequencedMessage {
    abstract Partition partition();

    abstract SequencedMessage sequencedMessage();

    private static PartitionedSequencedMessage of(
        Partition partition, SequencedMessage sequencedMessage) {
      return new AutoValue_PubsubLiteUnboundedReader_PartitionedSequencedMessage(
          partition, sequencedMessage);
    }
  }

  @Override
  public boolean start() throws IOException {
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    try (CloseableMonitor.Hold h = monitor.enter()) {
      if (permanentError.isPresent()) {
        throw permanentError.get();
      }
      // messages starts empty. This will not remove messages on the first iteration.
      if (!messages.isEmpty()) {
        PartitionedSequencedMessage unusedMessage = messages.poll();
      }
      // Intentionally do this twice: We don't bound the buffer in this class, so we want to flush
      // the last pull from the subscribers before pulling new messages.
      if (!messages.isEmpty()) {
        setLastDelivered(messages.peek());
        return true;
      }
      pullFromSubscribers();
      if (!messages.isEmpty()) {
        setLastDelivered(messages.peek());
        return true;
      }
      return false;
    } catch (StatusException e) {
      throw new IOException(e);
    }
  }

  @GuardedBy("monitor.monitor")
  private void setLastDelivered(PartitionedSequencedMessage message) {
    SubscriberState state = subscriberMap.get(message.partition());
    state.lastDelivered =
        Optional.of(Offset.of(message.sequencedMessage().getCursor().getOffset()));
    Timestamp timestamp = message.sequencedMessage().getPublishTime();
    state.lastDeliveredPublishTimestamp = new Instant(Timestamps.toMillis(timestamp));
  }

  @GuardedBy("monitor.monitor")
  private void pullFromSubscribers() throws StatusException {
    for (Map.Entry<Partition, SubscriberState> entry : subscriberMap.entrySet()) {
      for (SequencedMessage message : entry.getValue().subscriber.pull()) {
        messages.add(PartitionedSequencedMessage.of(entry.getKey(), message));
      }
    }
  }

  @Override
  public SequencedMessage getCurrent() throws NoSuchElementException {
    try (CloseableMonitor.Hold h = monitor.enter()) {
      if (messages.isEmpty()) {
        throw new NoSuchElementException();
      }
      return messages.peek().sequencedMessage();
    }
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    try (CloseableMonitor.Hold h = monitor.enter()) {
      if (messages.isEmpty()) {
        throw new NoSuchElementException();
      }
      return new Instant(Timestamps.toMillis(messages.peek().sequencedMessage().getPublishTime()));
    }
  }

  @Override
  public void close() {
    try (CloseableMonitor.Hold h = monitor.enter()) {
      for (SubscriberState state : subscriberMap.values()) {
        try {
          state.subscriber.close();
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    }
    committerProxy.stopAsync().awaitTerminated();
  }

  @Override
  public Instant getWatermark() {
    try (CloseableMonitor.Hold h = monitor.enter()) {
      return subscriberMap.values().stream()
          .map(state -> state.lastDeliveredPublishTimestamp)
          .min(Instant::compareTo)
          .get();
    }
  }

  @Override
  public CheckpointMark getCheckpointMark() {
    try (CloseableMonitor.Hold h = monitor.enter()) {
      ImmutableMap.Builder<Partition, Offset> builder = ImmutableMap.builder();
      subscriberMap.forEach(
          (partition, subscriberState) ->
              subscriberState.lastDelivered.ifPresent(offset -> builder.put(partition, offset)));
      return new OffsetCheckpointMark(this, builder.build());
    }
  }

  @Override
  public UnboundedSource<SequencedMessage, ?> getCurrentSource() {
    return source;
  }
}
