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

import static org.apache.beam.sdk.io.gcp.pubsublite.internal.ApiServices.asCloseable;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.core.ApiService.State;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

public class UnboundedReaderImpl extends UnboundedReader<SequencedMessage> {
  private final UnboundedSource<SequencedMessage, CheckpointMarkImpl> source;
  private final MemoryBufferedSubscriber subscriber;
  private final TopicBacklogReader backlogReader;
  private final Supplier<BlockingCommitter> committer;

  private Offset fetchOffset;
  private Optional<Instant> lastMessageTimestamp = Optional.empty();
  private boolean advanced = false;

  UnboundedReaderImpl(
      UnboundedSource<SequencedMessage, CheckpointMarkImpl> source,
      MemoryBufferedSubscriber subscriber,
      TopicBacklogReader backlogReader,
      Supplier<BlockingCommitter> committer,
      Offset initialOffset) {
    checkArgument(initialOffset.equals(subscriber.fetchOffset()));
    this.source = source;
    this.subscriber = subscriber;
    this.backlogReader = backlogReader;
    this.committer = committer;
    this.fetchOffset = initialOffset;
  }

  @Override
  public SequencedMessage getCurrent() throws NoSuchElementException {
    if (!advanced) {
      throw new NoSuchElementException();
    }
    return subscriber.peek().get();
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    return getTimestamp(getCurrent());
  }

  private static Instant getTimestamp(SequencedMessage message) {
    return Instant.ofEpochMilli(Timestamps.toMillis(message.getPublishTime()));
  }

  @Override
  public void close() throws IOException {
    try (AutoCloseable c1 = backlogReader;
        AutoCloseable c3 = asCloseable(subscriber)) {
    } catch (Exception e) {
      throw new IOException("Failed when closing reader.", e);
    }
  }

  @Override
  public boolean start() throws IOException {
    try {
      subscriber.startAsync().awaitRunning(1, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    if (!subscriber.state().equals(State.RUNNING)) {
      Throwable t = subscriber.failureCause();
      if ("DUPLICATE_SUBSCRIBER_CONNECTIONS"
          .equals(ExtractStatus.getErrorInfoReason(ExtractStatus.toCanonical(t)))) {
        throw new IOException(
            "Partition reassigned to a different worker- this is expected and can be ignored.", t);
      }
      throw new IOException("Subscriber failed when trying to advance.", t);
    }
    if (advanced) {
      subscriber.pop();
    }
    Optional<SequencedMessage> next = subscriber.peek();
    advanced = next.isPresent();
    if (!advanced) {
      return false;
    }
    Offset nextOffset = Offset.of(next.get().getCursor().getOffset() + 1);
    checkState(nextOffset.value() > fetchOffset.value());
    fetchOffset = nextOffset;
    lastMessageTimestamp = Optional.of(getTimestamp(next.get()));
    return true;
  }

  @Override
  public Instant getWatermark() {
    return lastMessageTimestamp.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
  }

  @Override
  public CheckpointMarkImpl getCheckpointMark() {
    // By checkpointing, the runtime indicates it has finished processing all data it has already
    // pulled. This means we can ask Pub/Sub Lite to refill our in-memory buffer without causing
    // unbounded memory usage.
    subscriber.rebuffer();
    return new CheckpointMarkImpl(fetchOffset, committer);
  }

  @Override
  public UnboundedSource<SequencedMessage, CheckpointMarkImpl> getCurrentSource() {
    return source;
  }

  @Override
  public long getSplitBacklogBytes() {
    return backlogReader.computeMessageStats(fetchOffset).getMessageBytes();
  }
}
