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

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.ProxyService;
import com.google.cloud.pubsublite.internal.wire.Subscriber;
import com.google.cloud.pubsublite.proto.FlowControlRequest;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.sdk.io.gcp.pubsublite.internal.MemoryLimiter.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MemoryBufferedSubscriberImpl extends ProxyService implements MemoryBufferedSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryBufferedSubscriberImpl.class);

  private final Partition partition;
  private final MemoryLimiter limiter;
  private final Subscriber subscriber;
  private long targetMemory;
  private Offset fetchOffset;
  private Block memBlock;

  private long bytesOutstandingToServer = 0;
  private long bytesOutstanding = 0;
  private final Queue<SequencedMessage> messages = new ArrayDeque<>();
  private boolean shutdown = false;

  // onReceive will not be called inline as subscriber is not started.
  // addServices is intended to be called from the constructor.
  @SuppressWarnings({"methodref.receiver.bound", "method.invocation"})
  public MemoryBufferedSubscriberImpl(
      Partition partition,
      Offset startOffset,
      MemoryLimiter limiter,
      Function<Consumer<List<SequencedMessage>>, Subscriber> subscriberFactory) {
    this.partition = partition;
    this.fetchOffset = startOffset;
    this.limiter = limiter;
    this.targetMemory = limiter.maxBlockSize();
    this.subscriber = subscriberFactory.apply(this::onReceive);
    addServices(this.subscriber);
    memBlock = limiter.claim(targetMemory);
  }

  @Override
  protected synchronized void start() throws CheckedApiException {
    bytesOutstandingToServer += memBlock.claimed();
    bytesOutstanding += memBlock.claimed();
    subscriber.allowFlow(
        FlowControlRequest.newBuilder()
            .setAllowedBytes(memBlock.claimed())
            .setAllowedMessages(Long.MAX_VALUE)
            .build());
  }

  @Override
  protected synchronized void stop() {
    if (shutdown) {
      return;
    }
    shutdown = true;
    memBlock.close();
  }

  @Override
  protected synchronized void handlePermanentError(CheckedApiException e) {
    stop();
  }

  private synchronized void onReceive(List<SequencedMessage> batch) {
    if (shutdown) {
      return;
    }
    for (SequencedMessage message : batch) {
      bytesOutstandingToServer -= message.getSizeBytes();
    }
    messages.addAll(batch);
  }

  @Override
  public synchronized Offset fetchOffset() {
    return fetchOffset;
  }

  @Override
  public synchronized void rebuffer() throws ApiException {
    if (shutdown) {
      return;
    }
    if (bytesOutstandingToServer < (targetMemory / 3)) {
      // Server is delivering lots of data, increase the target so that it is not throttled.
      targetMemory = Math.min(limiter.maxBlockSize(), targetMemory * 2);
    } else if (bytesOutstandingToServer > (2 * targetMemory / 3)) {
      // Server is delivering little data, decrease the target so that memory can be used for other
      // users of the limiter.
      targetMemory = Math.max(limiter.minBlockSize(), targetMemory / 2);
    }
    long claimTarget = Math.max(bytesOutstanding, targetMemory);
    memBlock.close();
    memBlock = limiter.claim(claimTarget);
    long toAllow = Math.max(memBlock.claimed() - bytesOutstanding, 0);
    if (toAllow > 0) {
      bytesOutstanding += toAllow;
      bytesOutstandingToServer += toAllow;
      try {
        subscriber.allowFlow(FlowControlRequest.newBuilder().setAllowedBytes(toAllow).build());
      } catch (CheckedApiException e) {
        throw e.underlying;
      }
    } else {
      LOG.debug(
          "Not claiming memory: partition {} outstanding {} to server {} target {} claimed {} messages {}",
          partition,
          bytesOutstanding,
          bytesOutstandingToServer,
          targetMemory,
          memBlock.claimed(),
          messages.size());
    }
  }

  @Override
  public synchronized Optional<SequencedMessage> peek() {
    return Optional.ofNullable(messages.peek());
  }

  @Override
  public synchronized void pop() {
    SequencedMessage message = messages.remove();
    bytesOutstanding -= message.getSizeBytes();
    fetchOffset = Offset.of(message.getCursor().getOffset() + 1);
  }
}
