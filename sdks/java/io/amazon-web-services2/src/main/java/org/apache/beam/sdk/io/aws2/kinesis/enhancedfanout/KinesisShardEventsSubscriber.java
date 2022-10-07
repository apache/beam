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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout;

import java.util.concurrent.CountDownLatch;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

class KinesisShardEventsSubscriber implements Subscriber<SubscribeToShardEventStream> {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisShardEventsSubscriber.class);

  private final ShardSubscribersPoolImpl pool;
  private final CountDownLatch isRunningLatch;
  private final String streamName;
  private final String consumerArn;
  private final String shardId;

  private @Nullable Subscription s;
  private volatile boolean decommissioned = false;
  private volatile boolean cancelled = false;

  KinesisShardEventsSubscriber(
      ShardSubscribersPoolImpl pool,
      CountDownLatch isRunningLatch,
      String streamName,
      String consumerArn,
      String shardId) {
    this.pool = pool;
    this.isRunningLatch = isRunningLatch;
    this.streamName = streamName;
    this.consumerArn = consumerArn;
    this.shardId = shardId;
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    s = subscription;
    isRunningLatch.countDown();
  }

  /** AWS SDK Netty thread calls this at least every ~ 5 seconds even when no new records arrive. */
  @Override
  public void onNext(SubscribeToShardEventStream subscribeToShardEventStream) {
    subscribeToShardEventStream.accept(
        new SubscribeToShardResponseHandler.Visitor() {
          @Override
          public void visit(SubscribeToShardEvent event) {
            if (!ReShardEvent.isReShard(event)) {
              pushEvent(RecordsShardEvent.fromNext(streamName, shardId, event));
            } else {
              pushEvent(ReShardEvent.fromNext(shardId, event));
            }
          }
        });
  }

  @Override
  public void onError(Throwable throwable) {
    LOG.warn("Pool id = {} shard id = {} got error", pool.getPoolId(), shardId, throwable);
    pushEvent(ErrorShardEvent.fromErr(shardId, throwable));
  }

  /**
   * AWS SDK Netty thread calls this every ~ 5 minutes, these events alone are not enough signal to
   * conclude the shard has no more records to consume.
   *
   * <p>Not that it is also called after a re-shard event handled by {@link
   * #onNext(SubscribeToShardEventStream)}.
   */
  @Override
  public void onComplete() {
    LOG.info(
        "Pool id = {} stream = {} consumer = {} shard = {}. Subscription complete",
        pool.getPoolId(),
        streamName,
        shardId,
        consumerArn);
    pushEvent(SubscriptionCompleteShardEvent.create(shardId));
  }

  void requestRecords(long n) {
    if (!cancelled && s != null) {
      s.request(n);
    }
  }

  void cancel() {
    LOG.warn(
        "Pool id = {} stream = {} consumer = {} shard = {}. Subscription canceled",
        pool.getPoolId(),
        streamName,
        consumerArn,
        shardId);

    if (cancelled) {
      return;
    }
    cancelled = true;

    if (s != null) {
      s.cancel();
    }
  }

  private void pushEvent(ShardEvent event) {
    if (cancelled) {
      return;
    }

    try {
      if (!decommissioned) {
        pool.handleEvent(event);
      }
      if (event.getType().equals(ShardEventType.RE_SHARD)) {
        decommissioned = true;
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted while trying to enqueue event");
    }
  }
}
