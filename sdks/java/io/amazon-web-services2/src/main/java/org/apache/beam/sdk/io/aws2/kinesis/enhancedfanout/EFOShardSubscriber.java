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

import io.netty.channel.ChannelException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

class EFOShardSubscriber {
  // TODO: get rid of these 2?
  private final EFOShardSubscribersPool pool;

  // Shard id of this subscriber
  private final String consumerArn;
  private final String shardId;
  // Read configuration
  private final AsyncClientProxy kinesis;

  /**
   * Completes once this shard subscriber is done, either normally (stopped or shard is completely
   * consumed) or exceptionally due to a non retryable error.
   */
  private final CompletableFuture<Void> done = new CompletableFuture<>();

  /**
   * Async completion handler for {@link AsyncClientProxy#subscribeToShard} that:
   * <li>exists immediately if {@link #done} is already completed (exceptionally),
   * <li>re-subscribes at {@link ShardEventsSubscriber#sequenceNumber} for retryable errors such as
   *     retryable {@link SdkException}, {@link ClosedChannelException}, {@link ChannelException},
   *     {@link TimeoutException} (any of these might be wrapped in {@link CompletionException}s)
   * <li>or completes {@link #done} exceptionally for any other error,
   * <li>completes {@done} normally if subscriber {@link #isStopped} or if shard completed (no
   *     further {@link ShardEventsSubscriber#sequenceNumber}),
   * <li>or otherwise re-subscribes at {@link ShardEventsSubscriber#sequenceNumber}.
   */
  @SuppressWarnings({"FutureReturnValueIgnored", "all"})
  BiConsumer<Void, Throwable> reSubscriptionHandler =
      (Void unused, Throwable error) -> {
        if (error != null) {
          // FIXME potentially resubscribe
          done.completeExceptionally(error);
        } else {
          String lastContinuationSequenceNumber = "0";
          if (lastContinuationSequenceNumber == null) {
            done.complete(null); // completely consumed this shard, done
          } else {
            subscribe(
                StartingPosition.builder()
                    .type(ShardIteratorType.AT_SEQUENCE_NUMBER)
                    .sequenceNumber(lastContinuationSequenceNumber)
                    .build());
          }
        }
      };

  /** Tracks number of delivered events in flight (until acked). */
  AtomicInteger inFlight;

  private static final Integer IN_FLIGHT_LIMIT = 3;

  /** Signals if subscriber is stopped from externally. */
  volatile boolean isStopped = false;

  volatile EFOShardSubscriber.ShardEventsSubscriber eventsSubscriber;

  public EFOShardSubscriber(
      EFOShardSubscribersPool pool,
      String shardId,
      KinesisIO.Read read,
      AsyncClientProxy kinesis) {
    this.pool = pool;
    this.consumerArn = checkArgumentNotNull(read.getConsumerArn());
    this.shardId = shardId;
    this.kinesis = kinesis;
    this.inFlight = new AtomicInteger();
    this.eventsSubscriber = new ShardEventsSubscriber();
  }

  /**
   * Subscribes to shard {@link #shardId} at starting position and automatically re-subscribes when
   * necessary using {@link #reSubscriptionHandler}.
   *
   * <p>Note:
   * <li>{@link #subscribe} may only ever be invoked once by an external caller.
   * <li>The re-subscription is hidden from the external caller. To the outside it looks this
   *     subscriber is always subscribed to the shard once {@link #subscribe} was called.
   *
   * @return {@link #done} to signal completion of this subscriber, normally (stopped or shard is
   *     completely consumed) or exceptionally due to a non retryable error.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  CompletableFuture<Void> subscribe(StartingPosition position) {
    try {
      kinesis
          .subscribeToShard(subscribeRequest(position), responseHandler())
          .whenCompleteAsync(reSubscriptionHandler);
      return done;
    } catch (RuntimeException e) {
      done.completeExceptionally(e);
      return done;
    }
  }

  private SubscribeToShardRequest subscribeRequest(StartingPosition position) {
    return SubscribeToShardRequest.builder()
        .consumerARN(consumerArn)
        .shardId(shardId)
        .startingPosition(position)
        .build();
  }

  private SubscribeToShardResponseHandler responseHandler() {
    return SubscribeToShardResponseHandler.builder().subscriber(() -> eventsSubscriber).build();
  }

  /**
   * Cancels shard subscriber. Sets {@link #isStopped} and invokes {@link
   * ShardEventsSubscriber#cancel()} if defined.
   */
  void cancel() {
    if (!isStopped && eventsSubscriber != null) {
      eventsSubscriber.cancel();
      isStopped = true;
    }
  }

  /**
   * Decrements events {@link #inFlight} and, if previously at the limit, requests a next event from
   * {@link ShardEventsSubscriber#subscription} (if active).
   */
  void ackEvent() {
    if (inFlight.decrementAndGet() < IN_FLIGHT_LIMIT) {
      Subscription s = eventsSubscriber.subscription;
      if (s != null) {
        s.request(1);
      }
    }
  }

  /** Subscriber to the Kinesis event stream */
  private class ShardEventsSubscriber
      implements Subscriber<SubscribeToShardEventStream>, SubscribeToShardResponseHandler.Visitor {
    /** Tracks continuation sequence number. */
    @Nullable String sequenceNumber;
    /** Current active subscription to request more events or cancel it. */
    @Nullable Subscription subscription;

    /** Cancels {@link #subscription}. */
    void cancel() {
      if (subscription != null) {
        subscription.cancel();
      }
    }

    /**
     * Handles new established {@link Subscription}.
     *
     * <p>Cancels subscription immediately if {@link EFOShardSubscriber#isStopped} already.
     * Otherwise, if below the {@link #inFlight} limit, the first event is requested.
     */
    @Override
    public void onSubscribe(Subscription subscription) {
      this.subscription = subscription;
      this.subscription.request(1);
    }

    /**
     * Handles {@link SubscribeToShardEvent} and forwards it to {@link
     * EFOShardSubscribersPool#enqueueEvent}.
     *
     * <p>This increments {@link #inFlight} and immediately {@link Subscription#request requests}
     * another event if below the {@link #inFlight} limit. Otherwise this is done on the next {@link
     * #ackEvent()}.
     *
     * <p>This continuously updates {@link #sequenceNumber}. In case of any exception, {@link
     * #subscription} is cancelled and {@link #done} is completed exceptionally.
     */
    @Override
    public void visit(SubscribeToShardEvent event) {
      pool.enqueueEvent(shardId, event);
      sequenceNumber = event.continuationSequenceNumber();
      if (inFlight.incrementAndGet() < IN_FLIGHT_LIMIT && subscription != null) {
        subscription.request(1);
      }
    }

    /** Delegates to {@link #visit} */
    @Override
    public void onNext(SubscribeToShardEventStream event) {
      event.accept(this);
    }

    @Override
    public void onError(Throwable t) {
      // nothing to do here, handled in resubscriptionHandler
    }

    /** Unsets {@link #eventsSubscriber} of {@link EFOShardSubscriber}. */
    @Override
    public void onComplete() {
      subscription = null;
    }
  }
}
