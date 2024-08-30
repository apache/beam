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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.sdk.io.aws2.kinesis.EFOShardSubscriber.State.INITIALIZED;
import static org.apache.beam.sdk.io.aws2.kinesis.EFOShardSubscriber.State.PAUSED;
import static org.apache.beam.sdk.io.aws2.kinesis.EFOShardSubscriber.State.RUNNING;
import static org.apache.beam.sdk.io.aws2.kinesis.EFOShardSubscriber.State.STOPPED;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import io.netty.channel.ChannelException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

class EFOShardSubscriber {
  enum State {
    INITIALIZED, // Initialized, but not started yet
    RUNNING, // Subscriber started
    PAUSED, // Subscriber paused due to backpressure
    STOPPED // Subscriber stopped
  }

  private static final Logger LOG = LoggerFactory.getLogger(EFOShardSubscriber.class);

  private final EFOShardSubscribersPool pool;
  private final String consumerArn;

  // Shard id of this subscriber
  private final String shardId;

  private final KinesisAsyncClient kinesis;

  /** Internal subscriber state. */
  private volatile State state = INITIALIZED;

  /**
   * Kept only for cases when a subscription starts and then fails with a non-critical error, before
   * any event updates {@link ShardEventsSubscriber#sequenceNumber}.
   */
  private @MonotonicNonNull StartingPosition initialPosition;

  /**
   * Completes once this shard subscriber is done, either normally (stopped or shard is completely
   * consumed) or exceptionally due to a non retry-able error.
   */
  private final CompletableFuture<Void> done = new CompletableFuture<>();

  private final ShardEventsSubscriber eventsSubscriber = new ShardEventsSubscriber();

  /** Tracks number of delivered events in flight (until ack-ed). */
  private final AtomicInteger inFlight = new AtomicInteger();

  /**
   * Async completion handler for {@link KinesisAsyncClient#subscribeToShard} that:
   * <li>exits immediately if {@link #done} is already completed (exceptionally),
   * <li>re-subscribes at {@link ShardEventsSubscriber#sequenceNumber} for retry-able errors such as
   *     retry-able {@link SdkException}, {@link ClosedChannelException}, {@link ChannelException},
   *     {@link TimeoutException} (any of these might be wrapped in {@link CompletionException}s)
   * <li>or completes {@link #done} exceptionally for any other error,
   * <li>completes {@link #done} normally if subscriber {@link #state} is {@link State#STOPPED} or
   *     if shard completed (no further {@link ShardEventsSubscriber#sequenceNumber}),
   * <li>or otherwise re-subscribes at {@link ShardEventsSubscriber#sequenceNumber}.
   */
  private final BiConsumer<Void, Throwable> reSubscriptionHandler;

  private static boolean isRetryable(Throwable error) {
    Throwable cause = unwrapCompletionException(error);
    if (cause instanceof SdkException && ((SdkException) cause).retryable()) {
      return true; // retryable SDK exception
    }
    // check the root cause for issues that can be addressed using retries
    cause = Throwables.getRootCause(cause);
    return cause instanceof ClosedChannelException // Java Nio
        || cause instanceof TimeoutException // Java
        || cause instanceof ChannelException; // Netty (e.g. ReadTimeoutException)
  }

  /** Loops through completion exceptions until we get the underlying cause. */
  private static Throwable unwrapCompletionException(Throwable completionException) {
    Throwable current = completionException;
    while (current instanceof CompletionException) {
      Throwable cause = current.getCause();
      if (cause != null) {
        current = cause;
      } else {
        return current;
      }
    }
    return current;
  }

  /** Not fully init-ed until {@link #subscribe(StartingPosition)} is called. */
  @SuppressWarnings({"FutureReturnValueIgnored", "method.invocation", "argument"})
  EFOShardSubscriber(
      EFOShardSubscribersPool pool,
      String shardId,
      String consumerArn,
      KinesisAsyncClient kinesis,
      int onErrorCoolDownMs) {
    this.pool = pool;
    this.consumerArn = consumerArn;
    this.shardId = shardId;
    this.kinesis = kinesis;
    this.reSubscriptionHandler =
        (Void unused, Throwable error) -> {
          eventsSubscriber.cancel();

          if (error != null && !isRetryable(error)) {
            done.completeExceptionally(error);
            return;
          }

          if (error != null && isRetryable(error) && state != STOPPED) {
            String lastContinuationSequenceNumber = eventsSubscriber.sequenceNumber;
            if (inFlight.get() == pool.getMaxCapacityPerShard()) {
              state = PAUSED;
            } else {
              if (lastContinuationSequenceNumber != null) {
                // retry-able error occurs after at least one SubscribeToShardEvent is received:
                pool.delayedTask(
                    () -> internalReSubscribe(lastContinuationSequenceNumber), onErrorCoolDownMs);
              } else {
                // retry-able error occurs before any SubscribeToShardEvent is received:
                pool.delayedTask(() -> internalSubscribe(initialPosition), onErrorCoolDownMs);
              }
            }
            return;
          }

          String lastContinuationSequenceNumber = eventsSubscriber.sequenceNumber;

          // happy-path re-subscribe, subscription was complete by the SDK after 5 min
          if (error == null && state != STOPPED && lastContinuationSequenceNumber != null) {
            internalReSubscribe(lastContinuationSequenceNumber);
            return;
          }

          // shard is fully consumed - re-shard happened
          if (error == null && state != STOPPED && lastContinuationSequenceNumber == null) {
            done.complete(null);
            return;
          }

          String msg =
              String.format(
                  "Pool %s - unknown case which is likely a bug: state=%s seqnum=%s",
                  pool.getPoolId(), state, lastContinuationSequenceNumber);
          LOG.warn(msg);
          done.completeExceptionally(new IllegalStateException(msg));
        };
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
   *     completely consumed) or exceptionally due to a non retry-able error.
   */
  CompletableFuture<Void> subscribe(StartingPosition position) {
    checkState(state == INITIALIZED, "Subscriber was already started");
    initialPosition = position;
    return internalSubscribe(position);
  }

  private CompletableFuture<Void> internalReSubscribe(String sequenceNumber) {
    return internalSubscribe(
        StartingPosition.builder()
            .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
            .sequenceNumber(sequenceNumber)
            .build());
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private CompletableFuture<Void> internalSubscribe(StartingPosition position) {
    SubscribeToShardRequest request = subscribeRequest(position);
    LOG.info(
        "Pool {} - shard {} starting subscribe request {}", pool.getPoolId(), shardId, request);
    try {
      kinesis.subscribeToShard(request, responseHandler()).whenComplete(reSubscriptionHandler);
      return done;
    } catch (Exception e) {
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
   * Cancels shard subscriber. Sets {@link #state} to {@link State#STOPPED} and invokes {@link
   * ShardEventsSubscriber#cancel()} if defined.
   */
  void cancel() {
    LOG.info("Pool {} - shard {} cancelling", pool.getPoolId(), shardId);
    if (state != STOPPED && eventsSubscriber != null) {
      eventsSubscriber.cancel();
      state = STOPPED;
    }
  }

  /**
   * Decrements events {@link #inFlight} and, if previously at the limit, requests a next event from
   * {@link ShardEventsSubscriber#subscription} (if active).
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  void ackEvent() {
    int prevInFlight = inFlight.getAndDecrement();
    if (state == PAUSED) {
      state = RUNNING;
      internalReSubscribe(checkStateNotNull(eventsSubscriber.sequenceNumber));
    } else if (prevInFlight == pool.getMaxCapacityPerShard()) {
      Subscription s = eventsSubscriber.subscription;
      if (s != null) {
        s.request(1);
      }
    }
  }

  /** Subscriber to the Kinesis event stream. */
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
      subscription = null;
    }

    /**
     * Handles new established {@link Subscription}.
     *
     * <p>Cancels subscription immediately if {@link EFOShardSubscriber#state} is {@link
     * State#STOPPED} already. Otherwise, if below the {@link #inFlight} limit, the first event is
     * requested.
     */
    @Override
    public void onSubscribe(Subscription subscription) {
      this.subscription = subscription;
      if (state == STOPPED) {
        cancel();
      } else if (inFlight.get() < pool.getMaxCapacityPerShard()) {
        subscription.request(1);
      }
    }

    /**
     * Handles {@link SubscribeToShardEvent} and forwards it to {@link
     * EFOShardSubscribersPool#enqueueEvent}.
     *
     * <p>This increments {@link #inFlight} and immediately {@link Subscription#request requests}
     * another event if below the {@link #inFlight} limit. Otherwise this is done on the next {@link
     * #ackEvent()}.
     */
    @Override
    public void visit(SubscribeToShardEvent event) {
      pool.enqueueEvent(shardId, event);
      sequenceNumber = event.continuationSequenceNumber();
      int capacity = pool.getMaxCapacityPerShard() - inFlight.incrementAndGet();
      checkState(capacity >= 0, "Exceeded in-flight limit");
      if (capacity > 0 && subscription != null) {
        subscription.request(1);
      }
    }

    /** Delegates to {@link #visit}. */
    @Override
    public void onNext(SubscribeToShardEventStream event) {
      event.accept(this);
    }

    /** Nothing to do here, handled in {@link #reSubscriptionHandler}. */
    @Override
    public void onError(Throwable t) {
      LOG.warn("Pool {} - shard {} subscriber got error", pool.getPoolId(), shardId, t);
    }

    /** Unsets {@link #eventsSubscriber} of {@link EFOShardSubscriber}. */
    @Override
    public void onComplete() {
      subscription = null;
    }
  }
}
