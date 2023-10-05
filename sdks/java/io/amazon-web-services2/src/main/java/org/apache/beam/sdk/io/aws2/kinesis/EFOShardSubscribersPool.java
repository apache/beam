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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.beam.sdk.io.aws2.kinesis.TimeUtil.minTimestamp;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ForwardingIterator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

@SuppressWarnings({"nullness"})
class EFOShardSubscribersPool {
  private static final Logger LOG = LoggerFactory.getLogger(EFOShardSubscribersPool.class);
  private static final int ON_ERROR_COOL_DOWN_MS_DEFAULT = 1_000;
  private static final int DEFAULT_MAX_CAPACITY_PER_SHARD = 10;
  private final int onErrorCoolDownMs;

  /**
   * Identifier of the current subscribers pool.
   *
   * <p>Injected into other objects which belong to this pool to ease tracing with logs.
   */
  private final String poolId;

  private final KinesisIO.Read read;
  private final String consumerArn;
  private final KinesisAsyncClient kinesis;

  /**
   * Unbounded queue of events, but events in-flight are limited by the {@link EFOShardSubscriber}.
   */
  private final ConcurrentLinkedQueue<EventRecords> eventQueue = new ConcurrentLinkedQueue<>();

  private final int maxCapacityPerShard;

  /**
   * State map of currently active shards that can be checkpoint-ed.
   *
   * <p>This map may only be accessed and updated from within {@link #start}, {@link #getNextRecord}
   * and dependent {@link #onEventDone} to prevent race conditions.
   */
  private final Map<String, ShardState> state = new HashMap<>();

  /**
   * Async subscription error (as first seen), if set all subscribers must be cancelled and no new
   * ones started.
   *
   * <p>Must be volatile as it is accessed from various threads. But it's best effort, setting this
   * doesn't have to be atomic.
   */
  private volatile @MonotonicNonNull Throwable subscriptionError;

  /**
   * May only ever be altered from within {@link #stop()} or {@link #getNextRecord()} to prevent
   * race conditions when cancelling subscribers.
   */
  private boolean isStopped = false;

  /**
   * Async completion callback handling {@link EFOShardSubscriber#subscribe supscriptions} that
   * terminate exceptionally.
   *
   * <p>Unless already in error state, stores error as {@link #subscriptionError}. This pool will be
   * stopped when {@link #getNextRecord()} is called next, but allowing the {@link #eventQueue} to
   * be drained. Only once empty any {@link #subscriptionError} is propagated. This simplifies state
   * management and checkpointing a lot.
   */
  private final BiConsumer<Void, Throwable> errorHandler =
      (Void unused, Throwable error) -> {
        if (error != null && subscriptionError == null) {
          subscriptionError = error;
        }
      };

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  // EventRecords iterator that is currently consumed
  @Nullable EventRecords current = null;

  private final WatermarkPolicyFactory watermarkPolicyFactory;

  EFOShardSubscribersPool(KinesisIO.Read readSpec, String consumerArn, KinesisAsyncClient kinesis) {
    this(readSpec, consumerArn, kinesis, ON_ERROR_COOL_DOWN_MS_DEFAULT);
  }

  EFOShardSubscribersPool(
      KinesisIO.Read readSpec,
      String consumerArn,
      KinesisAsyncClient kinesis,
      int onErrorCoolDownMs) {
    this.poolId = generatePoolId();
    this.read = readSpec;
    this.consumerArn = consumerArn;
    this.kinesis = kinesis;
    this.watermarkPolicyFactory = read.getWatermarkPolicyFactory();
    this.onErrorCoolDownMs = onErrorCoolDownMs;
    this.maxCapacityPerShard =
        readSpec.getMaxCapacityPerShard() != null
            ? read.getMaxCapacityPerShard()
            : DEFAULT_MAX_CAPACITY_PER_SHARD;
  }

  /**
   * Starts a subscribers pool by starting a {@link EFOShardSubscriber#subscribe shard subscription}
   * for each {@link ShardCheckpoint} with the subscription {@link #errorHandler} callback.
   *
   * <p>{@link EFOShardSubscriber}s with their respective state are tracked in {@link #state}.
   */
  void start(Iterable<ShardCheckpoint> checkpoints) {
    LOG.info(
        "Pool {} - starting for stream {} consumer {}. Checkpoints = {}",
        poolId,
        read.getStreamName(),
        consumerArn,
        checkpoints);
    for (ShardCheckpoint shardCheckpoint : checkpoints) {
      checkState(
          !state.containsKey(shardCheckpoint.getShardId()),
          "Duplicate shard id %s",
          shardCheckpoint.getShardId());
      ShardState shardState =
          new ShardState(
              initShardSubscriber(shardCheckpoint), shardCheckpoint, watermarkPolicyFactory);
      state.put(shardCheckpoint.getShardId(), shardState);
    }
  }

  /**
   * Returns the next disaggregated {@link KinesisRecord} if available and updates {@link #state}
   * accordingly so that it reflects a mutable checkpoint AFTER returning that record.
   *
   * <p>Async subscription errors are delayed until {@link #eventQueue} is completely drained and
   * then rethrown here.
   *
   * <p>This repeats the following steps until a record or {@code null} was returned:
   *
   * <ol>
   *   <li>If {@link #current} is null and {@link #eventQueue} is empty, return {@code null} unless
   *       {@link #subscriptionError} is set: in that case rethrow.
   *   <li>Otherwise if {@link #current} is null, poll next from {@link #eventQueue}.
   *   <li>If {@link #current} has a next {@link KinesisClientRecord}, update {@link #state}
   *       accordingly and return the corresponding converted {@link KinesisRecord}, optionally
   *       triggering {@link #onEventDone} if that was the last record of {@link #current}.
   *   <li>Finally, if nothing was returned yet, trigger {@link #onEventDone} and continue loop.
   * </ol>
   *
   * <p>It polls the {@link #eventQueue} in a while loop to avoid returning null immediately if an
   * event without records arrived. There may be events with records after the {@link #current}, and
   * it is better to poll again instead of having {@link EFOKinesisReader#advance()} signalling
   * false to Beam. Otherwise, Beam would poll again later, which would introduce unnecessary delay.
   */
  @Nullable
  KinesisRecord getNextRecord() throws IOException {
    while (true) {
      if (!isStopped && subscriptionError != null) {
        // Stop the pool to cancel all subscribers and prevent new subscriptions.
        // Doing this as part of getNextRecord() avoids concurrent access to the state map and
        // prevents any related issues.
        stop();
      }

      if (current == null) {
        current = eventQueue.poll();
      }

      if (current != null) {
        String shardId = current.shardId;
        ShardState shardState = Preconditions.checkStateNotNull(state.get(shardId));
        if (current.hasNext()) {
          KinesisClientRecord r = current.next();
          // Make sure to update shard state accordingly if `current` does not contain any more
          // events. This is necessary to account for any re-sharding, so we could correctly resume
          // from a checkpoint if taken once we advanced to the record returned by getNextRecord().
          if (!current.hasNext()) {
            onEventDone(shardState, current);
            current = null;
          }
          KinesisRecord kinesisRecord = new KinesisRecord(r, read.getStreamName(), shardId);
          if (shardState.isAfterInitialCheckpoint(kinesisRecord)) {
            shardState.update(kinesisRecord);
            return kinesisRecord;
          }
        } else {
          onEventDone(shardState, current);
          current = null;
        }
      } else if (subscriptionError != null) {
        stop();
        throw new IOException(subscriptionError);
      } else {
        return null; // no record available, queue is empty
      }
    }
  }

  /**
   * Unsets {@link #current} and updates {@link #state} accordingly.
   *
   * <p>If {@link SubscribeToShardEvent#continuationSequenceNumber()} is defined, update {@link
   * ShardState} accordingly. Otherwise, or if {@link SubscribeToShardEvent#childShards()} exists,
   * handle re-sharding: remove old shard from {@link #state} and add new ones at TRIM_HORIZON.
   *
   * <p>In case of re-sharding, start all new {@link EFOShardSubscriber#subscribe subscriptions}
   * with the subscription {@link #errorHandler} if there is no {@link #subscriptionError} yet.
   */
  private void onEventDone(ShardState shardState, EventRecords eventContainer) {
    SubscribeToShardEvent event = eventContainer.event;
    if (event.continuationSequenceNumber() == null && event.hasChildShards()) {
      LOG.info("Pool {} - processing re-shard signal {}", poolId, event);
      List<String> successorShardsIds = computeSuccessorShardsIds(eventContainer);
      for (String successorShardId : successorShardsIds) {
        ShardCheckpoint newCheckpoint =
            new ShardCheckpoint(
                read.getStreamName(),
                successorShardId,
                new StartingPoint(InitialPositionInStream.TRIM_HORIZON));
        state.computeIfAbsent(
            successorShardId,
            id ->
                new ShardState(
                    initShardSubscriber(newCheckpoint), newCheckpoint, watermarkPolicyFactory));
      }

      state.remove(eventContainer.shardId);
    } else {
      shardState.update(eventContainer);
    }
  }

  /**
   * Always initialize a new subscriber to make sure checkpoints will be correct. But only start the
   * subscription if there is no {@link #subscriptionError}.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private EFOShardSubscriber initShardSubscriber(ShardCheckpoint cp) {
    EFOShardSubscriber subscriber =
        new EFOShardSubscriber(this, cp.getShardId(), consumerArn, kinesis, onErrorCoolDownMs);
    StartingPosition startingPosition = cp.toEFOStartingPosition();
    if (subscriptionError == null) {
      subscriber.subscribe(startingPosition).whenCompleteAsync(errorHandler);
    }
    return subscriber;
  }

  private List<String> computeSuccessorShardsIds(EventRecords eventContainer) {
    List<String> successorShardsIds = new ArrayList<>();
    SubscribeToShardEvent event = eventContainer.event;
    for (ChildShard childShard : event.childShards()) {
      if (childShard.parentShards().contains(eventContainer.shardId)) {
        if (childShard.parentShards().size() > 1) {
          // This is the case of merging two shards into one.
          // When there are 2 parent shards, we only pick it up if
          // its max shard equals to sender shard ID.
          String maxParentId = childShard.parentShards().stream().max(String::compareTo).get();
          if (eventContainer.shardId.equals(maxParentId)) {
            successorShardsIds.add(childShard.shardId());
          }
        } else {
          // This is the case when shard is split - we must add both
          // and start subscriptions for them.
          successorShardsIds.add(childShard.shardId());
        }
      }
    }

    if (successorShardsIds.isEmpty()) {
      LOG.info("Pool {} - found no successors for shard {}", poolId, eventContainer.shardId);
    } else {
      LOG.info(
          "Pool {} - found successors for shard {}: {}",
          poolId,
          eventContainer.shardId,
          successorShardsIds);
    }
    return successorShardsIds;
  }

  /** Adds a {@link EventRecords} iterator for shardId and event to {@link #eventQueue}. */
  void enqueueEvent(String shardId, SubscribeToShardEvent event) {
    eventQueue.offer(new EventRecords(shardId, event));
  }

  Instant getWatermark() {
    return minTimestamp(state.values().stream().map(ShardState::getWatermark));
  }

  /** This is assumed to be never called before {@link #start} is called. */
  KinesisReaderCheckpoint getCheckpointMark() {
    List<ShardCheckpoint> checkpoints = new ArrayList<>(state.size());
    for (ShardState shardState : state.values()) {
      checkpoints.add(shardState.toCheckpoint());
    }

    return new KinesisReaderCheckpoint(checkpoints);
  }

  void stop() {
    LOG.info("Pool {} - stopping", poolId);
    isStopped = true;
    state.forEach((shardId, st) -> st.subscriber.cancel());
    scheduler.shutdownNow(); // immediately discard all scheduled tasks
  }

  /**
   * Mutable class tracking state and progress per shard.
   *
   * <p>When {@link #getCheckpointMark()} is called, {@link ShardCheckpoint} instances are created
   * from these objects, and 3 cases are possible:
   *
   * <ul>
   *   <li>Pool is just created, and a shard never gave out any record - {@link ShardCheckpoint}
   *       falls back to {@link ShardState#initCheckpoint}
   *   <li>Pool was running and got re-shard events - same as above
   *   <li>Pool was running, and gave out events - use {@link ShardState#sequenceNumber} and {@link
   *       ShardState#subSequenceNumber}
   * </ul>
   */
  private static class ShardState {
    final EFOShardSubscriber subscriber;
    final ShardCheckpoint initCheckpoint;
    final WatermarkPolicy watermarkPolicy;

    @Nullable String sequenceNumber = null;
    long subSequenceNumber = 0L;

    ShardState(
        EFOShardSubscriber subscriber,
        ShardCheckpoint initCheckpoint,
        WatermarkPolicyFactory watermarkPolicyFactory) {
      this.subscriber = subscriber;
      this.initCheckpoint = initCheckpoint;
      this.watermarkPolicy = watermarkPolicyFactory.createWatermarkPolicy();
    }

    void update(KinesisRecord r) {
      sequenceNumber = checkNotNull(r.getSequenceNumber());
      subSequenceNumber = r.getSubSequenceNumber();
      watermarkPolicy.update(r);
    }

    /**
     * To be used for end-of-record handling / heartbeat records.
     *
     * <p>{@link #subSequenceNumber} can not be re-set to 0 here, cause otherwise
     * end-of-aggregated-record would erase progress in consuming aggregated records.
     *
     * @param eventRecords
     */
    void update(EventRecords eventRecords) {
      sequenceNumber = checkNotNull(eventRecords.event.continuationSequenceNumber());
      subscriber.ackEvent();
    }

    /**
     * Follows semantics of {@link ShardCheckpoint#moveAfter(KinesisRecord)}, e.g. it will always
     * persist {@link ShardIteratorType#AFTER_SEQUENCE_NUMBER} as soon as some record gets its
     * {@link #sequenceNumber} registered.
     */
    ShardCheckpoint toCheckpoint() {
      if (sequenceNumber != null) {
        return new ShardCheckpoint(
            initCheckpoint.getStreamName(),
            initCheckpoint.getShardId(),
            ShardIteratorType.AFTER_SEQUENCE_NUMBER,
            sequenceNumber,
            subSequenceNumber);
      } else {
        // sequenceNumber was never updated for this shard,
        // fall back to its init checkpoint
        return initCheckpoint;
      }
    }

    Instant getWatermark() {
      return watermarkPolicy.getWatermark();
    }

    /**
     * Compares record with initialisation checkpoint.
     *
     * <p>This is necessary when consuming aggregated records from Kinesis: only a part of records
     * from an aggregate might have been checkpoint-ed before, but the other part still must be
     * loaded. This check filters out the part which was already consumed, since it's not possible
     * to subscribe with a given {@link #subSequenceNumber}
     */
    boolean isAfterInitialCheckpoint(KinesisRecord r) {
      return initCheckpoint.isBeforeOrAt(r);
    }
  }

  /**
   * Lazy iterator over deaggregated {@link KinesisClientRecord}s of {@link #event}.
   *
   * <p>Event {@link Record}s are lazily deaggregated using {@link AggregatorUtil} when {@link
   * ForwardingIterator#delegate()} is first called.
   */
  private static class EventRecords extends ForwardingIterator<KinesisClientRecord> {
    private static final AggregatorUtil AGG_UTIL = new AggregatorUtil();
    String shardId;
    SubscribeToShardEvent event;
    @MonotonicNonNull Iterator<KinesisClientRecord> delegate = null;

    EventRecords(String shardId, SubscribeToShardEvent event) {
      this.shardId = shardId;
      this.event = event;
    }

    @Override
    protected Iterator<KinesisClientRecord> delegate() {
      if (delegate == null) {
        if (event.hasRecords() && !event.records().isEmpty()) {
          delegate =
              AGG_UTIL
                  .deaggregate(Lists.transform(event.records(), KinesisClientRecord::fromRecord))
                  .iterator();
        } else {
          delegate = Collections.emptyIterator();
        }
      }
      return delegate;
    }
  }

  String getPoolId() {
    return poolId;
  }

  int getMaxCapacityPerShard() {
    return maxCapacityPerShard;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  <T> CompletableFuture<T> delayedTask(Supplier<CompletableFuture<T>> task, long delayMs) {
    if (delayMs <= 0) {
      return task.get();
    }
    final CompletableFuture<T> cf = new CompletableFuture<>();
    try {
      scheduler.schedule(
          () ->
              task.get().handle((t, e) -> e == null ? cf.complete(t) : cf.completeExceptionally(e)),
          delayMs,
          MILLISECONDS);
    } catch (RejectedExecutionException e) {
      cf.completeExceptionally(e);
    }
    return cf;
  }

  private static String generatePoolId() {
    return RandomStringUtils.randomAlphanumeric(8).toLowerCase();
  }
}
