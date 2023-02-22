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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisReaderCheckpoint;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.ShardCheckpoint;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicy;
import org.apache.beam.sdk.io.aws2.kinesis.WatermarkPolicyFactory;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ForwardingIterator;
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

/**
 * TODO: - switch to existing ShardCheckpoint - we don't care what consumer name was, we want to
 * switch back and forth from / to EFO - back-fill - EFO consumer, then use normal consumer to keep
 * going - test with real Kinesis - check whenComplete (netty) vs whenCompleteAsync (fork join pool)
 * - Work on testing & stub - think of delay of re-connect (in the pool -
 * oneThreadScheduledExecService) - Re-sharding: state is always reflecting the ack-ed checkpoint,
 * so getNextRecord() executes actual state mutations. The caller of that thing will handle starting
 * new subscriptions and deleting the orphaned shards' checkpoints from the map. - Inner classes are
 * fine: - You limit public surface, clear isolation - Everything in the package level - hard to
 * navigate, too many classes - Inner class can be used only in the context of enclosing class, it
 * is more clear - Readers don't need to worry about all the places class can be re-used -
 * KinesisIO.Read / Write - Javadoc is easier to navigate - Sometimes classes with inner classes
 * become too huge - Static helper classes (Util) used in a single place - also good candidates for
 * inner classes - If you really re-use over and over - better not to - DirectRunner interrupts and
 * re-creates new sources too often. Use FlinkRunner - Config class - potential alternative - Client
 * configuration - option: not to have back-offs in custom code, client itself has back-offs
 * internally - we can think of this later, add any custom back offs as late as possible - ?
 * recommend in javadoc to use large initial back offs ?
 */
@SuppressWarnings({"nullness"})
class EFOShardSubscribersPool {
  private static final Logger LOG = LoggerFactory.getLogger(EFOShardSubscribersPool.class);
  private static final long SCHEDULER_SHUTDOWN_TIMEOUT_MS = 1_000L;
  private static final long ON_ERROR_COOL_DOWN_MS_DEFAULT = 1_000L;
  private final long onErrorCoolDownMs;

  private final UUID poolId;
  private final KinesisIO.Read read;
  private final KinesisAsyncClient kinesis;

  /**
   * Unbounded queue of events, but events in-flight are limited by the {@link EFOShardSubscriber}.
   */
  private final ConcurrentLinkedQueue<EventRecords> eventQueue = new ConcurrentLinkedQueue<>();

  /**
   * State map of currently active shards that can be checkpointed.
   *
   * <p>This map may only be updated from within {@link #getNextRecord()} (and dependent {@link
   * #onEventDone}).
   */
  private final Map<String, ShardState> state = new ConcurrentHashMap<>();

  /**
   * Async subscription error (as first seen), if set all subscribers must be cancelled and no new
   * ones started.
   *
   * <p>Must be volatile as it is accessed from various threads. But it's best effort, setting this
   * doesn't have to be atomic.
   */
  volatile @MonotonicNonNull Throwable subscriptionError;

  /**
   * Async completion callback handling {@link EFOShardSubscriber#subscribe supscriptions} that
   * terminate exceptionally.
   *
   * <p>Unless already in error state, stores error as {@link #subscriptionError} and cancels all
   * subscribers in {@link #state} to drain the {@link #eventQueue}. The {@link #subscriptionError}
   * is only propagated when the queue is empty as this simplifies state management and
   * checkpointing a lot.
   */
  private final BiConsumer<Void, Throwable> errorHandler =
      (Void unused, Throwable error) -> {
        if (error != null && subscriptionError == null) {
          subscriptionError = error;
          state.forEach((k, v) -> v.subscriber.cancel());
        }
      };

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  // EventRecords iterator that is currently consumed
  @Nullable EventRecords current = null;

  private final WatermarkPolicy latestRecordTimestampPolicy =
      WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();

  EFOShardSubscribersPool(KinesisIO.Read readSpec, KinesisAsyncClient kinesis) {
    this.poolId = UUID.randomUUID();
    this.read = readSpec;
    this.kinesis = kinesis;
    this.onErrorCoolDownMs = ON_ERROR_COOL_DOWN_MS_DEFAULT;
  }

  EFOShardSubscribersPool(
      KinesisIO.Read readSpec, KinesisAsyncClient kinesis, long onErrorCoolDownMs) {
    this.poolId = UUID.randomUUID();
    this.read = readSpec;
    this.kinesis = kinesis;
    this.onErrorCoolDownMs = onErrorCoolDownMs;
  }

  /**
   * Starts a subscribers pool by starting a {@link EFOShardSubscriber#subscribe shard subscription}
   * for each {@link ShardCheckpoint} with the subscription {@link #errorHandler} callback.
   *
   * <p>{@link EFOShardSubscriber}s with their respective state are tracked in {@link #state}.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void start(Iterable<ShardCheckpoint> checkpoints) {
    LOG.info(
        "Starting pool {} {} {}. Checkpoints = {}",
        poolId,
        read.getStreamName(),
        read.getConsumerArn(),
        checkpoints);
    checkpoints.forEach(
        ch -> {
          EFOShardSubscriber subscriber =
              new EFOShardSubscriber(this, ch.getShardId(), read, kinesis, onErrorCoolDownMs);
          StartingPosition startingPosition = ch.toStartingPosition();
          subscriber.subscribe(startingPosition).whenCompleteAsync(errorHandler);
          ShardState shardState = new ShardState(subscriber);
          state.putIfAbsent(ch.getShardId(), shardState);
        });
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
   */
  @Nullable
  KinesisRecord getNextRecord() throws IOException {
    if (current == null && eventQueue.isEmpty()) {
      if (subscriptionError == null) {
        return null;
      } else {
        throw new IOException(subscriptionError);
      }
    }

    if (current == null) {
      current = eventQueue.poll();
    }

    if (current != null) {
      String shardId = current.shardId;
      ShardState shardState = Preconditions.checkStateNotNull(state.get(shardId));
      if (current.hasNext()) {
        KinesisClientRecord r = current.next();
        shardState.update(r);
        KinesisRecord kinesisRecord = new KinesisRecord(r, read.getStreamName(), shardId);
        latestRecordTimestampPolicy.update(kinesisRecord);
        return kinesisRecord;
      } else {
        onEventDone(shardState, current);
        current = null;
      }
    }
    return null;
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
  @SuppressWarnings("FutureReturnValueIgnored")
  private void onEventDone(ShardState shardState, EventRecords noRecordsEvent) {
    if (noRecordsEvent.event.continuationSequenceNumber() == null
        && noRecordsEvent.event.hasChildShards()
        && subscriptionError == null) {
      LOG.info("Processing re-shard signal {}", noRecordsEvent.event);
      List<String> successorShardsIds = computeSuccessorShardsIds(noRecordsEvent);
      successorShardsIds.forEach(
          successorShardId -> {
            ShardCheckpoint newCheckpoint =
                new ShardCheckpoint(
                    read.getStreamName(),
                    successorShardId,
                    new StartingPoint(InitialPositionInStream.TRIM_HORIZON));

            state.computeIfAbsent(
                successorShardId,
                k -> {
                  EFOShardSubscriber subscriber =
                      new EFOShardSubscriber(
                          this, successorShardId, read, kinesis, onErrorCoolDownMs);
                  StartingPosition startingPosition = newCheckpoint.toStartingPosition();
                  subscriber.subscribe(startingPosition).whenCompleteAsync(errorHandler);
                  return new ShardState(subscriber);
                });
          });
      state.remove(noRecordsEvent.shardId);
    } else {
      shardState.update(noRecordsEvent);
    }
  }

  private static List<String> computeSuccessorShardsIds(EventRecords records) {
    List<String> successorShardsIds = new ArrayList<>();
    SubscribeToShardEvent event = records.event;
    for (ChildShard childShard : event.childShards()) {
      if (childShard.parentShards().contains(records.shardId)) {
        if (childShard.parentShards().size() > 1) {
          // This is the case of merging two shards into one.
          // when there are 2 parent shards, we only pick it up if
          // its max shard equals to sender shard ID
          String maxId = childShard.parentShards().stream().max(String::compareTo).get();
          if (records.shardId.equals(maxId)) {
            successorShardsIds.add(childShard.shardId());
          }
        } else {
          // This is the case when shard is split
          successorShardsIds.add(childShard.shardId());
        }
      }
    }

    if (successorShardsIds.isEmpty()) {
      LOG.info("Found no successors for shard {}", records.shardId);
    } else {
      LOG.info("Found successors for shard {}: {}", records.shardId, successorShardsIds);
    }
    return successorShardsIds;
  }

  /** Adds a {@link EventRecords} iterator for shardId and event to {@link #eventQueue}. */
  void enqueueEvent(String shardId, SubscribeToShardEvent event) {
    eventQueue.offer(new EventRecords(read.getStreamName(), shardId, event));
  }

  public Instant getWatermark() {
    return latestRecordTimestampPolicy.getWatermark();
  }

  public KinesisReaderCheckpoint getCheckpointMark() {
    List<ShardCheckpoint> checkpoints =
        state.entrySet().stream()
            .map(
                entry -> {
                  // FIXME: this must take into account initial checkpoint the pool was started with
                  // Example - with specific timestamp
                  if (entry.getValue().sequenceNumber == null) {
                    return new ShardCheckpoint(
                        read.getStreamName(),
                        entry.getKey(),
                        new StartingPoint(InitialPositionInStream.TRIM_HORIZON));
                  } else {
                    return new ShardCheckpoint(
                        read.getStreamName(),
                        entry.getKey(),
                        ShardIteratorType.AFTER_SEQUENCE_NUMBER,
                        entry.getValue().sequenceNumber,
                        entry.getValue().subSequenceNumber);
                  }
                })
            .collect(Collectors.toList());

    return new KinesisReaderCheckpoint(checkpoints);
  }

  public boolean stop() throws InterruptedException {
    LOG.info("Stopping pool {}", poolId);
    state.forEach((shardId, st) -> st.subscriber.cancel());
    scheduler.shutdown();
    return scheduler.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT_MS, MILLISECONDS);
  }

  /**
   * Mutable class tracking state and progress per shard.
   *
   * <p>A {@link ShardCheckpoint} is the immutable correspondence to this using iterator type {@link
   * ShardIteratorType#AFTER_SEQUENCE_NUMBER} or {@link ShardIteratorType#TRIM_HORIZON} for new
   * shards if {@link #sequenceNumber} is not set yet.
   */
  private static class ShardState {
    EFOShardSubscriber subscriber;
    @Nullable String sequenceNumber = null;
    long subSequenceNumber = 0L;

    ShardState(EFOShardSubscriber subscriber) {
      this.subscriber = subscriber;
    }

    ShardState update(KinesisClientRecord r) {
      sequenceNumber = r.sequenceNumber();
      subSequenceNumber = r.subSequenceNumber();
      return this;
    }

    ShardState update(EventRecords eventRecords) {
      sequenceNumber = eventRecords.event.continuationSequenceNumber();
      subSequenceNumber = 0L;
      subscriber.ackEvent();
      return this;
    }
  }

  /**
   * Lazy iterator over deaggregated {@link KinesisClientRecord}s of {@link #event}.
   *
   * <p>Event {@link Record}s are lazily deaggregated using {@link AggregatorUtil} when {@link
   * ForwardingIterator#delegate()} is first called.
   */
  private static class EventRecords extends ForwardingIterator<KinesisClientRecord> {
    String streamName;
    String shardId;
    SubscribeToShardEvent event;
    @MonotonicNonNull Iterator<KinesisClientRecord> delegate = null;

    public EventRecords(String streamName, String shardId, SubscribeToShardEvent event) {
      this.streamName = streamName;
      this.shardId = shardId;
      this.event = event;
    }

    @Override
    protected Iterator<KinesisClientRecord> delegate() {
      if (event.hasRecords() && !event.records().isEmpty()) {
        if (delegate == null) {
          AggregatorUtil au = new AggregatorUtil();
          delegate =
              au.deaggregate(
                      event.records().stream()
                          .map(KinesisClientRecord::fromRecord)
                          .collect(Collectors.toList()))
                  .iterator();
        }
      } else {
        delegate = Collections.emptyIterator();
      }
      return delegate;
    }
  }

  UUID getPoolId() {
    return poolId;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  <T> CompletableFuture<T> delayedTask(Supplier<CompletableFuture<T>> task, long delayMs) {
    if (delayMs <= 0) {
      return task.get();
    }
    final CompletableFuture<T> cf = new CompletableFuture<>();
    scheduler.schedule(
        () -> task.get().handle((t, e) -> e == null ? cf.complete(t) : cf.completeExceptionally(e)),
        delayMs,
        MILLISECONDS);
    return cf;
  }
}
