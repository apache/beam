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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.aws2.kinesis.CustomOptional;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;
import software.amazon.kinesis.common.InitialPositionInStream;

public class ShardSubscribersPoolImpl implements ShardSubscribersPool {
  private static final Logger LOG = LoggerFactory.getLogger(ShardSubscribersPoolImpl.class);
  private static final int BUFFER_SIZE = 10_000;
  private static final int TARGET_MIN_REMAINING_CAPACITY = BUFFER_SIZE / 5;
  private static final long INITIAL_DEMAND_PER_SHARD = 2L;
  private static final long BUFFER_POLL_WAIT_MS = 1_000L;
  private static final long ENQUEUE_TIMEOUT_MS = 3_000L;

  private final UUID poolId;
  private final Config config;
  private final AsyncClientProxy kinesis;
  private final KinesisReaderCheckpoint initialCheckpoint;
  private final ConcurrentMap<String, ShardSubscriberState> shardsStates;
  private final BlockingQueue<ExtendedKinesisRecord> eventsBuffer;

  public ShardSubscribersPoolImpl(
      Config config, AsyncClientProxy kinesis, KinesisReaderCheckpoint initialCheckpoint) {
    this.poolId = UUID.randomUUID();
    this.config = config;
    this.kinesis = kinesis;
    this.initialCheckpoint = initialCheckpoint;
    this.shardsStates = new ConcurrentHashMap<>();
    this.eventsBuffer = new LinkedBlockingQueue<>(BUFFER_SIZE);
  }

  @Override
  public UUID getPoolId() {
    return poolId;
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public boolean start() {
    // TODO: handle different types of start:
    // ShardIteratorType.LATEST
    // ShardIteratorType.AT_SEQUENCE_NUMBER
    // ShardIteratorType.TRIM_HORIZON
    // ShardIteratorType.AT_TIMESTAMP
    // AT_SEQUENCE_NUMBER:
    // - check if we have seen this seq number before
    // - check if we have seen a sub-sequence number before

    List<String> startingWithShards = checkpointToShardsIds(initialCheckpoint);
    LOG.info(
        "Starting pool {} {} {}. Shards = {}",
        poolId,
        config.getStreamName(),
        config.getConsumerArn(),
        startingWithShards);

    initialCheckpoint.forEach(
        shardCheckpoint -> createSubscription(shardCheckpoint.getShardId(), shardCheckpoint, true));
    return true;
  }

  private void decommissionShardSubscription(String shardId) {
    shardsStates.computeIfPresent(
        shardId,
        (s, st) -> {
          st.cancel();
          st.markClosed();
          return st;
        });
  }

  @Override
  public boolean stop() {
    KinesisReaderCheckpoint ch = getCheckpointMark();
    List<String> stoppingWithShards = checkpointToShardsIds(ch);
    LOG.info(
        "Stopping pool {} {} {}. Shards = {}",
        poolId,
        config.getStreamName(),
        config.getConsumerArn(),
        stoppingWithShards);

    shardsStates.values().forEach(ShardSubscriberState::cancel);
    try {
      Thread.sleep(config.getStopCoolDownDelayMs());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return true;
  }

  /**
   * This is ultimately called by Netty threads via {@link KinesisShardEventsSubscriber} callbacks.
   *
   * @param event
   * @throws InterruptedException
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public void handleEvent(ShardEvent event) throws InterruptedException {
    switch (event.getType()) {
      case SUBSCRIPTION_COMPLETE:
        CompletableFuture.runAsync(
            () -> {
              ShardSubscriberState state = shardsStates.get(event.getShardId());
              if (state != null) {
                createSubscription(event.getShardId(), state.getCheckpoint(), false);
              }
            });
        break;
      case RE_SHARD:
        CompletableFuture.runAsync(
            () -> {
              List<String> successorShards = computeSuccessorShardsIds((ReShardEvent) event);
              successorShards.forEach(
                  successorShardId -> {
                    ShardCheckpoint newCheckpoint =
                        new ShardCheckpoint(
                            config.getStreamName(),
                            config.getConsumerArn(),
                            successorShardId,
                            new StartingPoint(InitialPositionInStream.TRIM_HORIZON));
                    createSubscription(successorShardId, newCheckpoint, false);
                  });

              decommissionShardSubscription(event.getShardId());
              try {
                if (!eventsBuffer.offer(
                    ExtendedKinesisRecord.fromReShard(event.getShardId()),
                    ENQUEUE_TIMEOUT_MS,
                    TimeUnit.MILLISECONDS)) {
                  // We should never demand more than we consume.
                  throw new RuntimeException("This should never happen");
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
        break;
      case RECORDS:
        List<ExtendedKinesisRecord> records = ((RecordsShardEvent) event).getRecords();
        for (ExtendedKinesisRecord record : records) {
          if (!eventsBuffer.offer(record, ENQUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            // We should never demand more than we consume.
            throw new RuntimeException("This should never happen");
          }
        }
        break;
      case ERROR:
        ErrorShardEvent errorShardEvent = (ErrorShardEvent) event;
        ConsumerError error = ConsumerError.toConsumerError(errorShardEvent.getErr());

        if (error instanceof RecoverableConsumerError) {
          LOG.warn(
              "Pool id = {} shard id = {} got recoverable error",
              poolId,
              event.getShardId(),
              errorShardEvent.getErr());
          CompletableFuture.runAsync(
              () -> {
                try {
                  Thread.sleep(config.getReconnectAfterFailureDelayMs());
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                ShardSubscriberState state = shardsStates.get(event.getShardId());
                if (state != null) {
                  state.cancel();
                  createSubscription(event.getShardId(), state.getCheckpoint(), false);
                }
              });
        } else {
          LOG.error(
              "Pool id = {} shard id = {} got critical error",
              poolId,
              event.getShardId(),
              errorShardEvent.getErr());
          CompletableFuture.runAsync(
              () -> {
                shardsStates.computeIfPresent(
                    event.getShardId(),
                    (k, v) -> {
                      v.setErr(errorShardEvent.getErr());
                      return v;
                    });
                try {
                  if (!eventsBuffer.offer(
                      ExtendedKinesisRecord.fromError(event.getShardId()),
                      ENQUEUE_TIMEOUT_MS,
                      TimeUnit.MILLISECONDS)) {
                    // We should never demand more than we consume.
                    throw new RuntimeException("This should never happen");
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                stop();
              });
        }
        break;

      default:
        LOG.warn("Unknown event type {}", event.getType());
    }
  }

  private static List<String> computeSuccessorShardsIds(ReShardEvent event) {
    // FIXME: If future completes with unrecoverable error, entire reader should fail
    // FIXME: Down-sharding can cause shards to be "lost"! Re-visit this logic!
    LOG.info("Processing re-shard signal {}", event);

    List<String> successorShardsIds = new ArrayList<>();

    for (ChildShard childShard : event.getChildShards()) {
      if (childShard.parentShards().contains(event.getShardId())) {
        if (childShard.parentShards().size() > 1) {
          // This is the case of merging two shards into one.
          // when there are 2 parent shards, we only pick it up if
          // its max shard equals to sender shard ID
          String maxId = childShard.parentShards().stream().max(String::compareTo).get();
          if (event.getShardId().equals(maxId)) {
            successorShardsIds.add(childShard.shardId());
          }
        } else {
          // This is the case when shard is split
          successorShardsIds.add(childShard.shardId());
        }
      }
    }

    if (successorShardsIds.isEmpty()) {
      LOG.info("Found no successors for shard {}", event.getShardId());
    } else {
      LOG.info("Found successors for shard {}: {}", event.getShardId(), successorShardsIds);
    }
    return successorShardsIds;
  }

  private CompletableFuture<Void> createSubscription(
      String shardId, ShardCheckpoint checkpoint, boolean isInitial) {
    UUID subscribeRequestId = UUID.randomUUID();
    SubscribeToShardRequest request =
        SubscribeToShardRequest.builder()
            .consumerARN(config.getConsumerArn())
            .shardId(shardId)
            .startingPosition(checkpoint.toStartingPosition())
            .build();

    LOG.info("Pool id {} starting subscribe request {} - {}", poolId, subscribeRequestId, request);

    CountDownLatch eventsHandlerReadyLatch = new CountDownLatch(1);

    KinesisShardEventsSubscriber shardEventsSubscriber =
        new KinesisShardEventsSubscriber(
            this,
            eventsHandlerReadyLatch,
            config.getStreamName(),
            config.getConsumerArn(),
            shardId);

    SubscribeToShardResponseHandler responseHandler =
        SubscribeToShardResponseHandler.builder()
            .onError(
                e ->
                    LOG.error(
                        "Pool id = {} failed to execute subscribe request {} - {}",
                        poolId,
                        subscribeRequestId,
                        request,
                        e))
            .subscriber(() -> shardEventsSubscriber)
            .build();

    CompletableFuture<Void> f = kinesis.subscribeToShard(request, responseHandler);
    ShardSubscriberState shardState =
        new ShardSubscriberStateImpl(f, shardEventsSubscriber, checkpoint);
    boolean subscriptionWasEstablished = false;
    try {
      subscriptionWasEstablished =
          eventsHandlerReadyLatch.await(config.getPoolStartTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // handled below
    }

    if (!subscriptionWasEstablished) {
      LOG.error("Subscribe request {} failed.", subscribeRequestId);
      if (!f.isCompletedExceptionally()) {
        LOG.warn(
            "subscribeToShard request {} failed, but future was not complete.", subscribeRequestId);
      }
      shardState.cancel();
      throw new RuntimeException();
    }

    if (isInitial) {
      shardState.requestRecords(INITIAL_DEMAND_PER_SHARD);
    } else {
      shardState.requestRecords(1L);
    }
    shardsStates.put(shardId, shardState);
    LOG.info(
        "Pool id = {} request = {} subscription for shard {} established",
        poolId,
        subscribeRequestId,
        shardId);
    return f;
  }

  @Override
  public CustomOptional<KinesisRecord> nextRecord() throws IOException {
    try {
      ExtendedKinesisRecord maybeRecord =
          eventsBuffer.poll(BUFFER_POLL_WAIT_MS, TimeUnit.MILLISECONDS);
      if (maybeRecord != null) {
        try {
          shardsStates.computeIfPresent(
              maybeRecord.getShardId(),
              (k, v) -> {
                v.ackRecord(maybeRecord);
                if (eventsBuffer.remainingCapacity() > TARGET_MIN_REMAINING_CAPACITY) {
                  v.requestRecords(1L);
                }
                return v;
              });
        } catch (IllegalStateException e) {
          throw new IOException(e.getCause());
        }
        KinesisRecord record = maybeRecord.getKinesisRecord();
        if (record != null) {
          return CustomOptional.of(record);
        } else {
          return CustomOptional.absent();
        }
      } else {
        return CustomOptional.absent();
      }
    } catch (InterruptedException e) {
      return CustomOptional.absent();
    }
  }

  @Override
  public Instant getWatermark() {
    return shardsStates.values().stream()
        .map(ShardSubscriberState::getShardWatermark)
        .min(Comparator.naturalOrder())
        .orElse(BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  @Override
  public KinesisReaderCheckpoint getCheckpointMark() {
    List<ShardCheckpoint> checkpoints =
        shardsStates.values().stream()
            .map(ShardSubscriberState::getCheckpoint)
            .filter(ck -> !ck.isOrphan())
            .collect(Collectors.toList());

    return new KinesisReaderCheckpoint(checkpoints);
  }

  private List<String> checkpointToShardsIds(KinesisReaderCheckpoint checkpoint) {
    List<String> result = new ArrayList<>();
    checkpoint.forEach(chk -> result.add(chk.getShardId()));
    return Collections.unmodifiableList(result);
  }
}
