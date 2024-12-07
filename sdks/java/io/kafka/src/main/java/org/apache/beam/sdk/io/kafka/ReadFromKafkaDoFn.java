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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO.ReadSourceDescriptors;
import org.apache.beam.sdk.io.kafka.KafkaIOUtils.MovingAvg;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedReader.TimestampPolicyContext;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.MonotonicallyIncreasing;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalCause;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalListener;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closeables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.FluentFuture;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.FutureCallback;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListenableFuture;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListenableFutureTask;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Deserializer;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SplittableDoFn which reads from {@link KafkaSourceDescriptor} and outputs pair of {@link
 * KafkaSourceDescriptor} and {@link KafkaRecord}. By default, a {@link MonotonicallyIncreasing}
 * watermark estimator is used to track watermark.
 *
 * <p>{@link ReadFromKafkaDoFn} implements the logic of reading from Kafka. The element is a {@link
 * KafkaSourceDescriptor}, and the restriction is an {@link OffsetRange} which represents record
 * offset. A {@link GrowableOffsetRangeTracker} is used to track an {@link OffsetRange} ended with
 * {@code Long.MAX_VALUE}. For a finite range, a {@link OffsetRangeTracker} is created.
 *
 * <h4>Initial Restriction</h4>
 *
 * <p>The initial range for a {@link KafkaSourceDescriptor} is defined by {@code [startOffset,
 * Long.MAX_VALUE)} where {@code startOffset} is defined as:
 *
 * <ul>
 *   <li>the {@code startReadOffset} if {@link KafkaSourceDescriptor#getStartReadOffset} is set.
 *   <li>the first offset with a greater or equivalent timestamp if {@link
 *       KafkaSourceDescriptor#getStartReadTime()} is set.
 *   <li>the {@code last committed offset + 1} for the {@link Consumer#position(TopicPartition)
 *       topic partition}.
 * </ul>
 *
 * <h4>Splitting</h4>
 *
 * <p>TODO(https://github.com/apache/beam/issues/20280): Add support for initial splitting.
 *
 * <h4>Checkpoint and Resume Processing</h4>
 *
 * <p>There are 2 types of checkpoint here: self-checkpoint which invokes by the DoFn and
 * system-checkpoint which is issued by the runner via {@link
 * org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest}. Every time the
 * consumer gets empty response from {@link Consumer#poll(long)}, {@link ReadFromKafkaDoFn} will
 * checkpoint the current {@link KafkaSourceDescriptor} and move to process the next element. These
 * deferred elements will be resumed by the runner as soon as possible.
 *
 * <h4>Progress and Size</h4>
 *
 * <p>The progress is provided by {@link GrowableOffsetRangeTracker} or per {@link
 * KafkaSourceDescriptor}. For an infinite {@link OffsetRange}, a Kafka {@link Consumer} is used in
 * the {@link GrowableOffsetRangeTracker} as the {@link
 * GrowableOffsetRangeTracker.RangeEndEstimator} to poll the latest offset. Please refer to {@link
 * ReadFromKafkaDoFn#restrictionTracker(KafkaSourceDescriptor, OffsetRange)} for details.
 *
 * <p>The size is computed by {@link ReadFromKafkaDoFn#getSize(KafkaSourceDescriptor, OffsetRange)}.
 * A {@link KafkaIOUtils.MovingAvg} is used to track the average size of kafka records.
 *
 * <h4>Track Watermark</h4>
 *
 * <p>The {@link WatermarkEstimator} is created by {@link
 * ReadSourceDescriptors#getCreateWatermarkEstimatorFn()}. The estimated watermark is computed by
 * this {@link WatermarkEstimator} based on output timestamps computed by {@link
 * ReadSourceDescriptors#getExtractOutputTimestampFn()} (SerializableFunction)}. The default
 * configuration is using {@link ReadSourceDescriptors#withProcessingTime()} as the {@code
 * extractTimestampFn} and {@link
 * ReadSourceDescriptors#withMonotonicallyIncreasingWatermarkEstimator()} as the {@link
 * WatermarkEstimator}.
 *
 * <h4>Stop Reading from Removed {@link TopicPartition}</h4>
 *
 * {@link ReadFromKafkaDoFn} will stop reading from any removed {@link TopicPartition} automatically
 * by querying Kafka {@link Consumer} APIs. Please note that stopping reading may not happen as soon
 * as the {@link TopicPartition} is removed. For example, the removal could happen at the same time
 * when {@link ReadFromKafkaDoFn} performs a {@link Consumer#poll(java.time.Duration)}. In that
 * case, the {@link ReadFromKafkaDoFn} will still output the fetched records.
 *
 * <h4>Stop Reading from Stopped {@link TopicPartition}</h4>
 *
 * {@link ReadFromKafkaDoFn} will also stop reading from certain {@link TopicPartition} if it's a
 * good time to do so by querying {@link ReadFromKafkaDoFn#checkStopReadingFn}. {@link
 * ReadFromKafkaDoFn#checkStopReadingFn} is a customer-provided callback which is used to determine
 * whether to stop reading from the given {@link TopicPartition}. Similar to the mechanism of
 * stopping reading from removed {@link TopicPartition}, the stopping reading may not happens
 * immediately.
 */
abstract class ReadFromKafkaDoFn<K, V>
    extends DoFn<KafkaSourceDescriptor, KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> {

  static <K, V> ReadFromKafkaDoFn<K, V> create(
      ReadSourceDescriptors<K, V> transform,
      TupleTag<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> recordTag) {
    if (transform.isBounded()) {
      return new Bounded<>(transform, recordTag);
    } else {
      return new Unbounded<>(transform, recordTag);
    }
  }

  @UnboundedPerElement
  private static class Unbounded<K, V> extends ReadFromKafkaDoFn<K, V> {
    Unbounded(
        ReadSourceDescriptors<K, V> transform,
        TupleTag<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> recordTag) {
      super(transform, recordTag);
    }
  }

  @BoundedPerElement
  private static class Bounded<K, V> extends ReadFromKafkaDoFn<K, V> {
    Bounded(
        ReadSourceDescriptors<K, V> transform,
        TupleTag<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> recordTag) {
      super(transform, recordTag);
    }
  }

  private ReadFromKafkaDoFn(
      ReadSourceDescriptors<K, V> transform,
      TupleTag<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> recordTag) {
    this.consumerConfig = transform.getConsumerConfig();
    this.keyDeserializerProvider =
        Preconditions.checkArgumentNotNull(transform.getKeyDeserializerProvider());
    this.valueDeserializerProvider =
        Preconditions.checkArgumentNotNull(transform.getValueDeserializerProvider());
    this.consumerFactoryFn = transform.getConsumerFactoryFn();
    this.extractOutputTimestampFn = transform.getExtractOutputTimestampFn();
    this.createWatermarkEstimatorFn = transform.getCreateWatermarkEstimatorFn();
    this.timestampPolicyFactory = transform.getTimestampPolicyFactory();
    this.checkStopReadingFn = transform.getCheckStopReadingFn();
    this.badRecordRouter = transform.getBadRecordRouter();
    this.recordTag = recordTag;
    if (transform.getConsumerPollingTimeout() > 0) {
      this.consumerPollingTimeout = transform.getConsumerPollingTimeout();
    } else {
      this.consumerPollingTimeout = DEFAULT_KAFKA_POLL_TIMEOUT;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ReadFromKafkaDoFn.class);

  /**
   * A holder class for all construction time unique instances of {@link ReadFromKafkaDoFn}. Caches
   * must run clean up tasks when {@link #teardown()} is called.
   */
  private static final class SharedStateHolder {
    private static final Map<Long, LoadingCache<KafkaSourceDescriptor, AverageRecordSize>>
        AVG_RECORD_SIZE_CACHE = new ConcurrentHashMap<>();
    private static final Map<
            Long, LoadingCache<Optional<ImmutableSet<String>>, ConsumerExecutionContext>>
        CONSUMER_EXECUTION_CONTEXT_CACHE = new ConcurrentHashMap<>();
  }

  static final class TopicPartitionPollState implements AutoCloseable {
    private static final List<ConsumerRecord<byte[], byte[]>> CLOSED_SENTINEL = Arrays.asList();

    private final AtomicBoolean closed;
    private final LinkedTransferQueue<List<ConsumerRecord<byte[], byte[]>>> queue;
    private final TopicPartition topicPartition;
    private final OffsetRange offsetRange;

    TopicPartitionPollState(final TopicPartition topicPartition, final OffsetRange offsetRange) {
      this.closed = new AtomicBoolean();
      this.queue = new LinkedTransferQueue<>();
      this.topicPartition = topicPartition;
      this.offsetRange = offsetRange;
    }

    TopicPartition getTopicPartition() {
      return this.topicPartition;
    }

    OffsetRange getOffsetRange() {
      return this.offsetRange;
    }

    boolean hasWaitingConsumer() {
      return !this.closed.get() && queue.hasWaitingConsumer();
    }

    Optional<List<ConsumerRecord<byte[], byte[]>>> take() throws InterruptedException {
      if (this.closed.get()) {
        return Optional.empty();
      }
      return Optional.of(queue.take()).filter(records -> records != CLOSED_SENTINEL);
    }

    boolean tryTransfer(List<ConsumerRecord<byte[], byte[]>> records) {
      if (this.closed.get()) {
        return false;
      }
      if (this.closed.compareAndSet(queue.tryTransfer(records), true)) {
        queue.put(CLOSED_SENTINEL);
        return false;
      }
      return true;
    }

    boolean isClosed() {
      return this.closed.get();
    }

    @Override
    public void close() {
      if (this.closed.compareAndSet(false, true)) {
        queue.put(CLOSED_SENTINEL);
      }
    }
  }

  static final class ConsumerExecutionContext extends AbstractExecutionThreadService {
    private final Consumer<byte[], byte[]> consumer;
    private final LinkedBlockingQueue<Runnable> workQueue;
    private final AtomicReference<Map<TopicPartition, Long>> endOffsets;
    private final Map<TopicPartition, Long> offsetsForStartReadTimesArgument;
    private ListenableFutureTask<Map<TopicPartition, OffsetAndTimestamp>>
        offsetsForStartReadTimesTask;
    private final Map<TopicPartition, Long> offsetsForStopReadTimesArgument;
    private ListenableFutureTask<Map<TopicPartition, OffsetAndTimestamp>>
        offsetsForStopReadTimesTask;
    private final List<TopicPartitionPollState> queuedSplits;
    private final Map<TopicPartition, TopicPartitionPollState> splits;

    ConsumerExecutionContext(final Consumer<byte[], byte[]> consumer) {
      this.consumer = consumer;
      this.workQueue = new LinkedBlockingQueue<>();
      this.endOffsets = new AtomicReference<>(Collections.emptyMap());

      this.offsetsForStartReadTimesArgument = new HashMap<>();
      this.offsetsForStartReadTimesTask =
          ListenableFutureTask.create(
              () ->
                  consumer.offsetsForTimes(
                      this.offsetsForStartReadTimesArgument,
                      Duration.ofSeconds(DEFAULT_KAFKA_POLL_TIMEOUT)));

      this.offsetsForStopReadTimesArgument = new HashMap<>();
      this.offsetsForStopReadTimesTask =
          ListenableFutureTask.create(
              () ->
                  consumer.offsetsForTimes(
                      this.offsetsForStopReadTimesArgument,
                      Duration.ofSeconds(DEFAULT_KAFKA_POLL_TIMEOUT)));

      this.queuedSplits = new ArrayList<>();
      this.splits = new HashMap<>();
    }

    public ListenableFuture<@Nullable OffsetAndTimestamp> getOffsetForStartReadTime(
        final TopicPartition topicPartition, long time) {
      // Note: The ordering of statements is deliberate. Transformation callbacks are executed in
      // the thread that first observes the completion after the callback's registration.
      // Registration depends on inter-thread actions and enqueueing the task guarantees that these
      // actions are observed to happen before dequeueing the element and the consequent completion
      // of its future.
      // The partial ordering of these actions ensures that the future can not be observed as
      // fulfilled when the registration happens.
      final ListenableFutureTask<Void> task =
          ListenableFutureTask.create(
              () -> this.offsetsForStartReadTimesArgument.put(topicPartition, time), null);
      final FluentFuture<@Nullable OffsetAndTimestamp> future =
          FluentFuture.from(task)
              .transformAsync(
                  unused -> Futures.nonCancellationPropagating(this.offsetsForStartReadTimesTask),
                  MoreExecutors.directExecutor())
              .<@Nullable OffsetAndTimestamp>transform(
                  beginningOffsets -> beginningOffsets.get(topicPartition),
                  MoreExecutors.directExecutor());
      workQueue.add(task);
      return future;
    }

    public ListenableFuture<@Nullable OffsetAndTimestamp> getOffsetForStopReadTime(
        final TopicPartition topicPartition, long time) {
      // Note: The ordering of statements is deliberate. Transformation callbacks are executed in
      // the thread that first observes the completion after the callback's registration.
      // Registration depends on inter-thread actions and enqueueing the task guarantees that these
      // actions are observed to happen before dequeueing the element and the consequent completion
      // of its future.
      // The partial ordering of these actions ensures that the future can not be observed as
      // fulfilled when the registration happens.
      final ListenableFutureTask<Void> task =
          ListenableFutureTask.create(
              () -> this.offsetsForStopReadTimesArgument.put(topicPartition, time), null);
      final FluentFuture<@Nullable OffsetAndTimestamp> future =
          FluentFuture.from(task)
              .transformAsync(
                  unused -> Futures.nonCancellationPropagating(this.offsetsForStopReadTimesTask),
                  MoreExecutors.directExecutor())
              .<@Nullable OffsetAndTimestamp>transform(
                  beginningOffsets -> beginningOffsets.get(topicPartition),
                  MoreExecutors.directExecutor());
      workQueue.add(task);
      return future;
    }

    public TopicPartitionPollState assign(
        final TopicPartition topicPartition, final OffsetRange offsetRange) {
      final TopicPartitionPollState pollState =
          new TopicPartitionPollState(topicPartition, offsetRange);
      workQueue.add(() -> this.queuedSplits.add(pollState));
      return pollState;
    }

    @Override
    public void run() {
      try (Consumer<byte[], byte[]> consumer = this.consumer) {
        while (!Thread.currentThread().isInterrupted()) {
          List<Runnable> tasks;

          // If there's no assigned split, sleep until a task becomes available.
          // Otherwise drain the queue immediately.
          if (this.splits.isEmpty()) {
            try {
              tasks = Collections.singletonList(this.workQueue.take());
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              continue;
            }
          } else {
            tasks = new ArrayList<>();
            workQueue.drainTo(tasks);
          }
          tasks.forEach(Runnable::run);

          if (!offsetsForStartReadTimesArgument.isEmpty()) {
            final ListenableFutureTask<Map<TopicPartition, OffsetAndTimestamp>>
                currentOffsetsForStartReadTimeTask = this.offsetsForStartReadTimesTask;
            currentOffsetsForStartReadTimeTask.addListener(
                () -> this.offsetsForStartReadTimesArgument.clear(),
                MoreExecutors.directExecutor());
            this.offsetsForStartReadTimesTask =
                ListenableFutureTask.create(
                    () ->
                        consumer.offsetsForTimes(
                            this.offsetsForStartReadTimesArgument,
                            Duration.ofSeconds(DEFAULT_KAFKA_POLL_TIMEOUT)));

            currentOffsetsForStartReadTimeTask.run();
          }

          if (!offsetsForStopReadTimesArgument.isEmpty()) {
            final ListenableFutureTask<Map<TopicPartition, OffsetAndTimestamp>>
                currentOffsetsForStopReadTimeTask = this.offsetsForStopReadTimesTask;
            currentOffsetsForStopReadTimeTask.addListener(
                () -> this.offsetsForStopReadTimesArgument.clear(), MoreExecutors.directExecutor());
            this.offsetsForStopReadTimesTask =
                ListenableFutureTask.create(
                    () ->
                        consumer.offsetsForTimes(
                            this.offsetsForStopReadTimesArgument,
                            Duration.ofSeconds(DEFAULT_KAFKA_POLL_TIMEOUT)));

            currentOffsetsForStopReadTimeTask.run();
          }

          this.splits.entrySet().removeIf(entry -> entry.getValue().isClosed());

          if (!queuedSplits.isEmpty()) {
            queuedSplits.forEach(
                pollState ->
                    splits.compute(
                        pollState.getTopicPartition(),
                        (topicPartition, currentPollState) -> {
                          if (currentPollState != null) {
                            currentPollState.close();
                          }
                          return pollState;
                        }));
            consumer.assign(this.splits.keySet());
            queuedSplits.forEach(
                pollState -> {
                  if (pollState.getOffsetRange().getFrom() >= 0L) {
                    consumer.seek(
                        pollState.getTopicPartition(), pollState.getOffsetRange().getFrom());
                  }
                });
            queuedSplits.clear();
          }

          if (this.splits.isEmpty()) {
            continue;
          }

          final List<TopicPartition> toPause = new ArrayList<>();
          final List<TopicPartition> toResume = new ArrayList<>();

          final ListenableFutureTask<ConsumerRecords<byte[], byte[]>> currentPollTask =
              ListenableFutureTask.create(
                  () -> consumer.poll(Duration.ofSeconds(DEFAULT_KAFKA_POLL_TIMEOUT)));
          Futures.addCallback(
              currentPollTask,
              new FutureCallback<ConsumerRecords<byte[], byte[]>>() {
                @Override
                public void onSuccess(ConsumerRecords<byte[], byte[]> result) {
                  ConsumerExecutionContext.this
                      .splits
                      .entrySet()
                      .removeIf(
                          entry -> !entry.getValue().tryTransfer(result.records(entry.getKey())));
                }

                @Override
                public void onFailure(Throwable t) {
                  if (!(t instanceof TimeoutException)) {
                    ConsumerExecutionContext.this
                        .splits
                        .values()
                        .forEach(TopicPartitionPollState::close);
                  }
                }
              },
              MoreExecutors.directExecutor());
          final ListenableFutureTask<Map<TopicPartition, Long>> currentEndOffsetsTask =
              ListenableFutureTask.create(
                  () ->
                      consumer.endOffsets(
                          this.splits.keySet(), Duration.ofSeconds(DEFAULT_KAFKA_POLL_TIMEOUT)));
          Futures.addCallback(
              currentEndOffsetsTask,
              new FutureCallback<Map<TopicPartition, Long>>() {
                @Override
                public void onSuccess(Map<TopicPartition, Long> result) {
                  ConsumerExecutionContext.this.endOffsets.set(result);
                }

                @Override
                public void onFailure(Throwable t) {
                  ConsumerExecutionContext.this.endOffsets.set(Collections.emptyMap());
                }
              },
              MoreExecutors.directExecutor());
          this.splits.forEach(
              (topicPartition, pollState) -> {
                if (pollState.hasWaitingConsumer()) {
                  toResume.add(topicPartition);
                } else {
                  toPause.add(topicPartition);
                }
              });
          consumer.pause(toPause);
          consumer.resume(toResume);
          currentPollTask.run();
          currentEndOffsetsTask.run();
        }
      } finally {
        this.offsetsForStartReadTimesTask.cancel(false);
        this.offsetsForStopReadTimesTask.cancel(false);
        this.splits.values().forEach(TopicPartitionPollState::close);
      }
    }

    public Map<TopicPartition, Long> getEndOffsets() {
      return this.endOffsets.get();
    }

    public void wakeup() {
      LOG.info("Waking up consumer");
      this.consumer.wakeup();
    }
  }

  private static final AtomicLong FN_ID = new AtomicLong();

  private static final Joiner COMMA_JOINER = Joiner.on(',');

  // A unique identifier for the instance. Generally unique unless the ID generator overflows.
  private final long fnId = FN_ID.getAndIncrement();

  private final @Nullable CheckStopReadingFn checkStopReadingFn;

  private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
      consumerFactoryFn;
  private final @Nullable SerializableFunction<KafkaRecord<K, V>, Instant> extractOutputTimestampFn;
  private final @Nullable SerializableFunction<Instant, WatermarkEstimator<Instant>>
      createWatermarkEstimatorFn;
  private final @Nullable TimestampPolicyFactory<K, V> timestampPolicyFactory;

  private final BadRecordRouter badRecordRouter;

  private final TupleTag<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> recordTag;

  // Valid between bundle start and bundle finish.
  private transient @Nullable Deserializer<K> keyDeserializerInstance = null;
  private transient @Nullable Deserializer<V> valueDeserializerInstance = null;
  // Only used to retain a strong reference to the consumer execution context until this function
  // instance is torn down.
  // This ties the lifetime of the consumer execution context to that of the bundle processor (or
  // equivalent for non-portable runners).
  // The consumer execution context cache stores weak references to consumer execution contexts,
  // thus allowing the garbage collector to finalize the consumer execution context when no strong
  // references to it are held.
  @SuppressWarnings("unused")
  private transient @Nullable ConsumerExecutionContext consumerExecutionContextInstance = null;

  private transient @MonotonicNonNull LoadingCache<KafkaSourceDescriptor, AverageRecordSize>
      avgRecordSizeCache;

  private transient @MonotonicNonNull LoadingCache<
          Optional<ImmutableSet<String>>, ConsumerExecutionContext>
      consumerExecutionContextCache;

  private static final long DEFAULT_KAFKA_POLL_TIMEOUT = 10L;
  @VisibleForTesting final long consumerPollingTimeout;
  @VisibleForTesting final DeserializerProvider<K> keyDeserializerProvider;
  @VisibleForTesting final DeserializerProvider<V> valueDeserializerProvider;
  @VisibleForTesting final Map<String, Object> consumerConfig;
  @VisibleForTesting static final String METRIC_NAMESPACE = KafkaUnboundedReader.METRIC_NAMESPACE;

  @VisibleForTesting
  static final String RAW_SIZE_METRIC_PREFIX = KafkaUnboundedReader.RAW_SIZE_METRIC_PREFIX;

  /**
   * A {@link GrowableOffsetRangeTracker.RangeEndEstimator} which uses a Kafka {@link Consumer} to
   * fetch backlog.
   */
  private static class KafkaLatestOffsetEstimator
      implements GrowableOffsetRangeTracker.RangeEndEstimator {

    private final KafkaSourceDescriptor sourceDescriptor;
    private final LoadingCache<Optional<ImmutableSet<String>>, ConsumerExecutionContext>
        consumerExecutionContextCache;
    private @Nullable ConsumerExecutionContext consumerExecutionContextInstance;

    KafkaLatestOffsetEstimator(
        final KafkaSourceDescriptor sourceDescriptor,
        final LoadingCache<Optional<ImmutableSet<String>>, ConsumerExecutionContext>
            consumerExecutionContextCache) {
      this.sourceDescriptor = sourceDescriptor;
      this.consumerExecutionContextCache = consumerExecutionContextCache;
      this.consumerExecutionContextInstance = null;
    }

    @Override
    public long estimate() {
      Optional<ImmutableSet<String>> consumerExecutionContextKey =
          Optional.ofNullable(sourceDescriptor.getBootStrapServers()).map(ImmutableSet::copyOf);
      ConsumerExecutionContext consumerExecutionContext;
      try {
        consumerExecutionContext =
            this.consumerExecutionContextCache.get(consumerExecutionContextKey);
      } catch (ExecutionException ex) {
        return -1L;
      }
      try {
        consumerExecutionContext.awaitRunning();
      } catch (IllegalStateException ex) {
        consumerExecutionContextCache.invalidate(consumerExecutionContextKey);
        return -1L;
      }
      this.consumerExecutionContextInstance = consumerExecutionContext;
      return this.consumerExecutionContextInstance
          .getEndOffsets()
          .getOrDefault(this.sourceDescriptor.getTopicPartition(), -1L);
    }
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element KafkaSourceDescriptor kafkaSourceDescriptor)
      throws Throwable {
    LOG.info("Creating initial restriction for {}", kafkaSourceDescriptor);

    // The context may not be used at all, but unconditionally fetching it here may avoid a load
    // during processing.
    Optional<ImmutableSet<String>> consumerExecutionContextKey =
        Optional.ofNullable(kafkaSourceDescriptor.getBootStrapServers()).map(ImmutableSet::copyOf);
    LoadingCache<Optional<ImmutableSet<String>>, ConsumerExecutionContext>
        consumerExecutionContextCache = checkNotNull(this.consumerExecutionContextCache);
    ConsumerExecutionContext consumerExecutionContext =
        consumerExecutionContextCache.get(consumerExecutionContextKey);
    try {
      consumerExecutionContext.awaitRunning();
    } catch (IllegalStateException ex) {
      consumerExecutionContextCache.invalidate(consumerExecutionContextKey);
      throw ex;
    }
    this.consumerExecutionContextInstance = consumerExecutionContext;

    final ListenableFuture<Long> startOffset;
    @Nullable Long startReadOffset = kafkaSourceDescriptor.getStartReadOffset();
    @Nullable Instant startReadTime = kafkaSourceDescriptor.getStartReadTime();
    if (startReadOffset != null) {
      startOffset = Futures.immediateFuture(startReadOffset);
    } else if (startReadTime != null) {
      startOffset =
          Futures.transform(
              consumerExecutionContext.getOffsetForStartReadTime(
                  kafkaSourceDescriptor.getTopicPartition(), startReadTime.getMillis()),
              offsetAndTimestamp -> offsetAndTimestamp == null ? -1L : offsetAndTimestamp.offset(),
              MoreExecutors.directExecutor());
    } else {
      startOffset = Futures.immediateFuture(-1L);
    }

    final ListenableFuture<Long> endOffset;
    @Nullable Long stopReadOffset = kafkaSourceDescriptor.getStopReadOffset();
    @Nullable Instant stopReadTime = kafkaSourceDescriptor.getStopReadTime();
    if (stopReadOffset != null) {
      endOffset = Futures.immediateFuture(stopReadOffset);
    } else if (stopReadTime != null) {
      endOffset =
          Futures.transform(
              consumerExecutionContext.getOffsetForStopReadTime(
                  kafkaSourceDescriptor.getTopicPartition(), stopReadTime.getMillis()),
              offsetAndTimestamp ->
                  offsetAndTimestamp == null ? Long.MAX_VALUE : offsetAndTimestamp.offset(),
              MoreExecutors.directExecutor());
    } else {
      endOffset = Futures.immediateFuture(Long.MAX_VALUE);
    }

    OffsetRange initialRestriction;
    try {
      initialRestriction = new OffsetRange(startOffset.get(), endOffset.get());
    } catch (ExecutionException ex) {
      throw MoreObjects.firstNonNull(ex.getCause(), ex);
    }

    Lineage.getSources()
        .add(
            "kafka",
            ImmutableList.of(
                Optional.ofNullable(
                        (@Nullable List<String>)
                            ConfigDef.parseType(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                MoreObjects.firstNonNull(
                                    (@Nullable Object)
                                        consumerConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                                    (@Nullable Object) kafkaSourceDescriptor.getBootStrapServers()),
                                ConfigDef.Type.LIST))
                    .map(ImmutableSet::copyOf)
                    .map(COMMA_JOINER::join)
                    .orElse(""),
                MoreObjects.firstNonNull(
                    kafkaSourceDescriptor.getTopic(),
                    kafkaSourceDescriptor.getTopicPartition().topic())));
    return initialRestriction;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public WatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    SerializableFunction<Instant, WatermarkEstimator<Instant>> createWatermarkEstimatorFn =
        Preconditions.checkStateNotNull(this.createWatermarkEstimatorFn);
    return createWatermarkEstimatorFn.apply(ensureTimestampWithinBounds(watermarkEstimatorState));
  }

  @GetSize
  @RequiresNonNull({"avgRecordSizeCache", "consumerExecutionContextCache"})
  public double getSize(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor, @Restriction OffsetRange offsetRange)
      throws ExecutionException {
    final LoadingCache<KafkaSourceDescriptor, AverageRecordSize> avgRecordSizeCache =
        this.avgRecordSizeCache;
    // If present, estimates the record size to offset gap ratio. Compacted topics may hold less
    // records than the estimated offset range due to record deletion within a partition.
    final @Nullable AverageRecordSize avgRecordSize =
        avgRecordSizeCache.getIfPresent(kafkaSourceDescriptor);
    // The tracker estimates the offset range by subtracting the last claimed position from the
    // currently observed end offset for the partition belonging to this split.
    double estimatedOffsetRange =
        restrictionTracker(kafkaSourceDescriptor, offsetRange).getProgress().getWorkRemaining();
    // Before processing elements, we don't have a good estimated size of records and offset gap.
    // Return the estimated offset range without scaling by a size to gap ratio.
    if (avgRecordSize == null) {
      return estimatedOffsetRange;
    }
    // When processing elements, a moving average estimates the size of records and offset gap.
    // Return the estimated offset range scaled by the estimated size to gap ratio.
    return estimatedOffsetRange * avgRecordSize.estimateRecordByteSizeToOffsetCountRatio();
  }

  @NewTracker
  @RequiresNonNull("consumerExecutionContextCache")
  public OffsetRangeTracker restrictionTracker(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor, @Restriction OffsetRange restriction)
      throws ExecutionException {
    final LoadingCache<Optional<ImmutableSet<String>>, ConsumerExecutionContext>
        consumerExecutionContextCache = this.consumerExecutionContextCache;
    if (restriction.getTo() < Long.MAX_VALUE) {
      return new OffsetRangeTracker(restriction);
    }

    return new GrowableOffsetRangeTracker(
        restriction.getFrom(),
        new KafkaLatestOffsetEstimator(kafkaSourceDescriptor, consumerExecutionContextCache));
  }

  @ProcessElement
  @RequiresNonNull({"avgRecordSizeCache", "consumerExecutionContextCache"})
  public ProcessContinuation processElement(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator<Instant> watermarkEstimator,
      MultiOutputReceiver receiver)
      throws Throwable {
    final LoadingCache<KafkaSourceDescriptor, AverageRecordSize> avgRecordSizeCache =
        this.avgRecordSizeCache;
    final LoadingCache<Optional<ImmutableSet<String>>, ConsumerExecutionContext>
        consumerExecutionContextCache = this.consumerExecutionContextCache;
    final Deserializer<K> keyDeserializerInstance =
        Preconditions.checkStateNotNull(this.keyDeserializerInstance);
    final Deserializer<V> valueDeserializerInstance =
        Preconditions.checkStateNotNull(this.valueDeserializerInstance);
    final TopicPartition topicPartition = kafkaSourceDescriptor.getTopicPartition();
    final AverageRecordSize avgRecordSize = avgRecordSizeCache.get(kafkaSourceDescriptor);
    // TODO: Metrics should be reported per split instead of partition, add bootstrap server hash?
    final Distribution rawSizes =
        Metrics.distribution(METRIC_NAMESPACE, RAW_SIZE_METRIC_PREFIX + topicPartition.toString());
    final Gauge backlogBytes =
        Metrics.gauge(
            METRIC_NAMESPACE, RAW_SIZE_METRIC_PREFIX + "backlogBytes_" + topicPartition.toString());

    // Stop processing current TopicPartition when it's time to stop.
    if (checkStopReadingFn != null
        && checkStopReadingFn.apply(kafkaSourceDescriptor.getTopicPartition())) {
      // Attempt to claim the last element in the restriction, such that the restriction tracker
      // doesn't throw an exception when checkDone is called
      tracker.tryClaim(tracker.currentRestriction().getTo() - 1);
      return ProcessContinuation.stop();
    }

    Optional<ImmutableSet<String>> consumerExecutionContextKey =
        Optional.ofNullable(kafkaSourceDescriptor.getBootStrapServers()).map(ImmutableSet::copyOf);
    ConsumerExecutionContext consumerExecutionContext =
        consumerExecutionContextCache.get(consumerExecutionContextKey);
    try {
      consumerExecutionContext.awaitRunning();
    } catch (IllegalStateException ex) {
      consumerExecutionContextCache.invalidate(consumerExecutionContextKey);
      throw ex;
    }
    this.consumerExecutionContextInstance = consumerExecutionContext;

    // If there is a timestampPolicyFactory, create the TimestampPolicy for current
    // TopicPartition.
    TimestampPolicy<K, V> timestampPolicy = null;
    if (timestampPolicyFactory != null) {
      timestampPolicy =
          timestampPolicyFactory.createTimestampPolicy(
              topicPartition, Optional.ofNullable(watermarkEstimator.currentWatermark()));
    }

    LOG.info("Creating Kafka consumer for process continuation for {}", kafkaSourceDescriptor);

    try (TopicPartitionPollState pollState =
        consumerExecutionContext.assign(
            kafkaSourceDescriptor.getTopicPartition(), tracker.currentRestriction())) {
      long startOffset = tracker.currentRestriction().getFrom();
      long expectedOffset = startOffset;
      long skippedRecords = 0L;
      final Stopwatch sw = Stopwatch.createStarted();

      Optional<List<ConsumerRecord<byte[], byte[]>>> records;
      while ((records = pollState.take()).isPresent()) {
        List<ConsumerRecord<byte[], byte[]>> rawRecords = records.get();
        // When there are no records available for the current TopicPartition, self-checkpoint
        // and move to process the next element.
        if (rawRecords.isEmpty()) {
          if (timestampPolicy != null) {
            updateWatermarkManually(timestampPolicy, watermarkEstimator, tracker);
          }
          return ProcessContinuation.resume();
        }
        for (ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
          if (startOffset < 0L) {
            expectedOffset = rawRecord.offset();
          }
          // If the Kafka consumer returns a record with an offset that is already processed
          // the record can be safely skipped. This is needed because there is a possibility
          // that the seek() above fails to move the offset to the desired position. In which
          // case poll() would return records that are already cnsumed.
          if (rawRecord.offset() < startOffset) {
            // If the start offset is not reached even after skipping the records for 10 seconds
            // then the processing is stopped with a backoff to give the Kakfa server some time
            // catch up.
            if (sw.elapsed().getSeconds() > 10L) {
              LOG.error(
                  "The expected offset ({}) was not reached even after"
                      + " skipping consumed records for 10 seconds. The offset we could"
                      + " reach was {}. The processing of this bundle will be attempted"
                      + " at a later time.",
                  expectedOffset,
                  rawRecord.offset());
              return ProcessContinuation.resume()
                  .withResumeDelay(org.joda.time.Duration.standardSeconds(10L));
            }
            skippedRecords++;
            continue;
          }
          if (skippedRecords > 0L) {
            LOG.warn(
                "{} records were skipped due to seek returning an"
                    + " earlier position than requested position of {}",
                skippedRecords,
                expectedOffset);
            skippedRecords = 0L;
          }
          if (!tracker.tryClaim(rawRecord.offset())) {
            return ProcessContinuation.stop();
          }
          try {
            KafkaRecord<K, V> kafkaRecord =
                new KafkaRecord<>(
                    rawRecord.topic(),
                    rawRecord.partition(),
                    rawRecord.offset(),
                    ConsumerSpEL.getRecordTimestamp(rawRecord),
                    ConsumerSpEL.getRecordTimestampType(rawRecord),
                    ConsumerSpEL.hasHeaders() ? rawRecord.headers() : null,
                    ConsumerSpEL.deserializeKey(keyDeserializerInstance, rawRecord),
                    ConsumerSpEL.deserializeValue(valueDeserializerInstance, rawRecord));
            int recordSize =
                (rawRecord.key() == null ? 0 : rawRecord.key().length)
                    + (rawRecord.value() == null ? 0 : rawRecord.value().length);
            avgRecordSizeCache
                .getUnchecked(kafkaSourceDescriptor)
                .update(recordSize, rawRecord.offset() - expectedOffset);
            rawSizes.update(recordSize);
            expectedOffset = rawRecord.offset() + 1;
            Instant outputTimestamp;
            // The outputTimestamp and watermark will be computed by timestampPolicy, where the
            // WatermarkEstimator should be a manual one.
            if (timestampPolicy != null) {
              TimestampPolicyContext context =
                  updateWatermarkManually(timestampPolicy, watermarkEstimator, tracker);
              outputTimestamp = timestampPolicy.getTimestampForRecord(context, kafkaRecord);
            } else {
              Preconditions.checkStateNotNull(this.extractOutputTimestampFn);
              outputTimestamp = extractOutputTimestampFn.apply(kafkaRecord);
            }
            receiver
                .get(recordTag)
                .outputWithTimestamp(KV.of(kafkaSourceDescriptor, kafkaRecord), outputTimestamp);
          } catch (SerializationException e) {
            // This exception should only occur during the key and value deserialization when
            // creating the Kafka Record
            badRecordRouter.route(
                receiver,
                rawRecord,
                null,
                e,
                "Failure deserializing Key or Value of Kakfa record reading from Kafka");
            if (timestampPolicy != null) {
              updateWatermarkManually(timestampPolicy, watermarkEstimator, tracker);
            }
          }
        }

        backlogBytes.set(
            (long)
                (((HasProgress) tracker).getProgress().getWorkRemaining()
                    * avgRecordSize.estimateRecordByteSizeToOffsetCountRatio()));
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    return ProcessContinuation.resume();
  }

  private TimestampPolicyContext updateWatermarkManually(
      TimestampPolicy<K, V> timestampPolicy,
      WatermarkEstimator<Instant> watermarkEstimator,
      RestrictionTracker<OffsetRange, Long> tracker) {
    checkState(watermarkEstimator instanceof ManualWatermarkEstimator);
    TimestampPolicyContext context =
        new TimestampPolicyContext(
            (long) ((HasProgress) tracker).getProgress().getWorkRemaining(), Instant.now());
    ((ManualWatermarkEstimator<Instant>) watermarkEstimator)
        .setWatermark(ensureTimestampWithinBounds(timestampPolicy.getWatermark(context)));
    return context;
  }

  @GetRestrictionCoder
  public Coder<OffsetRange> restrictionCoder() {
    return new OffsetRange.Coder();
  }

  @Setup
  @EnsuresNonNull({"avgRecordSizeCache", "consumerExecutionContextCache"})
  public void setup() throws Exception {
    // Start to track record size and offset gap per bundle.
    this.avgRecordSizeCache =
        SharedStateHolder.AVG_RECORD_SIZE_CACHE.computeIfAbsent(
            fnId,
            k -> {
              return CacheBuilder.newBuilder()
                  .maximumSize(1000L)
                  .build(
                      new CacheLoader<KafkaSourceDescriptor, AverageRecordSize>() {
                        @Override
                        public AverageRecordSize load(KafkaSourceDescriptor kafkaSourceDescriptor)
                            throws Exception {
                          return new AverageRecordSize();
                        }
                      });
            });
    this.consumerExecutionContextCache =
        SharedStateHolder.CONSUMER_EXECUTION_CONTEXT_CACHE.computeIfAbsent(
            fnId,
            k -> {
              return CacheBuilder.newBuilder()
                  .weakValues()
                  .removalListener(
                      new RemovalListener<
                          Optional<ImmutableSet<String>>, ConsumerExecutionContext>() {
                        @Override
                        public void onRemoval(
                            RemovalNotification<
                                    Optional<ImmutableSet<String>>, ConsumerExecutionContext>
                                notification) {
                          final @Nullable ConsumerExecutionContext value = notification.getValue();
                          if (notification.getCause() != RemovalCause.COLLECTED && value != null) {
                            value.stopAsync();
                          }
                        }
                      })
                  .build(
                      new CacheLoader<Optional<ImmutableSet<String>>, ConsumerExecutionContext>() {
                        @Override
                        public ConsumerExecutionContext load(
                            Optional<ImmutableSet<String>> optionalBootstrapServers)
                            throws Exception {
                          final Map<String, Object> consumerConfig =
                              new HashMap<>(ReadFromKafkaDoFn.this.consumerConfig);
                          ImmutableSet<String> bootstrapServers;
                          if (optionalBootstrapServers.isPresent()
                              && (bootstrapServers = optionalBootstrapServers.get()).size() > 0) {
                            consumerConfig.put(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                COMMA_JOINER.join(bootstrapServers));
                          }
                          checkState(
                              consumerConfig.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
                          final ConsumerExecutionContext context =
                              new ConsumerExecutionContext(
                                  ReadFromKafkaDoFn.this.consumerFactoryFn.apply(consumerConfig));
                          context.startAsync();
                          return context;
                        }
                      });
            });
    keyDeserializerInstance = keyDeserializerProvider.getDeserializer(consumerConfig, true);
    valueDeserializerInstance = valueDeserializerProvider.getDeserializer(consumerConfig, false);
    if (checkStopReadingFn != null) {
      checkStopReadingFn.setup();
    }
  }

  @Teardown
  @RequiresNonNull({"avgRecordSizeCache", "consumerExecutionContextCache"})
  public void teardown() throws Exception {
    final LoadingCache<KafkaSourceDescriptor, AverageRecordSize> avgRecordSizeCache =
        this.avgRecordSizeCache;
    final LoadingCache<Optional<ImmutableSet<String>>, ConsumerExecutionContext>
        consumerExecutionContextCache = this.consumerExecutionContextCache;
    try {
      if (valueDeserializerInstance != null) {
        Closeables.close(valueDeserializerInstance, true);
        valueDeserializerInstance = null;
      }
      if (keyDeserializerInstance != null) {
        Closeables.close(keyDeserializerInstance, true);
        keyDeserializerInstance = null;
      }
    } catch (Exception anyException) {
      LOG.warn("Fail to close resource during finishing bundle.", anyException);
    }
    if (checkStopReadingFn != null) {
      checkStopReadingFn.teardown();
    }

    // Allow the cache to perform clean up tasks when this instance is about to be deleted.
    avgRecordSizeCache.cleanUp();
    consumerExecutionContextCache.cleanUp();
  }

  // TODO: Collapse the two moving average trackers into a single accumulator using a single Guava
  // AtomicDouble. Note that this requires that a single thread will call update and that while get
  // may be called by multiple threads the method must only load the accumulator itself.
  @ThreadSafe
  private static class AverageRecordSize {
    @GuardedBy("this")
    private MovingAvg avgRecordSize;

    @GuardedBy("this")
    private MovingAvg avgRecordGap;

    public AverageRecordSize() {
      this.avgRecordSize = new MovingAvg();
      this.avgRecordGap = new MovingAvg();
    }

    public synchronized void update(int recordSize, long gap) {
      avgRecordSize.update(recordSize);
      avgRecordGap.update(gap);
    }

    public double estimateRecordByteSizeToOffsetCountRatio() {
      double avgRecordSize;
      double avgRecordGap;

      synchronized (this) {
        avgRecordSize = this.avgRecordSize.get();
        avgRecordGap = this.avgRecordGap.get();
      }

      // The offset increases between records in a batch fetched from a compacted topic may be
      // greater than 1. Compacted topics only store records with the greatest offset per key per
      // partition, the records in between are deleted and will not be observed by a consumer.
      // The observed gap between offsets is used to estimate the number of records that are likely
      // to be observed for the provided number of records.
      return avgRecordSize / (1 + avgRecordGap);
    }
  }

  private static Instant ensureTimestampWithinBounds(Instant timestamp) {
    if (timestamp.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    } else if (timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
    return timestamp;
  }
}
