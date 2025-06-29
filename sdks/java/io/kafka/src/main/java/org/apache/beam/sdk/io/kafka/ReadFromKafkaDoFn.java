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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
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
import org.apache.beam.sdk.util.MemoizingPerInstantiationSerializableSupplier;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.SerializableSupplier;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closeables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.checkerframework.checker.nullness.qual.Nullable;
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
 * when {@link ReadFromKafkaDoFn} performs a {@link Consumer#poll(Duration)}. In that case, the
 * {@link ReadFromKafkaDoFn} will still output the fetched records.
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
    final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn =
        transform.getConsumerFactoryFn();
    this.consumerConfig = transform.getConsumerConfig();
    this.keyDeserializerProvider =
        Preconditions.checkArgumentNotNull(transform.getKeyDeserializerProvider());
    this.valueDeserializerProvider =
        Preconditions.checkArgumentNotNull(transform.getValueDeserializerProvider());
    this.extractOutputTimestampFn = transform.getExtractOutputTimestampFn();
    this.createWatermarkEstimatorFn = transform.getCreateWatermarkEstimatorFn();
    this.timestampPolicyFactory = transform.getTimestampPolicyFactory();
    this.checkStopReadingFn = transform.getCheckStopReadingFn();
    this.badRecordRouter = transform.getBadRecordRouter();
    this.recordTag = recordTag;
    this.avgRecordSizeCacheSupplier =
        new MemoizingPerInstantiationSerializableSupplier<>(
            () ->
                CacheBuilder.newBuilder()
                    .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                    .weakValues()
                    .build(
                        new CacheLoader<KafkaSourceDescriptor, MovingAvg>() {
                          @Override
                          public MovingAvg load(KafkaSourceDescriptor kafkaSourceDescriptor)
                              throws Exception {
                            return new MovingAvg();
                          }
                        }));
    this.latestOffsetEstimatorCacheSupplier =
        new MemoizingPerInstantiationSerializableSupplier<>(
            () ->
                CacheBuilder.newBuilder()
                    .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                    .weakValues()
                    .removalListener(
                        (RemovalNotification<KafkaSourceDescriptor, KafkaLatestOffsetEstimator>
                                notification) -> {
                          final @Nullable KafkaLatestOffsetEstimator value;
                          if (notification.getCause() == RemovalCause.COLLECTED
                              && (value = notification.getValue()) != null) {
                            value.close();
                          }
                        })
                    .build(
                        new CacheLoader<KafkaSourceDescriptor, KafkaLatestOffsetEstimator>() {
                          @Override
                          public KafkaLatestOffsetEstimator load(
                              final KafkaSourceDescriptor sourceDescriptor) {
                            LOG.info(
                                "Creating Kafka consumer for offset estimation for {}",
                                sourceDescriptor);
                            final Map<String, Object> config =
                                KafkaIOUtils.overrideBootstrapServersConfig(
                                    consumerConfig, sourceDescriptor);
                            final Consumer<byte[], byte[]> consumer =
                                consumerFactoryFn.apply(config);
                            return new KafkaLatestOffsetEstimator(
                                consumer, sourceDescriptor.getTopicPartition());
                          }
                        }));
    this.pollConsumerCacheSupplier =
        new MemoizingPerInstantiationSerializableSupplier<>(
            () ->
                CacheBuilder.newBuilder()
                    .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                    .weakValues()
                    .removalListener(
                        (RemovalNotification<KafkaSourceDescriptor, Consumer<byte[], byte[]>>
                                notification) -> {
                          final @Nullable Consumer<byte[], byte[]> value;
                          if (notification.getCause() == RemovalCause.COLLECTED
                              && (value = notification.getValue()) != null) {
                            value.close();
                          }
                        })
                    .build(
                        new CacheLoader<KafkaSourceDescriptor, Consumer<byte[], byte[]>>() {
                          @Override
                          public Consumer<byte[], byte[]> load(
                              KafkaSourceDescriptor sourceDescriptor) {
                            LOG.info(
                                "Creating Kafka consumer for restriction processing for {}",
                                sourceDescriptor);
                            final Map<String, Object> config =
                                KafkaIOUtils.overrideBootstrapServersConfig(
                                    consumerConfig, sourceDescriptor);
                            final Consumer<byte[], byte[]> consumer =
                                consumerFactoryFn.apply(config);
                            consumer.assign(
                                Collections.singleton(sourceDescriptor.getTopicPartition()));
                            return consumer;
                          }
                        }));
    this.consumerPollingTimeout =
        Duration.ofSeconds(
            transform.getConsumerPollingTimeout() > 0
                ? transform.getConsumerPollingTimeout()
                : DEFAULT_KAFKA_POLL_TIMEOUT);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ReadFromKafkaDoFn.class);

  private static final Joiner COMMA_JOINER = Joiner.on(',');

  private final @Nullable CheckStopReadingFn checkStopReadingFn;

  private final @Nullable SerializableFunction<KafkaRecord<K, V>, Instant> extractOutputTimestampFn;
  private final @Nullable SerializableFunction<Instant, WatermarkEstimator<Instant>>
      createWatermarkEstimatorFn;
  private final @Nullable TimestampPolicyFactory<K, V> timestampPolicyFactory;

  private final BadRecordRouter badRecordRouter;

  private final TupleTag<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> recordTag;

  private final SerializableSupplier<LoadingCache<KafkaSourceDescriptor, MovingAvg>>
      avgRecordSizeCacheSupplier;

  private final SerializableSupplier<
          LoadingCache<KafkaSourceDescriptor, KafkaLatestOffsetEstimator>>
      latestOffsetEstimatorCacheSupplier;

  private final SerializableSupplier<LoadingCache<KafkaSourceDescriptor, Consumer<byte[], byte[]>>>
      pollConsumerCacheSupplier;

  // Valid between bundle start and bundle finish.
  private transient @Nullable Deserializer<K> keyDeserializerInstance = null;
  private transient @Nullable Deserializer<V> valueDeserializerInstance = null;
  private static final long DEFAULT_KAFKA_POLL_TIMEOUT = 2L;
  @VisibleForTesting final Duration consumerPollingTimeout;
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
      implements GrowableOffsetRangeTracker.RangeEndEstimator, Closeable {
    private static final AtomicReferenceFieldUpdater<KafkaLatestOffsetEstimator, @Nullable Runnable>
        CURRENT_REFRESH_TASK =
            (AtomicReferenceFieldUpdater<KafkaLatestOffsetEstimator, @Nullable Runnable>)
                AtomicReferenceFieldUpdater.newUpdater(
                    KafkaLatestOffsetEstimator.class, Runnable.class, "currentRefreshTask");
    private final Executor executor;
    private final Consumer<byte[], byte[]> offsetConsumer;
    private final TopicPartition topicPartition;
    // TODO(sjvanrossum): Use VarHandle.setOpaque/getOpaque when Java 8 support is dropped
    private long lastRefreshEndOffset;
    // TODO(sjvanrossum): Use VarHandle.setOpaque/getOpaque when Java 8 support is dropped
    private long nextRefreshNanos;
    private volatile @Nullable Runnable currentRefreshTask;

    /*
    Periodic refreshes of lastRefreshEndOffset and nextRefreshNanos are guarded by the volatile
    field currentRefreshTask. This guard's correctness depends on specific ordering of reads and
    writes (loads and stores).

    To validate the behavior of this guard please read the Java Memory Model (JMM) specification.
    For the current context consider the following oversimplifications of the JMM:
      - Writes to a non-volatile long or double field are non-atomic.
      - Writes to a non-volatile field may never become visible to another core.
      - Writes to a volatile field are atomic and will become visible to another core.
      - Lazy writes to a volatile field are atomic and will become visible to another core for
        reads of that volatile field.
      - Writes preceeding writes or lazy writes to a volatile field are visible to another core.

    In short, the contents of this class' guarded fields are visible if the guard field is (lazily)
    written last and read first. The contents of the volatile guard may be stale in comparison to
    the contents of the guarded fields. For this method it is important that no more than one
    thread will schedule a refresh task. Using currentRefreshTask as the guard field ensures that
    lastRefreshEndOffset and nextRefreshNanos are at least as stale as currentRefreshTask.
    It's fine if lastRefreshEndOffset and nextRefreshNanos are less stale than currentRefreshTask.

    Removing currentRefreshTask by guarding on nextRefreshNanos is possible, but executing
    currentRefreshTask == null is practically free (measured in cycles) compared to executing
    nextRefreshNanos < System.nanoTime() (measured in nanoseconds).

    Note that the JMM specifies that writes to a long or double are not guaranteed to be atomic.
    In practice, every 64-bit JVM will treat them as atomic (and the JMM encourages this).
    There's no way to force atomicity without visibility in Java 8 so atomicity guards have been
    omitted. Java 9 introduces VarHandle with "opaque" getters/setters which do provide this.
    */

    KafkaLatestOffsetEstimator(
        final Consumer<byte[], byte[]> offsetConsumer, final TopicPartition topicPartition) {
      this.executor = Executors.newSingleThreadExecutor();
      this.offsetConsumer = offsetConsumer;
      this.topicPartition = topicPartition;
      this.lastRefreshEndOffset = -1L;
      this.nextRefreshNanos = Long.MIN_VALUE;
      this.currentRefreshTask = null;
    }

    @Override
    public long estimate() {
      final @Nullable Runnable task = currentRefreshTask; // volatile load (acquire)

      final long currentNanos;
      if (task == null
          && nextRefreshNanos < (currentNanos = System.nanoTime()) // normal load
          && CURRENT_REFRESH_TASK.compareAndSet(this, null, this::refresh)) { // volatile load/store
        try {
          executor.execute(this::refresh);
        } catch (RejectedExecutionException ex) {
          LOG.error("Execution of end offset refresh rejected for {}", topicPartition, ex);
          nextRefreshNanos = currentNanos + TimeUnit.SECONDS.toNanos(1); // normal store
          CURRENT_REFRESH_TASK.lazySet(this, null); // ordered store (release)
        }
      }

      return lastRefreshEndOffset; // normal load
    }

    @Override
    public void close() {
      offsetConsumer.close();
    }

    private void refresh() {
      try {
        @Nullable
        Long endOffset =
            offsetConsumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
        if (endOffset == null) {
          LOG.warn("No end offset found for partition {}.", topicPartition);
        } else {
          lastRefreshEndOffset = endOffset; // normal store
        }
        nextRefreshNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(1); // normal store
      } finally {
        CURRENT_REFRESH_TASK.lazySet(this, null); // ordered store (release)
      }
    }
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element KafkaSourceDescriptor kafkaSourceDescriptor) {
    final Consumer<byte[], byte[]> consumer =
        pollConsumerCacheSupplier.get().getUnchecked(kafkaSourceDescriptor);

    final long startOffset;
    final long stopOffset;

    final @Nullable Long startReadOffset = kafkaSourceDescriptor.getStartReadOffset();
    final @Nullable Instant startReadTime = kafkaSourceDescriptor.getStartReadTime();
    if (startReadOffset != null) {
      startOffset = startReadOffset;
    } else if (startReadTime != null) {
      startOffset =
          Preconditions.checkStateNotNull(
                  consumer
                      .offsetsForTimes(
                          Collections.singletonMap(
                              kafkaSourceDescriptor.getTopicPartition(), startReadTime.getMillis()))
                      .get(kafkaSourceDescriptor.getTopicPartition()))
              .offset();
    } else {
      startOffset = consumer.position(kafkaSourceDescriptor.getTopicPartition());
    }

    final @Nullable Long stopReadOffset = kafkaSourceDescriptor.getStopReadOffset();
    final @Nullable Instant stopReadTime = kafkaSourceDescriptor.getStopReadTime();
    if (stopReadOffset != null) {
      stopOffset = stopReadOffset;
    } else if (stopReadTime != null) {
      stopOffset =
          Preconditions.checkStateNotNull(
                  consumer
                      .offsetsForTimes(
                          Collections.singletonMap(
                              kafkaSourceDescriptor.getTopicPartition(), stopReadTime.getMillis()))
                      .get(kafkaSourceDescriptor.getTopicPartition()))
              .offset();
    } else {
      stopOffset = Long.MAX_VALUE;
    }

    final OffsetRange initialRestriction = new OffsetRange(startOffset, stopOffset);
    Lineage.getSources()
        .add(
            "kafka",
            ImmutableList.of(
                Optional.ofNullable(
                        KafkaIOUtils.overrideBootstrapServersConfig(
                                consumerConfig, kafkaSourceDescriptor)
                            .get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
                    .map(
                        value ->
                            (@Nullable List<String>)
                                ConfigDef.parseType(
                                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    value,
                                    ConfigDef.Type.LIST))
                    .map(ImmutableSet::copyOf)
                    .map(COMMA_JOINER::join)
                    .get(),
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
  public double getSize(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor, @Restriction OffsetRange offsetRange) {
    // If present, estimates the record size to offset gap ratio. Compacted topics may hold less
    // records than the estimated offset range due to record deletion within a partition.
    final @Nullable MovingAvg avgRecordSize =
        avgRecordSizeCacheSupplier.get().getIfPresent(kafkaSourceDescriptor);
    // The tracker estimates the offset range by subtracting the last claimed position from the
    // currently observed end offset for the partition belonging to this split.
    final double estimatedOffsetRange =
        restrictionTracker(kafkaSourceDescriptor, offsetRange).getProgress().getWorkRemaining();

    // Before processing elements, we don't have a good estimated size of records.
    // When processing elements, a moving average estimates the size of records.
    // Return the estimated offset range scaled by the estimated size if present.
    return avgRecordSize == null
        ? estimatedOffsetRange
        : estimatedOffsetRange * avgRecordSize.get();
  }

  @NewTracker
  public OffsetRangeTracker restrictionTracker(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor, @Restriction OffsetRange restriction) {
    if (restriction.getTo() < Long.MAX_VALUE) {
      return new OffsetRangeTracker(restriction);
    }

    // OffsetEstimators are cached for each topic-partition because they hold a stateful connection,
    // so we want to minimize the amount of connections that we start and track with Kafka. Another
    // point is that it has a memoized backlog, and this should make that more reusable estimations.
    return new GrowableOffsetRangeTracker(
        restriction.getFrom(),
        latestOffsetEstimatorCacheSupplier.get().getUnchecked(kafkaSourceDescriptor));
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator<Instant> watermarkEstimator,
      MultiOutputReceiver receiver)
      throws Exception {
    final MovingAvg avgRecordSize = avgRecordSizeCacheSupplier.get().get(kafkaSourceDescriptor);
    final KafkaLatestOffsetEstimator latestOffsetEstimator =
        latestOffsetEstimatorCacheSupplier.get().get(kafkaSourceDescriptor);
    final Consumer<byte[], byte[]> consumer =
        pollConsumerCacheSupplier.get().get(kafkaSourceDescriptor);
    final Deserializer<K> keyDeserializerInstance =
        Preconditions.checkStateNotNull(this.keyDeserializerInstance);
    final Deserializer<V> valueDeserializerInstance =
        Preconditions.checkStateNotNull(this.valueDeserializerInstance);
    final TopicPartition topicPartition = kafkaSourceDescriptor.getTopicPartition();

    // TODO: Metrics should be reported per split instead of partition, add bootstrap server hash?
    final Distribution rawSizes =
        Metrics.distribution(METRIC_NAMESPACE, RAW_SIZE_METRIC_PREFIX + topicPartition.toString());
    final Gauge backlogBytes =
        Metrics.gauge(
            METRIC_NAMESPACE, RAW_SIZE_METRIC_PREFIX + "backlogBytes_" + topicPartition.toString());

    // Stop processing current TopicPartition when it's time to stop.
    if (checkStopReadingFn != null && checkStopReadingFn.apply(topicPartition)) {
      // Attempt to claim the last element in the restriction, such that the restriction tracker
      // doesn't throw an exception when checkDone is called
      tracker.tryClaim(tracker.currentRestriction().getTo() - 1);
      return ProcessContinuation.stop();
    }
    // If there is a timestampPolicyFactory, create the TimestampPolicy for current
    // TopicPartition.
    TimestampPolicy<K, V> timestampPolicy = null;
    if (timestampPolicyFactory != null) {
      timestampPolicy =
          timestampPolicyFactory.createTimestampPolicy(
              topicPartition, Optional.ofNullable(watermarkEstimator.currentWatermark()));
    }

    long expectedOffset = tracker.currentRestriction().getFrom();
    consumer.resume(Collections.singleton(topicPartition));
    consumer.seek(topicPartition, expectedOffset);
    final Stopwatch pollTimer = Stopwatch.createUnstarted();

    final KafkaMetrics kafkaMetrics = KafkaSinkMetrics.kafkaMetrics();
    try {
      while (true) {
        // TODO: Remove this timer and use the existing fetch-latency-avg	metric.
        // A consumer will often have prefetches waiting to be returned immediately in which case
        // this timer may contribute more latency than it measures.
        // See https://shipilev.net/blog/2014/nanotrusting-nanotime/ for more information.
        pollTimer.reset().start();
        // Fetch the next records.
        final ConsumerRecords<byte[], byte[]> rawRecords =
            consumer.poll(this.consumerPollingTimeout);
        kafkaMetrics.updateSuccessfulRpcMetrics(topicPartition.topic(), pollTimer.elapsed());

        // No progress when the polling timeout expired.
        // Self-checkpoint and move to process the next element.
        if (rawRecords == ConsumerRecords.<byte[], byte[]>empty()) {
          consumer.pause(Collections.singleton(topicPartition));

          if (!topicPartitionExists(
              kafkaSourceDescriptor.getTopicPartition(),
              consumer.partitionsFor(kafkaSourceDescriptor.getTopic()))) {
            return ProcessContinuation.stop();
          }
          if (timestampPolicy != null) {
            updateWatermarkManually(timestampPolicy, watermarkEstimator, tracker);
          }
          return ProcessContinuation.resume();
        }

        // Visible progress within the consumer polling timeout.
        // Partially or fully claim and process records in this batch.
        for (ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
          if (!tracker.tryClaim(rawRecord.offset())) {
            consumer.seek(topicPartition, rawRecord.offset());
            consumer.pause(Collections.singleton(topicPartition));

            return ProcessContinuation.stop();
          }
          expectedOffset = rawRecord.offset() + 1;
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
            avgRecordSize.update(recordSize);
            rawSizes.update(recordSize);
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

        // Non-visible progress within the consumer polling timeout.
        // Claim up to the current position.
        if (expectedOffset < (expectedOffset = consumer.position(topicPartition))) {
          if (!tracker.tryClaim(expectedOffset - 1)) {
            consumer.seek(topicPartition, expectedOffset - 1);
            consumer.pause(Collections.singleton(topicPartition));

            return ProcessContinuation.stop();
          }
          if (timestampPolicy != null) {
            updateWatermarkManually(timestampPolicy, watermarkEstimator, tracker);
          }
        }

        final long estimatedBacklogBytes =
            (long)
                (BigDecimal.valueOf(latestOffsetEstimator.estimate())
                        .subtract(BigDecimal.valueOf(expectedOffset), MathContext.DECIMAL128)
                        .doubleValue()
                    * avgRecordSize.get());
        backlogBytes.set(estimatedBacklogBytes);
        kafkaMetrics.updateBacklogBytes(
            kafkaSourceDescriptor.getTopic(),
            kafkaSourceDescriptor.getPartition(),
            estimatedBacklogBytes);
      }
    } finally {
      kafkaMetrics.flushBufferedMetrics();
    }
  }

  private boolean topicPartitionExists(
      TopicPartition topicPartition, List<PartitionInfo> partitionInfos) {
    // Check if the current TopicPartition still exists.
    return partitionInfos.stream()
        .anyMatch(partitionInfo -> partitionInfo.partition() == (topicPartition.partition()));
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
  public void setup() throws Exception {
    keyDeserializerInstance = keyDeserializerProvider.getDeserializer(consumerConfig, true);
    valueDeserializerInstance = valueDeserializerProvider.getDeserializer(consumerConfig, false);
    if (checkStopReadingFn != null) {
      checkStopReadingFn.setup();
    }
  }

  @Teardown
  public void teardown() throws Exception {
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
    avgRecordSizeCacheSupplier.get().cleanUp();
    latestOffsetEstimatorCacheSupplier.get().cleanUp();
    pollConsumerCacheSupplier.get().cleanUp();
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
