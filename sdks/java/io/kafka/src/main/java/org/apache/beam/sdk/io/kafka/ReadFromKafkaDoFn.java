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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.beam.sdk.options.PipelineOptions;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.LongMath;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
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
    this.consumerPollingTimeout =
        Duration.ofSeconds(
            transform.getConsumerPollingTimeout() > 0
                ? transform.getConsumerPollingTimeout()
                : DEFAULT_KAFKA_POLL_TIMEOUT);
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
            Long, LoadingCache<Optional<ImmutableSet<String>>, ConcurrentConsumer<byte[], byte[]>>>
        CONSUMER_EXECUTION_CONTEXT_CACHE = new ConcurrentHashMap<>();
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
  private transient @Nullable ConcurrentConsumer<byte[], byte[]> consumerExecutionContextInstance =
      null;

  private transient @MonotonicNonNull LoadingCache<KafkaSourceDescriptor, AverageRecordSize>
      avgRecordSizeCache;

  private transient @MonotonicNonNull LoadingCache<
          Optional<ImmutableSet<String>>, ConcurrentConsumer<byte[], byte[]>>
      consumerExecutionContextCache;

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
      implements GrowableOffsetRangeTracker.RangeEndEstimator {

    private final KafkaSourceDescriptor sourceDescriptor;
    private final LoadingCache<Optional<ImmutableSet<String>>, ConcurrentConsumer<byte[], byte[]>>
        consumerExecutionContextCache;
    private @MonotonicNonNull ConcurrentConsumer<byte[], byte[]> consumerExecutionContextInstance;

    KafkaLatestOffsetEstimator(
        final KafkaSourceDescriptor sourceDescriptor,
        final LoadingCache<Optional<ImmutableSet<String>>, ConcurrentConsumer<byte[], byte[]>>
            consumerExecutionContextCache) {
      this.sourceDescriptor = sourceDescriptor;
      this.consumerExecutionContextCache = consumerExecutionContextCache;
      this.consumerExecutionContextInstance = null;
    }

    @Override
    public long estimate() {
      Optional<ImmutableSet<String>> consumerExecutionContextKey =
          Optional.ofNullable(this.sourceDescriptor.getBootStrapServers())
              .map(ImmutableSet::copyOf);
      ConcurrentConsumer<byte[], byte[]> consumerExecutionContext;
      try {
        consumerExecutionContext =
            this.consumerExecutionContextCache.get(consumerExecutionContextKey);
      } catch (ExecutionException ex) {
        return -1L;
      }
      this.consumerExecutionContextInstance = consumerExecutionContext;

      final long position =
          this.consumerExecutionContextInstance.position(this.sourceDescriptor.getTopicPartition());
      final long lag =
          this.consumerExecutionContextInstance.currentLagOrMaxLag(
              this.sourceDescriptor.getTopicPartition());

      return LongMath.saturatedAdd(position, lag);
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
    LoadingCache<Optional<ImmutableSet<String>>, ConcurrentConsumer<byte[], byte[]>>
        consumerExecutionContextCache = checkNotNull(this.consumerExecutionContextCache);
    ConcurrentConsumer<byte[], byte[]> consumerExecutionContext =
        consumerExecutionContextCache.get(consumerExecutionContextKey);
    this.consumerExecutionContextInstance = consumerExecutionContext;

    final long startOffset;
    final long endOffset;
    try {
      final @Nullable Long startReadOffset = kafkaSourceDescriptor.getStartReadOffset();
      final @Nullable Instant startReadTime = kafkaSourceDescriptor.getStartReadTime();
      if (startReadOffset != null) {
        startOffset = startReadOffset;
      } else if (startReadTime != null) {
        final @Nullable OffsetAndTimestamp offsetAndTimestamp =
            consumerExecutionContext.initialOffsetForTime(
                kafkaSourceDescriptor.getTopicPartition(), startReadTime.getMillis());
        startOffset = offsetAndTimestamp == null ? 0L : offsetAndTimestamp.offset();
      } else {
        startOffset =
            consumerExecutionContext.initialOffsetForPartition(
                kafkaSourceDescriptor.getTopicPartition());
      }

      final @Nullable Long stopReadOffset = kafkaSourceDescriptor.getStopReadOffset();
      final @Nullable Instant stopReadTime = kafkaSourceDescriptor.getStopReadTime();
      if (stopReadOffset != null) {
        endOffset = stopReadOffset;
      } else if (stopReadTime != null) {
        final @Nullable OffsetAndTimestamp offsetAndTimestamp =
            consumerExecutionContext.initialOffsetForTime(
                kafkaSourceDescriptor.getTopicPartition(), stopReadTime.getMillis());
        endOffset = offsetAndTimestamp == null ? Long.MAX_VALUE : offsetAndTimestamp.offset();
      } else {
        endOffset = Long.MAX_VALUE;
      }
    } catch (Exception e) {
      LOG.error("Failed to set initial restriction", e);
      if (consumerExecutionContext.isClosed()) {
        LOG.warn("Invalidating closed consumer.");
        consumerExecutionContextCache.invalidate(consumerExecutionContextKey);
      }
      throw e;
    }

    final OffsetRange initialRestriction = new OffsetRange(startOffset, endOffset);

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
    final LoadingCache<Optional<ImmutableSet<String>>, ConcurrentConsumer<byte[], byte[]>>
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
    final LoadingCache<Optional<ImmutableSet<String>>, ConcurrentConsumer<byte[], byte[]>>
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
    ConcurrentConsumer<byte[], byte[]> consumerExecutionContext =
        consumerExecutionContextCache.get(consumerExecutionContextKey);
    this.consumerExecutionContextInstance = consumerExecutionContext;

    // If there is a timestampPolicyFactory, create the TimestampPolicy for current
    // TopicPartition.
    TimestampPolicy<K, V> timestampPolicy = null;
    if (timestampPolicyFactory != null) {
      timestampPolicy =
          timestampPolicyFactory.createTimestampPolicy(
              topicPartition, Optional.ofNullable(watermarkEstimator.currentWatermark()));
    }

    long startOffset = tracker.currentRestriction().getFrom();
    long expectedOffset = startOffset;
    long skippedRecords = 0L;
    final Stopwatch sw = Stopwatch.createStarted();

    try {
      consumerExecutionContext.assignAndSeek(
          kafkaSourceDescriptor.getTopicPartition(), startOffset);

      while (!consumerExecutionContext.isClosed()) {
        for (ConsumerRecord<byte[], byte[]> rawRecord :
            consumerExecutionContext.poll(kafkaSourceDescriptor.getTopicPartition())) {
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
      }
    } catch (Exception e) {
      LOG.error("Poll failed", e);
    } finally {
      consumerExecutionContext.unassign(kafkaSourceDescriptor.getTopicPartition());
      if (consumerExecutionContext.isClosed()) {
        LOG.warn("Invalidating closed consumer.");
        consumerExecutionContextCache.invalidate(consumerExecutionContextKey);
      }

      backlogBytes.set(
          (long)
              (((HasProgress) tracker).getProgress().getWorkRemaining()
                  * avgRecordSize.estimateRecordByteSizeToOffsetCountRatio()));

      if (timestampPolicy != null) {
        updateWatermarkManually(timestampPolicy, watermarkEstimator, tracker);
      }
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
  public void setup(final PipelineOptions options) throws Exception {
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
                          Optional<ImmutableSet<String>>, ConcurrentConsumer<byte[], byte[]>>() {
                        @Override
                        public void onRemoval(
                            RemovalNotification<
                                    Optional<ImmutableSet<String>>,
                                    ConcurrentConsumer<byte[], byte[]>>
                                notification) {
                          final @Nullable ConcurrentConsumer<byte[], byte[]> value =
                              notification.getValue();
                          if (notification.getCause() != RemovalCause.COLLECTED && value != null) {
                            value.close();
                          }
                        }
                      })
                  .build(
                      new CacheLoader<
                          Optional<ImmutableSet<String>>, ConcurrentConsumer<byte[], byte[]>>() {
                        @Override
                        public ConcurrentConsumer<byte[], byte[]> load(
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
                          return new ConcurrentConsumer<>(
                              ReadFromKafkaDoFn.this.consumerFactoryFn.apply(consumerConfig),
                              ReadFromKafkaDoFn.this.consumerPollingTimeout);
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
    final LoadingCache<Optional<ImmutableSet<String>>, ConcurrentConsumer<byte[], byte[]>>
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
