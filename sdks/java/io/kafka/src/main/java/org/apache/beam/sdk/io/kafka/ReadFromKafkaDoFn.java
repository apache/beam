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

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closeables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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
    this.offsetConsumerConfig = transform.getOffsetConsumerConfig();
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

    private static final Map<Long, LoadingCache<KafkaSourceDescriptor, KafkaLatestOffsetEstimator>>
        OFFSET_ESTIMATOR_CACHE = new ConcurrentHashMap<>();
    private static final Map<Long, LoadingCache<KafkaSourceDescriptor, AverageRecordSize>>
        AVG_RECORD_SIZE_CACHE = new ConcurrentHashMap<>();
  }

  private static final AtomicLong FN_ID = new AtomicLong();

  // A unique identifier for the instance. Generally unique unless the ID generator overflows.
  private final long fnId = FN_ID.getAndIncrement();

  private final @Nullable Map<String, Object> offsetConsumerConfig;

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
  private transient @Nullable LoadingCache<KafkaSourceDescriptor, KafkaLatestOffsetEstimator>
      offsetEstimatorCache;

  private transient @Nullable LoadingCache<KafkaSourceDescriptor, AverageRecordSize>
      avgRecordSizeCache;
  private static final long DEFAULT_KAFKA_POLL_TIMEOUT = 2L;
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

    private final Consumer<byte[], byte[]> offsetConsumer;
    private final TopicPartition topicPartition;
    private final Supplier<Long> memoizedBacklog;

    KafkaLatestOffsetEstimator(
        Consumer<byte[], byte[]> offsetConsumer, TopicPartition topicPartition) {
      this.offsetConsumer = offsetConsumer;
      this.topicPartition = topicPartition;
      memoizedBacklog =
          Suppliers.memoizeWithExpiration(
              () -> {
                synchronized (offsetConsumer) {
                  return Preconditions.checkStateNotNull(
                      offsetConsumer
                          .endOffsets(Collections.singleton(topicPartition))
                          .get(topicPartition),
                      "No end offset found for partition %s.",
                      topicPartition);
                }
              },
              1,
              TimeUnit.SECONDS);
    }

    @Override
    protected void finalize() {
      try {
        Closeables.close(offsetConsumer, true);
        LOG.info("Offset Estimator consumer was closed for {}", topicPartition);
      } catch (Exception anyException) {
        LOG.warn("Failed to close offset consumer for {}", topicPartition);
      }
    }

    @Override
    public long estimate() {
      return memoizedBacklog.get();
    }
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element KafkaSourceDescriptor kafkaSourceDescriptor) {
    Map<String, Object> updatedConsumerConfig =
        overrideBootstrapServersConfig(consumerConfig, kafkaSourceDescriptor);
    TopicPartition partition = kafkaSourceDescriptor.getTopicPartition();
    LOG.info("Creating Kafka consumer for initial restriction for {}", kafkaSourceDescriptor);
    try (Consumer<byte[], byte[]> offsetConsumer = consumerFactoryFn.apply(updatedConsumerConfig)) {
      offsetConsumer.assign(ImmutableList.of(partition));
      long startOffset;
      @Nullable Instant startReadTime = kafkaSourceDescriptor.getStartReadTime();
      if (kafkaSourceDescriptor.getStartReadOffset() != null) {
        startOffset = kafkaSourceDescriptor.getStartReadOffset();
      } else if (startReadTime != null) {
        startOffset = ConsumerSpEL.offsetForTime(offsetConsumer, partition, startReadTime);
      } else {
        startOffset = offsetConsumer.position(partition);
      }

      long endOffset = Long.MAX_VALUE;
      @Nullable Instant stopReadTime = kafkaSourceDescriptor.getStopReadTime();
      if (kafkaSourceDescriptor.getStopReadOffset() != null) {
        endOffset = kafkaSourceDescriptor.getStopReadOffset();
      } else if (stopReadTime != null) {
        endOffset = ConsumerSpEL.offsetForTime(offsetConsumer, partition, stopReadTime);
      }
      new OffsetRange(startOffset, endOffset);
      Lineage.getSources()
          .add(
              "kafka",
              ImmutableList.of(
                  (String) updatedConsumerConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                  MoreObjects.firstNonNull(kafkaSourceDescriptor.getTopic(), partition.topic())));
      return new OffsetRange(startOffset, endOffset);
    }
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
      @Element KafkaSourceDescriptor kafkaSourceDescriptor, @Restriction OffsetRange offsetRange)
      throws ExecutionException {
    // If present, estimates the record size to offset gap ratio. Compacted topics may hold less
    // records than the estimated offset range due to record deletion within a partition.
    final LoadingCache<KafkaSourceDescriptor, AverageRecordSize> avgRecordSizeCache =
        Preconditions.checkStateNotNull(this.avgRecordSizeCache);
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
  public OffsetRangeTracker restrictionTracker(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor, @Restriction OffsetRange restriction)
      throws ExecutionException {
    if (restriction.getTo() < Long.MAX_VALUE) {
      return new OffsetRangeTracker(restriction);
    }

    // OffsetEstimators are cached for each topic-partition because they hold a stateful connection,
    // so we want to minimize the amount of connections that we start and track with Kafka. Another
    // point is that it has a memoized backlog, and this should make that more reusable estimations.
    final LoadingCache<KafkaSourceDescriptor, KafkaLatestOffsetEstimator> offsetEstimatorCache =
        Preconditions.checkStateNotNull(this.offsetEstimatorCache);
    final KafkaLatestOffsetEstimator offsetEstimator =
        offsetEstimatorCache.get(kafkaSourceDescriptor);

    return new GrowableOffsetRangeTracker(restriction.getFrom(), offsetEstimator);
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator<Instant> watermarkEstimator,
      MultiOutputReceiver receiver)
      throws Exception {
    final LoadingCache<KafkaSourceDescriptor, AverageRecordSize> avgRecordSizeCache =
        Preconditions.checkStateNotNull(this.avgRecordSizeCache);
    final LoadingCache<KafkaSourceDescriptor, KafkaLatestOffsetEstimator> offsetEstimatorCache =
        Preconditions.checkStateNotNull(this.offsetEstimatorCache);
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
    Map<String, Object> updatedConsumerConfig =
        overrideBootstrapServersConfig(consumerConfig, kafkaSourceDescriptor);
    // If there is a timestampPolicyFactory, create the TimestampPolicy for current
    // TopicPartition.
    TimestampPolicy<K, V> timestampPolicy = null;
    if (timestampPolicyFactory != null) {
      timestampPolicy =
          timestampPolicyFactory.createTimestampPolicy(
              topicPartition, Optional.ofNullable(watermarkEstimator.currentWatermark()));
    }

    LOG.info("Creating Kafka consumer for process continuation for {}", kafkaSourceDescriptor);
    try (Consumer<byte[], byte[]> consumer = consumerFactoryFn.apply(updatedConsumerConfig)) {
      consumer.assign(ImmutableList.of(kafkaSourceDescriptor.getTopicPartition()));
      long startOffset = tracker.currentRestriction().getFrom();
      long expectedOffset = startOffset;
      consumer.seek(kafkaSourceDescriptor.getTopicPartition(), startOffset);
      ConsumerRecords<byte[], byte[]> rawRecords = ConsumerRecords.empty();
      long skippedRecords = 0L;
      final Stopwatch sw = Stopwatch.createStarted();

      while (true) {
        rawRecords = poll(consumer, kafkaSourceDescriptor.getTopicPartition());
        // When there are no records available for the current TopicPartition, self-checkpoint
        // and move to process the next element.
        if (rawRecords.isEmpty()) {
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
        for (ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
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
                (BigDecimal.valueOf(
                            Preconditions.checkStateNotNull(
                                offsetEstimatorCache.get(kafkaSourceDescriptor).estimate()))
                        .subtract(BigDecimal.valueOf(expectedOffset), MathContext.DECIMAL128)
                        .doubleValue()
                    * avgRecordSize.estimateRecordByteSizeToOffsetCountRatio()));
        KafkaMetrics kafkaResults = KafkaSinkMetrics.kafkaMetrics();
        kafkaResults.updateBacklogBytes(
            kafkaSourceDescriptor.getTopic(),
            kafkaSourceDescriptor.getPartition(),
            (long)
                (BigDecimal.valueOf(
                            Preconditions.checkStateNotNull(
                                offsetEstimatorCache.get(kafkaSourceDescriptor).estimate()))
                        .subtract(BigDecimal.valueOf(expectedOffset), MathContext.DECIMAL128)
                        .doubleValue()
                    * avgRecordSize.estimateRecordByteSizeToOffsetCountRatio()));
        kafkaResults.flushBufferedMetrics();
      }
    }
  }

  private boolean topicPartitionExists(
      TopicPartition topicPartition, List<PartitionInfo> partitionInfos) {
    // Check if the current TopicPartition still exists.
    return partitionInfos.stream()
        .anyMatch(partitionInfo -> partitionInfo.partition() == (topicPartition.partition()));
  }

  // see https://github.com/apache/beam/issues/25962
  private ConsumerRecords<byte[], byte[]> poll(
      Consumer<byte[], byte[]> consumer, TopicPartition topicPartition) {
    final Stopwatch sw = Stopwatch.createStarted();
    long previousPosition = -1;
    java.time.Duration elapsed = java.time.Duration.ZERO;
    java.time.Duration timeout = java.time.Duration.ofSeconds(this.consumerPollingTimeout);
    while (true) {
      final ConsumerRecords<byte[], byte[]> rawRecords = consumer.poll(timeout.minus(elapsed));
      if (!rawRecords.isEmpty()) {
        // return as we have found some entries
        return rawRecords;
      }
      if (previousPosition == (previousPosition = consumer.position(topicPartition))) {
        // there was no progress on the offset/position, which indicates end of stream
        return rawRecords;
      }
      elapsed = sw.elapsed();
      if (elapsed.toMillis() >= timeout.toMillis()) {
        // timeout is over
        LOG.warn(
            "No messages retrieved with polling timeout {} seconds. Consider increasing the consumer polling timeout using withConsumerPollingTimeout method.",
            consumerPollingTimeout);
        return rawRecords;
      }
    }
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
    // Start to track record size and offset gap per bundle.
    avgRecordSizeCache =
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
    keyDeserializerInstance = keyDeserializerProvider.getDeserializer(consumerConfig, true);
    valueDeserializerInstance = valueDeserializerProvider.getDeserializer(consumerConfig, false);
    offsetEstimatorCache =
        SharedStateHolder.OFFSET_ESTIMATOR_CACHE.computeIfAbsent(
            fnId,
            k -> {
              final Map<String, Object> consumerConfig = ImmutableMap.copyOf(this.consumerConfig);
              final @Nullable Map<String, Object> offsetConsumerConfig =
                  this.offsetConsumerConfig == null
                      ? null
                      : ImmutableMap.copyOf(this.offsetConsumerConfig);
              return CacheBuilder.newBuilder()
                  .weakValues()
                  .expireAfterAccess(1, TimeUnit.MINUTES)
                  .build(
                      new CacheLoader<KafkaSourceDescriptor, KafkaLatestOffsetEstimator>() {
                        @Override
                        public KafkaLatestOffsetEstimator load(
                            KafkaSourceDescriptor kafkaSourceDescriptor) throws Exception {
                          LOG.info(
                              "Creating Kafka consumer for offset estimation for {}",
                              kafkaSourceDescriptor);

                          TopicPartition topicPartition = kafkaSourceDescriptor.getTopicPartition();
                          Map<String, Object> updatedConsumerConfig =
                              overrideBootstrapServersConfig(consumerConfig, kafkaSourceDescriptor);
                          Consumer<byte[], byte[]> offsetConsumer =
                              consumerFactoryFn.apply(
                                  KafkaIOUtils.getOffsetConsumerConfig(
                                      "tracker-" + topicPartition,
                                      offsetConsumerConfig,
                                      updatedConsumerConfig));
                          return new KafkaLatestOffsetEstimator(offsetConsumer, topicPartition);
                        }
                      });
            });
    if (checkStopReadingFn != null) {
      checkStopReadingFn.setup();
    }
  }

  @Teardown
  public void teardown() throws Exception {
    final LoadingCache<KafkaSourceDescriptor, AverageRecordSize> avgRecordSizeCache =
        Preconditions.checkStateNotNull(this.avgRecordSizeCache);
    final LoadingCache<KafkaSourceDescriptor, KafkaLatestOffsetEstimator> offsetEstimatorCache =
        Preconditions.checkStateNotNull(this.offsetEstimatorCache);
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
    offsetEstimatorCache.cleanUp();
  }

  private Map<String, Object> overrideBootstrapServersConfig(
      Map<String, Object> currentConfig, KafkaSourceDescriptor description) {
    checkState(
        currentConfig.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
            || description.getBootStrapServers() != null);
    Map<String, Object> config = new HashMap<>(currentConfig);
    if (description.getBootStrapServers() != null && description.getBootStrapServers().size() > 0) {
      config.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          String.join(",", description.getBootStrapServers()));
    }
    return config;
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
