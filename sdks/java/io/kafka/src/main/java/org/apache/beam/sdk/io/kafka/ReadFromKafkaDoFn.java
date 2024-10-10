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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Closeables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * A {@link MovingAvg} is used to track the average size of kafka records.
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

  private static final Supplier<KafkaConsumerPollThreadCache> cache =
      Suppliers.memoize(KafkaConsumerPollThreadCache::new);

  // Valid between bundle start and bundle finish.
  private transient @Nullable Deserializer<K> keyDeserializerInstance = null;
  private transient @Nullable Deserializer<V> valueDeserializerInstance = null;
  private transient @Nullable Map<TopicPartition, KafkaLatestOffsetEstimator> offsetEstimatorCache;

  private static final Supplier<KafkaOffsetConsumerPollThreadCache> offsetConsumerPollThreadCache =
      Suppliers.memoize(KafkaOffsetConsumerPollThreadCache::new);

  private transient @Nullable LoadingCache<TopicPartition, AverageRecordSize> avgRecordSize;
  private static final long DEFAULT_KAFKA_POLL_TIMEOUT = 2L;

  private final HashMap<String, Long> perPartitionBacklogMetrics = new HashMap<>();

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

    private final KafkaOffsetConsumerPollThread offsetTrackerThread;

    public KafkaLatestOffsetEstimator(
        Map<String, Object> fullOffsetConsumerConfig,
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
        KafkaSourceDescriptor kafkaSourceDescriptor) {
      this.offsetTrackerThread =
          offsetConsumerPollThreadCache
              .get()
              .acquireOffsetTrackerThread(
                  fullOffsetConsumerConfig, consumerFactoryFn, kafkaSourceDescriptor);
    }

    @Override
    public long estimate() {
      return offsetTrackerThread.readEndOffset();
    }

    public boolean isClosed() {
      return offsetTrackerThread.isClosed();
    }
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element KafkaSourceDescriptor kafkaSourceDescriptor) {
    Map<String, Object> updatedConsumerConfig =
        overrideBootstrapServersConfig(consumerConfig, kafkaSourceDescriptor);
    TopicPartition partition = kafkaSourceDescriptor.getTopicPartition();
    LOG.info("Creating Kafka consumer for initial restriction for {}", partition);
    try (Consumer<byte[], byte[]> offsetConsumer = consumerFactoryFn.apply(updatedConsumerConfig)) {
      ConsumerSpEL.evaluateAssign(offsetConsumer, ImmutableList.of(partition));
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
      throws Exception {
    final LoadingCache<TopicPartition, AverageRecordSize> avgRecordSize =
        Preconditions.checkStateNotNull(this.avgRecordSize);
    double numRecords =
        restrictionTracker(kafkaSourceDescriptor, offsetRange).getProgress().getWorkRemaining();
    // Before processing elements, we don't have a good estimated size of records and offset gap.
    if (!avgRecordSize.asMap().containsKey(kafkaSourceDescriptor.getTopicPartition())) {
      return numRecords;
    }
    if (offsetEstimatorCache != null) {
      for (Map.Entry<TopicPartition, KafkaLatestOffsetEstimator> tp :
          offsetEstimatorCache.entrySet()) {
        perPartitionBacklogMetrics.put(tp.getKey().toString(), tp.getValue().estimate());
      }
    }

    return avgRecordSize.get(kafkaSourceDescriptor.getTopicPartition()).getTotalSize(numRecords);
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
    final Map<TopicPartition, KafkaLatestOffsetEstimator> offsetEstimatorCacheInstance =
        Preconditions.checkStateNotNull(this.offsetEstimatorCache);

    TopicPartition topicPartition = kafkaSourceDescriptor.getTopicPartition();
    KafkaLatestOffsetEstimator offsetEstimator = offsetEstimatorCacheInstance.get(topicPartition);
    if (offsetEstimator == null || offsetEstimator.isClosed()) {
      Map<String, Object> updatedConsumerConfig =
          overrideBootstrapServersConfig(consumerConfig, kafkaSourceDescriptor);

      Map<String, Object> offsetConsumerConfig1 =
          KafkaIOUtils.getOffsetConsumerConfig(
              "tracker-" + topicPartition, offsetConsumerConfig, updatedConsumerConfig);

      offsetEstimator =
          new KafkaLatestOffsetEstimator(
              offsetConsumerConfig1, consumerFactoryFn, kafkaSourceDescriptor);
    }

    return new GrowableOffsetRangeTracker(restriction.getFrom(), offsetEstimator);
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator<Instant> watermarkEstimator,
      MultiOutputReceiver receiver)
      throws Exception {
    final LoadingCache<TopicPartition, AverageRecordSize> avgRecordSize =
        Preconditions.checkStateNotNull(this.avgRecordSize);
    final Deserializer<K> keyDeserializerInstance =
        Preconditions.checkStateNotNull(this.keyDeserializerInstance);
    final Deserializer<V> valueDeserializerInstance =
        Preconditions.checkStateNotNull(this.valueDeserializerInstance);
    final Distribution rawSizes =
        Metrics.distribution(
            METRIC_NAMESPACE,
            RAW_SIZE_METRIC_PREFIX + kafkaSourceDescriptor.getTopicPartition().toString());
    for (Map.Entry<String, Long> backlogSplit : perPartitionBacklogMetrics.entrySet()) {
      Gauge backlog =
          Metrics.gauge(
              METRIC_NAMESPACE, RAW_SIZE_METRIC_PREFIX + "backlogBytes_" + backlogSplit.getKey());
      backlog.set(backlogSplit.getValue());
    }

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
              kafkaSourceDescriptor.getTopicPartition(),
              Optional.ofNullable(watermarkEstimator.currentWatermark()));
    }
    final long startOffset = tracker.currentRestriction().getFrom();
    long expectedOffset = startOffset;
    long resumeOffset = startOffset;
    long skippedRecords = 0L;
    final Stopwatch sw = Stopwatch.createStarted();
    @Nullable KafkaConsumerPollThread pollThread = null;
    try {
      pollThread =
          cache
              .get()
              .acquireConsumer(
                  updatedConsumerConfig, consumerFactoryFn, kafkaSourceDescriptor, startOffset);
      while (true) {
        ConsumerRecord<byte[], byte[]> rawRecord = pollThread.peek();

        // When there are no records available for the current TopicPartition, self-checkpoint
        // and move to process the next element.
        if (pollThread.isTopicListUpdated() && rawRecord == null) {
          Map<String, List<PartitionInfo>> topicsList = pollThread.getTopicsList();
          if (!topicPartitionExists(kafkaSourceDescriptor.getTopicPartition(), topicsList)) {
            LOG.info(
                "Closing the offset thread for {} because the partition doesn't exist anymore",
                kafkaSourceDescriptor.getTopicPartition());
            offsetConsumerPollThreadCache.get().invalidate(kafkaSourceDescriptor);
            return ProcessContinuation.stop();
          }
          if (timestampPolicy != null) {
            updateWatermarkManually(timestampPolicy, watermarkEstimator, tracker);
          }
          return ProcessContinuation.resume();
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
          avgRecordSize
              .getUnchecked(kafkaSourceDescriptor.getTopicPartition())
              .update(recordSize, rawRecord.offset() - resumeOffset);
          rawSizes.update(recordSize);
          resumeOffset = rawRecord.offset() + 1;
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
              "Failure deserializing Key or Value of Kafka record reading from Kafka");
          if (timestampPolicy != null) {
            updateWatermarkManually(timestampPolicy, watermarkEstimator, tracker);
          }

          pollThread.advance();
        }
      }
    } finally {
      if (pollThread != null) {
        cache
            .get()
            .releaseConsumer(
                updatedConsumerConfig,
                consumerFactoryFn,
                kafkaSourceDescriptor,
                pollThread,
                resumeOffset);
      }
    }
  }

  private boolean topicPartitionExists(
      TopicPartition topicPartition, Map<String, List<PartitionInfo>> topicListMap) {
    // Check if the current TopicPartition still exists.
    Set<TopicPartition> existingTopicPartitions = new HashSet<>();
    for (List<PartitionInfo> topicPartitionList : topicListMap.values()) {
      topicPartitionList.forEach(
          partitionInfo -> {
            existingTopicPartitions.add(
                new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
          });
    }
    if (!existingTopicPartitions.contains(topicPartition)) {
      return false;
    }
    return true;
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
    avgRecordSize =
        CacheBuilder.newBuilder()
            .maximumSize(1000L)
            .build(
                new CacheLoader<TopicPartition, AverageRecordSize>() {
                  @Override
                  public AverageRecordSize load(TopicPartition topicPartition) {
                    return new AverageRecordSize();
                  }
                });
    keyDeserializerInstance = keyDeserializerProvider.getDeserializer(consumerConfig, true);
    valueDeserializerInstance = valueDeserializerProvider.getDeserializer(consumerConfig, false);
    offsetEstimatorCache = new HashMap<>();
    if (checkStopReadingFn != null) {
      checkStopReadingFn.setup();
    }
  }

  @Teardown
  public void teardown() throws Exception {
    final Deserializer<K> keyDeserializerInstance =
        Preconditions.checkStateNotNull(this.keyDeserializerInstance);
    final Deserializer<V> valueDeserializerInstance =
        Preconditions.checkStateNotNull(this.valueDeserializerInstance);
    try {
      Closeables.close(keyDeserializerInstance, true);
      Closeables.close(valueDeserializerInstance, true);
    } catch (Exception anyException) {
      LOG.warn("Fail to close resource during finishing bundle.", anyException);
    }

    if (offsetEstimatorCache != null) {
      checkNotNull(offsetEstimatorCache).clear();
    }
    if (checkStopReadingFn != null) {
      checkStopReadingFn.teardown();
    }
  }

  private Map<String, Object> overrideBootstrapServersConfig(
      Map<String, Object> currentConfig, KafkaSourceDescriptor description) {
    checkState(
        currentConfig.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
            || description.getBootStrapServers() != null);
    Map<String, Object> config = new HashMap<>(currentConfig);
    if (description.getBootStrapServers() != null && !description.getBootStrapServers().isEmpty()) {
      config.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          String.join(",", description.getBootStrapServers()));
    }
    return config;
  }

  private static class AverageRecordSize {
    private final MovingAvg avgRecordSize;
    private final MovingAvg avgRecordGap;

    public AverageRecordSize() {
      this.avgRecordSize = new MovingAvg();
      this.avgRecordGap = new MovingAvg();
    }

    public void update(int recordSize, long gap) {
      avgRecordSize.update(recordSize);
      avgRecordGap.update(gap);
    }

    public double getTotalSize(double numRecords) {
      return avgRecordSize.get() * numRecords / (1 + avgRecordGap.get());
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
