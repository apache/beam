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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO.ReadSourceDescriptors;
import org.apache.beam.sdk.io.kafka.KafkaIOUtils.MovingAvg;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedReader.TimestampPolicyContext;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.MonotonicallyIncreasing;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Closeables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SplittableDoFn which reads from {@link KafkaSourceDescriptor} and outputs {@link KafkaRecord}.
 * By default, a {@link MonotonicallyIncreasing} watermark estimator is used to track watermark.
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
 * <p>TODO(BEAM-10319): Add support for initial splitting.
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
 */
@UnboundedPerElement
class ReadFromKafkaDoFn<K, V> extends DoFn<KafkaSourceDescriptor, KafkaRecord<K, V>> {

  ReadFromKafkaDoFn(ReadSourceDescriptors transform) {
    this.consumerConfig = transform.getConsumerConfig();
    this.offsetConsumerConfig = transform.getOffsetConsumerConfig();
    this.keyDeserializerProvider = transform.getKeyDeserializerProvider();
    this.valueDeserializerProvider = transform.getValueDeserializerProvider();
    this.consumerFactoryFn = transform.getConsumerFactoryFn();
    this.extractOutputTimestampFn = transform.getExtractOutputTimestampFn();
    this.createWatermarkEstimatorFn = transform.getCreateWatermarkEstimatorFn();
    this.timestampPolicyFactory = transform.getTimestampPolicyFactory();
  }

  private static final Logger LOG = LoggerFactory.getLogger(ReadFromKafkaDoFn.class);

  private final Map<String, Object> offsetConsumerConfig;

  private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
      consumerFactoryFn;
  private final SerializableFunction<KafkaRecord<K, V>, Instant> extractOutputTimestampFn;
  private final SerializableFunction<Instant, WatermarkEstimator<Instant>>
      createWatermarkEstimatorFn;
  private final TimestampPolicyFactory<K, V> timestampPolicyFactory;

  // Valid between bundle start and bundle finish.
  private transient ConsumerSpEL consumerSpEL = null;
  private transient Deserializer<K> keyDeserializerInstance = null;
  private transient Deserializer<V> valueDeserializerInstance = null;

  private transient LoadingCache<TopicPartition, AverageRecordSize> avgRecordSize;

  private static final Duration KAFKA_POLL_TIMEOUT = Duration.millis(1000);

  @VisibleForTesting final DeserializerProvider keyDeserializerProvider;
  @VisibleForTesting final DeserializerProvider valueDeserializerProvider;
  @VisibleForTesting final Map<String, Object> consumerConfig;

  /**
   * A {@link GrowableOffsetRangeTracker.RangeEndEstimator} which uses a Kafka {@link Consumer} to
   * fetch backlog.
   */
  private static class KafkaLatestOffsetEstimator
      implements GrowableOffsetRangeTracker.RangeEndEstimator {

    private final Consumer<byte[], byte[]> offsetConsumer;
    private final TopicPartition topicPartition;
    private final ConsumerSpEL consumerSpEL;
    private final Supplier<Long> memoizedBacklog;

    KafkaLatestOffsetEstimator(
        Consumer<byte[], byte[]> offsetConsumer, TopicPartition topicPartition) {
      this.offsetConsumer = offsetConsumer;
      this.topicPartition = topicPartition;
      this.consumerSpEL = new ConsumerSpEL();
      this.consumerSpEL.evaluateAssign(this.offsetConsumer, ImmutableList.of(this.topicPartition));
      memoizedBacklog =
          Suppliers.memoizeWithExpiration(
              () -> {
                consumerSpEL.evaluateSeek2End(offsetConsumer, topicPartition);
                return offsetConsumer.position(topicPartition);
              },
              5,
              TimeUnit.SECONDS);
    }

    @Override
    protected void finalize() {
      try {
        Closeables.close(offsetConsumer, true);
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
    try (Consumer<byte[], byte[]> offsetConsumer =
        consumerFactoryFn.apply(
            KafkaIOUtils.getOffsetConsumerConfig(
                "initialOffset", offsetConsumerConfig, updatedConsumerConfig))) {
      consumerSpEL.evaluateAssign(
          offsetConsumer, ImmutableList.of(kafkaSourceDescriptor.getTopicPartition()));
      long startOffset;
      if (kafkaSourceDescriptor.getStartReadOffset() != null) {
        startOffset = kafkaSourceDescriptor.getStartReadOffset();
      } else if (kafkaSourceDescriptor.getStartReadTime() != null) {
        startOffset =
            consumerSpEL.offsetForTime(
                offsetConsumer,
                kafkaSourceDescriptor.getTopicPartition(),
                kafkaSourceDescriptor.getStartReadTime());
      } else {
        startOffset = offsetConsumer.position(kafkaSourceDescriptor.getTopicPartition());
      }
      return new OffsetRange(startOffset, Long.MAX_VALUE);
    }
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public WatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return createWatermarkEstimatorFn.apply(watermarkEstimatorState);
  }

  @GetSize
  public double getSize(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor, @Restriction OffsetRange offsetRange)
      throws Exception {
    double numRecords =
        restrictionTracker(kafkaSourceDescriptor, offsetRange).getProgress().getWorkRemaining();
    // Before processing elements, we don't have a good estimated size of records and offset gap.
    if (!avgRecordSize.asMap().containsKey(kafkaSourceDescriptor.getTopicPartition())) {
      return numRecords;
    }
    return avgRecordSize.get(kafkaSourceDescriptor.getTopicPartition()).getTotalSize(numRecords);
  }

  @NewTracker
  public GrowableOffsetRangeTracker restrictionTracker(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor, @Restriction OffsetRange restriction) {
    Map<String, Object> updatedConsumerConfig =
        overrideBootstrapServersConfig(consumerConfig, kafkaSourceDescriptor);
    KafkaLatestOffsetEstimator offsetPoller =
        new KafkaLatestOffsetEstimator(
            consumerFactoryFn.apply(
                KafkaIOUtils.getOffsetConsumerConfig(
                    "tracker-" + kafkaSourceDescriptor.getTopicPartition(),
                    offsetConsumerConfig,
                    updatedConsumerConfig)),
            kafkaSourceDescriptor.getTopicPartition());
    return new GrowableOffsetRangeTracker(restriction.getFrom(), offsetPoller);
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element KafkaSourceDescriptor kafkaSourceDescriptor,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator watermarkEstimator,
      OutputReceiver<KafkaRecord<K, V>> receiver) {
    // If there is no future work, resume with max timeout and move to the next element.
    Map<String, Object> updatedConsumerConfig =
        overrideBootstrapServersConfig(consumerConfig, kafkaSourceDescriptor);
    // If there is a timestampPolicyFactory, create the TimestampPolicy for current
    // TopicPartition.
    TimestampPolicy timestampPolicy = null;
    if (timestampPolicyFactory != null) {
      timestampPolicy =
          timestampPolicyFactory.createTimestampPolicy(
              kafkaSourceDescriptor.getTopicPartition(),
              Optional.ofNullable(watermarkEstimator.currentWatermark()));
    }
    try (Consumer<byte[], byte[]> consumer = consumerFactoryFn.apply(updatedConsumerConfig)) {
      consumerSpEL.evaluateAssign(
          consumer, ImmutableList.of(kafkaSourceDescriptor.getTopicPartition()));
      long startOffset = tracker.currentRestriction().getFrom();
      long expectedOffset = startOffset;
      consumer.seek(kafkaSourceDescriptor.getTopicPartition(), startOffset);
      ConsumerRecords<byte[], byte[]> rawRecords = ConsumerRecords.empty();

      while (true) {
        rawRecords = consumer.poll(KAFKA_POLL_TIMEOUT.getMillis());
        // When there are no records available for the current TopicPartition, self-checkpoint
        // and move to process the next element.
        if (rawRecords.isEmpty()) {
          return ProcessContinuation.resume();
        }
        for (ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
          if (!tracker.tryClaim(rawRecord.offset())) {
            return ProcessContinuation.stop();
          }
          KafkaRecord<K, V> kafkaRecord =
              new KafkaRecord<>(
                  rawRecord.topic(),
                  rawRecord.partition(),
                  rawRecord.offset(),
                  consumerSpEL.getRecordTimestamp(rawRecord),
                  consumerSpEL.getRecordTimestampType(rawRecord),
                  ConsumerSpEL.hasHeaders() ? rawRecord.headers() : null,
                  (K) consumerSpEL.deserializeKey(keyDeserializerInstance, rawRecord),
                  (V) consumerSpEL.deserializeValue(valueDeserializerInstance, rawRecord));
          int recordSize =
              (rawRecord.key() == null ? 0 : rawRecord.key().length)
                  + (rawRecord.value() == null ? 0 : rawRecord.value().length);
          avgRecordSize
              .getUnchecked(kafkaSourceDescriptor.getTopicPartition())
              .update(recordSize, rawRecord.offset() - expectedOffset);
          expectedOffset = rawRecord.offset() + 1;
          Instant outputTimestamp;
          // The outputTimestamp and watermark will be computed by timestampPolicy, where the
          // WatermarkEstimator should be a manual one.
          if (timestampPolicy != null) {
            checkState(watermarkEstimator instanceof ManualWatermarkEstimator);
            TimestampPolicyContext context =
                new TimestampPolicyContext(
                    (long) ((HasProgress) tracker).getProgress().getWorkRemaining(), Instant.now());
            outputTimestamp = timestampPolicy.getTimestampForRecord(context, kafkaRecord);
            ((ManualWatermarkEstimator) watermarkEstimator)
                .setWatermark(timestampPolicy.getWatermark(context));
          } else {
            outputTimestamp = extractOutputTimestampFn.apply(kafkaRecord);
          }
          receiver.outputWithTimestamp(kafkaRecord, outputTimestamp);
        }
      }
    }
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
                  public AverageRecordSize load(TopicPartition topicPartition) throws Exception {
                    return new AverageRecordSize();
                  }
                });
    consumerSpEL = new ConsumerSpEL();
    keyDeserializerInstance = keyDeserializerProvider.getDeserializer(consumerConfig, true);
    valueDeserializerInstance = valueDeserializerProvider.getDeserializer(consumerConfig, false);
  }

  @Teardown
  public void teardown() throws Exception {
    try {
      Closeables.close(keyDeserializerInstance, true);
      Closeables.close(valueDeserializerInstance, true);
    } catch (Exception anyException) {
      LOG.warn("Fail to close resource during finishing bundle.", anyException);
    }
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

  private static class AverageRecordSize {
    private MovingAvg avgRecordSize;
    private MovingAvg avgRecordGap;

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
}
