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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.kafka.KafkaData.KafkaSourceDescription;
import org.apache.beam.sdk.io.kafka.KafkaData.KafkaSourceDescription.StartReadConfig;
import org.apache.beam.sdk.io.kafka.KafkaData.KafkaSourceDescription.StartReadConfig.StartReadCase;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.MonotonicallyIncreasing;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Closeables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that takes a PCollection of {@link KafkaSourceDescription} as input and
 * outputs a PCollection of {@link KafkaRecord}. The core implementation is based on {@code
 * SplittableDoFn}. For more details about the concept of {@code SplittableDoFn}, please refer to
 * the beam blog post: https://beam.apache.org/blog/splittable-do-fn/ and design
 * doc:https://s.apache.org/beam-fn-api. The major difference from {@link KafkaIO.Read} is, {@link
 * ReadViaSDF} doesn't require source descriptions(e.g., {@link KafkaIO.Read#getTopicPartitions()},
 * {@link KafkaIO.Read#getTopics()}, {@link KafkaIO.Read#getStartReadTime()}, etc.) during the
 * pipeline construction time. Instead, the pipeline can populate these source descriptions during
 * runtime. For example, the pipeline can query Kafka topics from BigQuery table and read these
 * topics via {@link ReadViaSDF}.
 *
 * <h3>Common Kafka Consumer Configurations</h3>
 *
 * <p>Most Kafka consumer configurations are similar to {@link KafkaIO.Read}:
 *
 * <ul>
 *   <li>{@link ReadViaSDF#getConsumerConfig()} is the same as {@link
 *       KafkaIO.Read#getConsumerConfig()}.
 *   <li>{@link ReadViaSDF#getConsumerFactoryFn()} is the same as {@link
 *       KafkaIO.Read#getConsumerFactoryFn()}.
 *   <li>{@link ReadViaSDF#getOffsetConsumerConfig()} is the same as {@link
 *       KafkaIO.Read#getOffsetConsumerConfig()}.
 *   <li>{@link ReadViaSDF#getKeyCoder()} is the same as {@link KafkaIO.Read#getKeyCoder()}.
 *   <li>{@link ReadViaSDF#getValueCoder()} is the same as {@link KafkaIO.Read#getValueCoder()}.
 *   <li>{@link ReadViaSDF#getKeyDeserializerProvider()} is the same as {@link
 *       KafkaIO.Read#getKeyDeserializerProvider()}.
 *   <li>{@link ReadViaSDF#getValueDeserializerProvider()} is the same as {@link
 *       KafkaIO.Read#getValueDeserializerProvider()}.
 *   <li>{@link ReadViaSDF#isCommitOffsetEnabled()} means the same as {@link
 *       KafkaIO.Read#isCommitOffsetsInFinalizeEnabled()}.
 * </ul>
 *
 * <p>For example, to create a basic {@link ReadViaSDF} transform:
 *
 * <pre>{@code
 * pipeline
 *  .apply(Create.of(KafkaSourceDescription.of(new TopicPartition("my_topic", 1))))
 *  .apply(ReadFromKafkaViaSDF.create()
 *          .withBootstrapServers("broker_1:9092,broker_2:9092")
 *          .withKeyDeserializer(LongDeserializer.class).
 *          .withValueDeserializer(StringDeserializer.class));
 * }</pre>
 *
 * <h3>Configurations of {@link ReadViaSDF}</h3>
 *
 * <p>Except configurations of Kafka Consumer, there are some other configurations which are related
 * to processing records.
 *
 * <p>{@link ReadViaSDF#commitOffsets()} enables committing offset after processing the record. Note
 * that if {@code isolation.level} is set to "read_committed" or {@link
 * ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG} is set in the consumer config, the {@link
 * ReadViaSDF#commitOffsets()} will be ignored.
 *
 * <p>{@link ReadViaSDF#withExtractOutputTimestampFn(SerializableFunction)} asks for a function
 * which takes a {@link KafkaRecord} as input and outputs outputTimestamp. This function is used to
 * produce output timestamp per {@link KafkaRecord}. There are three built-in types: {@link
 * ReadViaSDF#withProcessingTime()}, {@link ReadViaSDF#withCreateTime()} and {@link
 * ReadViaSDF#withLogAppendTime()}.
 *
 * <p>For example, to create a {@link ReadViaSDF} with these configurations:
 *
 * <pre>{@code
 * pipeline
 * .apply(Create.of(KafkaSourceDescription.of(new TopicPartition("my_topic", 1))))
 * .apply(ReadFromKafkaViaSDF.create()
 *          .withBootstrapServers("broker_1:9092,broker_2:9092")
 *          .withKeyDeserializer(LongDeserializer.class).
 *          .withValueDeserializer(StringDeserializer.class)
 *          .withProcessingTime()
 *          .commitOffsets());
 *
 * }</pre>
 *
 * <h3>Read from {@link KafkaSourceDescription}</h3>
 *
 * {@link ReadFromKafkaDoFn} implements the logic of reading from Kafka. The element is a {@link
 * KafkaSourceDescription}, and the restriction is an {@link OffsetRange} which represents record
 * offset. A {@link GrowableOffsetRangeTracker} is used to track an {@link OffsetRange} ended with
 * {@code Long.MAX_VALUE}. For a finite range, a {@link OffsetRangeTracker} is created.
 *
 * <h4>Initialize Restriction</h4>
 *
 * {@link ReadFromKafkaDoFn#initialRestriction(KafkaSourceDescription)} creates an initial range for
 * a input element {@link KafkaSourceDescription}. The end of range will be initialized as {@code
 * Long.MAX_VALUE}. For the start of the range:
 *
 * <ul>
 *   <li>If {@link KafkaSourceDescription#getStartOffset()} is set, use this offset as start.
 *   <li>If {@link KafkaSourceDescription#getStartReadTime()} is set, seek the start offset based on
 *       this time.
 *   <li>Otherwise, the last committed offset + 1 will be returned by {@link
 *       Consumer#position(TopicPartition)} as the start.
 * </ul>
 *
 * <h4>Initial Split</h4>
 *
 * <p>There is no initial split for now.
 *
 * <h4>Checkpoint and Resume Processing</h4>
 *
 * <p>There are 2 types of checkpoint here: self-checkpoint which invokes by the DoFn and
 * system-checkpoint which is issued by the runner via {@link
 * org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest}. Every time the
 * consumer gets empty response from {@link Consumer#poll(long)}, {@link ReadFromKafkaDoFn} will
 * checkpoint at current {@link KafkaSourceDescription} and move to process the next element. These
 * deferred elements will be resumed by the runner as soon as possible.
 *
 * <h4>Progress and Size</h4>
 *
 * <p>The progress is provided by {@link GrowableOffsetRangeTracker} or {@link OffsetRangeTracker}
 * per {@link KafkaSourceDescription}. For an infinite {@link OffsetRange}, a Kafka {@link Consumer}
 * is used in the {@link GrowableOffsetRangeTracker} as the {@link
 * GrowableOffsetRangeTracker.RangeEndEstimator} to poll the latest offset. Please refer to {@link
 * ReadFromKafkaDoFn.KafkaLatestOffsetEstimator} for details.
 *
 * <p>The size is computed by {@link ReadFromKafkaDoFn#getSize(KafkaSourceDescription,
 * OffsetRange).} A {@link KafkaIOUtils.MovingAvg} is used to track the average size of kafka
 * records.
 *
 * <h4>Track Watermark</h4>
 *
 * The estimated watermark is computed by {@link MonotonicallyIncreasing} based on output timestamps
 * per {@link KafkaSourceDescription}.
 */
@AutoValue
public abstract class ReadViaSDF<K, V>
    extends PTransform<PCollection<KafkaSourceDescription>, PCollection<KafkaRecord<K, V>>> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadViaSDF.class);

  abstract Map<String, Object> getConsumerConfig();

  @Nullable
  abstract Map<String, Object> getOffsetConsumerConfig();

  @Nullable
  abstract DeserializerProvider getKeyDeserializerProvider();

  @Nullable
  abstract DeserializerProvider getValueDeserializerProvider();

  @Nullable
  abstract Coder<K> getKeyCoder();

  @Nullable
  abstract Coder<V> getValueCoder();

  abstract SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
      getConsumerFactoryFn();

  abstract SerializableFunction<KafkaRecord<K, V>, Instant> getExtractOutputTimestampFn();

  abstract boolean isCommitOffsetEnabled();

  abstract Builder<K, V> toBuilder();

  @AutoValue.Builder
  abstract static class Builder<K, V> {
    abstract Builder<K, V> setConsumerConfig(Map<String, Object> config);

    abstract Builder<K, V> setOffsetConsumerConfig(Map<String, Object> offsetConsumerConfig);

    abstract Builder<K, V> setConsumerFactoryFn(
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn);

    abstract Builder<K, V> setKeyDeserializerProvider(DeserializerProvider deserializerProvider);

    abstract Builder<K, V> setValueDeserializerProvider(DeserializerProvider deserializerProvider);

    abstract Builder<K, V> setKeyCoder(Coder<K> keyCoder);

    abstract Builder<K, V> setValueCoder(Coder<V> valueCoder);

    abstract Builder<K, V> setExtractOutputTimestampFn(
        SerializableFunction<KafkaRecord<K, V>, Instant> fn);

    abstract Builder<K, V> setCommitOffsetEnabled(boolean commitOffsetEnabled);

    abstract ReadViaSDF<K, V> build();
  }

  public static <K, V> ReadViaSDF<K, V> read() {
    return new AutoValue_ReadViaSDF.Builder<K, V>()
        .setConsumerFactoryFn(KafkaIOUtils.KAFKA_CONSUMER_FACTORY_FN)
        .setConsumerConfig(KafkaIOUtils.DEFAULT_CONSUMER_PROPERTIES)
        .setExtractOutputTimestampFn(ExtractOutputTimestampFns.useProcessingTime())
        .setCommitOffsetEnabled(false)
        .build();
  }

  public ReadViaSDF<K, V> withBootstrapServers(String bootstrapServers) {
    return withConsumerConfigUpdates(
        ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
  }

  public ReadViaSDF<K, V> withKeyDeserializerProvider(
      DeserializerProvider<K> deserializerProvider) {
    return toBuilder().setKeyDeserializerProvider(deserializerProvider).build();
  }

  public ReadViaSDF<K, V> withValueDeserializerProvider(
      DeserializerProvider<V> deserializerProvider) {
    return toBuilder().setValueDeserializerProvider(deserializerProvider).build();
  }

  public ReadViaSDF<K, V> withKeyDeserializer(Class<? extends Deserializer<K>> keyDeserializer) {
    return withKeyDeserializerProvider(LocalDeserializerProvider.of(keyDeserializer));
  }

  public ReadViaSDF<K, V> withValueDeserializer(
      Class<? extends Deserializer<V>> valueDeserializer) {
    return withValueDeserializerProvider(LocalDeserializerProvider.of(valueDeserializer));
  }

  public ReadViaSDF<K, V> withKeyDeserializerAndCoder(
      Class<? extends Deserializer<K>> keyDeserializer, Coder<K> keyCoder) {
    return withKeyDeserializer(keyDeserializer).toBuilder().setKeyCoder(keyCoder).build();
  }

  public ReadViaSDF<K, V> withValueDeserializerAndCoder(
      Class<? extends Deserializer<V>> valueDeserializer, Coder<V> valueCoder) {
    return withValueDeserializer(valueDeserializer).toBuilder().setValueCoder(valueCoder).build();
  }

  public ReadViaSDF<K, V> withConsumerFactoryFn(
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn) {
    return toBuilder().setConsumerFactoryFn(consumerFactoryFn).build();
  }

  public ReadViaSDF<K, V> withConsumerConfigUpdates(Map<String, Object> configUpdates) {
    Map<String, Object> config =
        KafkaIOUtils.updateKafkaProperties(
            getConsumerConfig(), KafkaIOUtils.IGNORED_CONSUMER_PROPERTIES, configUpdates);
    return toBuilder().setConsumerConfig(config).build();
  }

  public ReadViaSDF<K, V> withExtractOutputTimestampFn(
      SerializableFunction<KafkaRecord<K, V>, Instant> fn) {
    return toBuilder().setExtractOutputTimestampFn(fn).build();
  }

  public ReadViaSDF<K, V> withLogAppendTime() {
    return withExtractOutputTimestampFn(ExtractOutputTimestampFns.useLogAppendTime());
  }

  public ReadViaSDF<K, V> withProcessingTime() {
    return withExtractOutputTimestampFn(ExtractOutputTimestampFns.useProcessingTime());
  }

  public ReadViaSDF<K, V> withCreateTime() {
    return withExtractOutputTimestampFn(ExtractOutputTimestampFns.useCreateTime());
  }

  // If a transactional producer is used and it's desired to only read records from committed
  // transaction, it's recommended to set read_committed. Otherwise, read_uncommitted is the default
  // value.
  public ReadViaSDF<K, V> withReadCommitted() {
    return withConsumerConfigUpdates(ImmutableMap.of("isolation.level", "read_committed"));
  }

  public ReadViaSDF<K, V> commitOffsets() {
    return toBuilder().setCommitOffsetEnabled(true).build();
  }

  public ReadViaSDF<K, V> withOffsetConsumerConfigOverrides(
      Map<String, Object> offsetConsumerConfig) {
    return toBuilder().setOffsetConsumerConfig(offsetConsumerConfig).build();
  }

  public ReadViaSDF<K, V> withConsumerConfigOverrides(Map<String, Object> consumerConfig) {
    return toBuilder().setConsumerConfig(consumerConfig).build();
  }

  @Override
  public PCollection<KafkaRecord<K, V>> expand(PCollection<KafkaSourceDescription> input) {
    checkArgument(
        ExperimentalOptions.hasExperiment(input.getPipeline().getOptions(), "beam_fn_api"),
        "The ReadFromKafkaViaSDF can only used when beam_fn_api is enabled.");
    checkArgument(
        getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) != null,
        "withBootstrapServers() is required");
    checkArgument(getKeyDeserializerProvider() != null, "withKeyDeserializer() is required");
    checkArgument(getValueDeserializerProvider() != null, "withValueDeserializer() is required");

    ConsumerSpEL consumerSpEL = new ConsumerSpEL();
    if (!consumerSpEL.hasOffsetsForTimes()) {
      LOG.warn(
          "Kafka client version {} is too old. Versions before 0.10.1.0 are deprecated and "
              + "may not be supported in next release of Apache Beam. "
              + "Please upgrade your Kafka client version.",
          AppInfoParser.getVersion());
    }

    if (isCommitOffsetEnabled()) {
      if (configuredKafkaCommit()) {
        LOG.info(
            "Either read_committed or auto_commit is set together with commitOffsetEnabled but you "
                + "only need one of them. The commitOffsetEnabled is going to be ignored");
      }
    }

    CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
    Coder<K> keyCoder = getKeyCoder(coderRegistry);
    Coder<V> valueCoder = getValueCoder(coderRegistry);
    Coder<KafkaRecord<K, V>> outputCoder = KafkaRecordCoder.of(keyCoder, valueCoder);
    PCollection<KafkaRecord<K, V>> output =
        input.apply(
            ParDo.of(
                new ReadFromKafkaDoFn<>(
                    getConsumerConfig(),
                    getOffsetConsumerConfig(),
                    getKeyDeserializerProvider(),
                    getValueDeserializerProvider(),
                    getConsumerFactoryFn(),
                    getExtractOutputTimestampFn())));
    output.setCoder(outputCoder);
    if (isCommitOffsetEnabled() && !configuredKafkaCommit()) {
      // TODO(BEAM-10123): Add CommitOffsetTransform to expansion.
      LOG.warn("Offset committed is not supported yet. Ignore the value.");
    }
    return output;
  }

  private Coder<K> getKeyCoder(CoderRegistry coderRegistry) {
    return (getKeyCoder() != null)
        ? getKeyCoder()
        : getKeyDeserializerProvider().getCoder(coderRegistry);
  }

  private Coder<V> getValueCoder(CoderRegistry coderRegistry) {
    return (getValueCoder() != null)
        ? getValueCoder()
        : getValueDeserializerProvider().getCoder(coderRegistry);
  }

  private boolean configuredKafkaCommit() {
    return getConsumerConfig().get("isolation.level") == "read_committed"
        || Boolean.TRUE.equals(getConsumerConfig().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
  }

  static class ExtractOutputTimestampFns<K, V> {
    public static <K, V> SerializableFunction<KafkaRecord<K, V>, Instant> useProcessingTime() {
      return record -> Instant.now();
    }

    public static <K, V> SerializableFunction<KafkaRecord<K, V>, Instant> useCreateTime() {
      return record -> {
        checkArgument(
            record.getTimestampType() == KafkaTimestampType.CREATE_TIME,
            "Kafka record's timestamp is not 'CREATE_TIME' "
                + "(topic: %s, partition %s, offset %s, timestamp type '%s')",
            record.getTopic(),
            record.getPartition(),
            record.getOffset(),
            record.getTimestampType());
        return new Instant(record.getTimestamp());
      };
    }

    public static <K, V> SerializableFunction<KafkaRecord<K, V>, Instant> useLogAppendTime() {
      return record -> {
        checkArgument(
            record.getTimestampType() == KafkaTimestampType.LOG_APPEND_TIME,
            "Kafka record's timestamp is not 'LOG_APPEND_TIME' "
                + "(topic: %s, partition %s, offset %s, timestamp type '%s')",
            record.getTopic(),
            record.getPartition(),
            record.getOffset(),
            record.getTimestampType());
        return new Instant(record.getTimestamp());
      };
    }
  }

  /**
   * A SplittableDoFn which reads from {@link KafkaSourceDescription} and outputs {@link
   * KafkaRecord}. By default, a {@link MonotonicallyIncreasing} watermark estimator is used to
   * track watermark.
   */
  static class ReadFromKafkaDoFn<K, V> extends DoFn<KafkaSourceDescription, KafkaRecord<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(ReadFromKafkaDoFn.class);

    public ReadFromKafkaDoFn(
        Map<String, Object> consumerConfig,
        Map<String, Object> offsetConsumerConfig,
        DeserializerProvider keyDeserializerProvider,
        DeserializerProvider valueDeserializerProvider,
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
        SerializableFunction<KafkaRecord<K, V>, Instant> extractOutputTimestampFn) {
      this.consumerConfig = consumerConfig;
      this.offsetConsumerConfig = offsetConsumerConfig;
      this.keyDeserializerProvider = keyDeserializerProvider;
      this.valueDeserializerProvider = valueDeserializerProvider;
      this.consumerFactoryFn = consumerFactoryFn;
      this.extractOutputTimestampFn = extractOutputTimestampFn;
    }

    private final Map<String, Object> consumerConfig;

    private final Map<String, Object> offsetConsumerConfig;

    private final DeserializerProvider keyDeserializerProvider;
    private final DeserializerProvider valueDeserializerProvider;

    private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
        consumerFactoryFn;
    private final SerializableFunction<KafkaRecord<K, V>, Instant> extractOutputTimestampFn;

    private static final Duration KAFKA_POLL_TIMEOUT = Duration.millis(1000);

    // Variables that are initialized when bundle is started and closed when FinishBundle is called.
    private transient ConsumerSpEL consumerSpEL = null;
    private transient Consumer<byte[], byte[]> consumer = null;
    private transient Deserializer<K> keyDeserializerInstance = null;
    private transient Deserializer<V> valueDeserializerInstance = null;

    private transient KafkaIOUtils.MovingAvg avgRecordSize = null;
    private transient KafkaIOUtils.MovingAvg avgOffsetGap = null;

    /**
     * A {@link GrowableOffsetRangeTracker.RangeEndEstimator} which uses a Kafka {@link Consumer} to
     * fetch backlog.
     */
    static class KafkaLatestOffsetEstimator
        implements GrowableOffsetRangeTracker.RangeEndEstimator {
      private final Consumer<byte[], byte[]> offsetConsumer;
      private final TopicPartition topicPartition;
      private final ConsumerSpEL consumerSpEL;
      private static final Logger LOG = LoggerFactory.getLogger(KafkaLatestOffsetEstimator.class);

      public KafkaLatestOffsetEstimator(
          Consumer<byte[], byte[]> offsetConsumer, TopicPartition topicPartition) {
        this.offsetConsumer = offsetConsumer;
        this.topicPartition = topicPartition;
        this.consumerSpEL = new ConsumerSpEL();
        this.consumerSpEL.evaluateAssign(
            this.offsetConsumer, ImmutableList.of(this.topicPartition));
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
        consumerSpEL.evaluateSeek2End(offsetConsumer, topicPartition);
        return offsetConsumer.position(topicPartition);
      }
    }

    @GetInitialRestriction
    public OffsetRange initialRestriction(@Element KafkaSourceDescription kafkaSourceDescription) {
      checkState(kafkaSourceDescription.hasTopicPartition());
      TopicPartition topicPartition =
          new TopicPartition(
              kafkaSourceDescription.getTopicPartition().getTopic(),
              kafkaSourceDescription.getTopicPartition().getPartition());
      try (Consumer<byte[], byte[]> offsetConsumer =
          consumerFactoryFn.apply(
              KafkaIOUtils.getOffsetConsumerConfig(
                  "initialOffset", offsetConsumerConfig, consumerConfig))) {
        consumerSpEL.evaluateAssign(offsetConsumer, ImmutableList.of(topicPartition));
        long startOffset;
        if (kafkaSourceDescription.hasStartReadConfig()) {
          int index = kafkaSourceDescription.getStartReadConfig().getStartReadCase().getNumber();
          StartReadCase readCase = kafkaSourceDescription.getStartReadConfig().getStartReadCase();
          switch (readCase) {
            case START_OFFSET:
              startOffset = kafkaSourceDescription.getStartReadConfig().getStartOffset();
              break;
            case START_READ_TIME:
              startOffset =
                  consumerSpEL.offsetForTime(
                      offsetConsumer,
                      topicPartition,
                      Instant.ofEpochMilli(
                          kafkaSourceDescription.getStartReadConfig().getStartReadTime()));
              break;
            default:
              startOffset = offsetConsumer.position(topicPartition);
          }
        } else {
          startOffset = offsetConsumer.position(topicPartition);
        }
        return new OffsetRange(startOffset, Long.MAX_VALUE);
      }
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
      return currentElementTimestamp;
    }

    @NewWatermarkEstimator
    public MonotonicallyIncreasing newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermarkEstimatorState) {
      return new MonotonicallyIncreasing(watermarkEstimatorState);
    }

    @GetSize
    public double getSize(
        @Element KafkaSourceDescription kafkaSourceDescription,
        @Restriction OffsetRange offsetRange)
        throws Exception {
      checkState(kafkaSourceDescription.hasTopicPartition());
      TopicPartition topicPartition =
          new TopicPartition(
              kafkaSourceDescription.getTopicPartition().getTopic(),
              kafkaSourceDescription.getTopicPartition().getPartition());
      double numOfRecords = 0.0;
      if (offsetRange.getTo() != Long.MAX_VALUE) {
        numOfRecords = (new OffsetRangeTracker(offsetRange)).getProgress().getWorkRemaining();
      } else {
        KafkaLatestOffsetEstimator offsetEstimator =
            new KafkaLatestOffsetEstimator(
                consumerFactoryFn.apply(
                    KafkaIOUtils.getOffsetConsumerConfig(
                        topicPartition.toString(),
                        KafkaIOUtils.getOffsetConsumerConfig(
                            "size-" + topicPartition.toString(),
                            offsetConsumerConfig,
                            consumerConfig),
                        consumerConfig)),
                topicPartition);
        numOfRecords =
            (new GrowableOffsetRangeTracker(offsetRange.getFrom(), offsetEstimator))
                .getProgress()
                .getWorkRemaining();
      }

      // Before processing elements, we don't have a good estimated size of records and offset gap.
      if (avgOffsetGap != null) {
        numOfRecords = numOfRecords / (1 + avgOffsetGap.get());
      }
      return (avgRecordSize == null ? 1 : avgRecordSize.get()) * numOfRecords;
    }

    @SplitRestriction
    public void splitRestriction(
        @Element KafkaSourceDescription kafkaSourceDescription,
        @Restriction OffsetRange offsetRange,
        OutputReceiver<OffsetRange> receiver)
        throws Exception {
      receiver.output(offsetRange);
    }

    @NewTracker
    public RestrictionTracker<OffsetRange, Long> restrictionTracker(
        @Element KafkaSourceDescription kafkaSourceDescription,
        @Restriction OffsetRange restriction) {
      TopicPartition topicPartition =
          new TopicPartition(
              kafkaSourceDescription.getTopicPartition().getTopic(),
              kafkaSourceDescription.getTopicPartition().getPartition());
      if (restriction.getTo() == Long.MAX_VALUE) {
        KafkaLatestOffsetEstimator offsetPoller =
            new KafkaLatestOffsetEstimator(
                consumerFactoryFn.apply(
                    KafkaIOUtils.getOffsetConsumerConfig(
                        "tracker-" + topicPartition, offsetConsumerConfig, consumerConfig)),
                topicPartition);
        return new GrowableOffsetRangeTracker(restriction.getFrom(), offsetPoller);
      }
      return new OffsetRangeTracker(restriction);
    }

    @ProcessElement
    public ProcessContinuation processElement(
        @Element KafkaSourceDescription kafkaSourceDescription,
        RestrictionTracker<OffsetRange, Long> tracker,
        WatermarkEstimator watermarkEstimator,
        OutputReceiver<KafkaRecord<K, V>> receiver) {
      checkState(kafkaSourceDescription.hasTopicPartition());
      TopicPartition topicPartition =
          new TopicPartition(
              kafkaSourceDescription.getTopicPartition().getTopic(),
              kafkaSourceDescription.getTopicPartition().getPartition());
      consumerSpEL.evaluateAssign(consumer, ImmutableList.of(topicPartition));
      long startOffset = tracker.currentRestriction().getFrom();
      long expectedOffset = startOffset;
      consumer.seek(topicPartition, startOffset);
      ConsumerRecords<byte[], byte[]> rawRecords = ConsumerRecords.empty();

      try {
        while (true) {
          rawRecords = consumer.poll(KAFKA_POLL_TIMEOUT.getMillis());
          // When there is no records from the current TopicPartition temporarily, self-checkpoint
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
                    keyDeserializerInstance.deserialize(rawRecord.topic(), rawRecord.key()),
                    valueDeserializerInstance.deserialize(rawRecord.topic(), rawRecord.value()));
            Instant outputTimestamp = extractOutputTimestampFn.apply(kafkaRecord);
            int recordSize =
                (rawRecord.key() == null ? 0 : rawRecord.key().length)
                    + (rawRecord.value() == null ? 0 : rawRecord.value().length);
            avgRecordSize.update(recordSize);
            avgOffsetGap.update(expectedOffset - rawRecord.offset());
            expectedOffset = rawRecord.offset() + 1;
            receiver.outputWithTimestamp(kafkaRecord, outputTimestamp);
          }
        }
      } catch (Exception anyException) {
        LOG.error("{}: Exception while reading from Kafka", this, anyException);
        throw anyException;
      }
    }

    @GetRestrictionCoder
    public Coder<OffsetRange> restrictionCoder() {
      return new OffsetRange.Coder();
    }

    @Setup
    public void setup() throws Exception {
      // Start to track record size and offset gap per bundle.
      avgRecordSize = new KafkaIOUtils.MovingAvg();
      avgOffsetGap = new KafkaIOUtils.MovingAvg();
      consumerSpEL = new ConsumerSpEL();
      consumer = consumerFactoryFn.apply(consumerConfig);
      keyDeserializerInstance = keyDeserializerProvider.getDeserializer(consumerConfig, true);
      valueDeserializerInstance = valueDeserializerProvider.getDeserializer(consumerConfig, false);
    }

    @Teardown
    public void teardown() throws Exception {
      try {
        Closeables.close(keyDeserializerInstance, true);
        Closeables.close(valueDeserializerInstance, true);
        Closeables.close(consumer, true);
        avgRecordSize = null;
        avgOffsetGap = null;
      } catch (Exception anyException) {
        LOG.warn("Fail to close resource during finishing bundle: {}", anyException.getMessage());
      }
    }

    @VisibleForTesting
    Map<String, Object> getConsumerConfig() {
      return consumerConfig;
    }

    @VisibleForTesting
    DeserializerProvider getKeyDeserializerProvider() {
      return keyDeserializerProvider;
    }

    @VisibleForTesting
    DeserializerProvider getValueDeserializerProvider() {
      return valueDeserializerProvider;
    }
  }
}
