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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded source and a sink for <a href="http://kafka.apache.org/">Kafka</a> topics.
 *
 * <h3>Reading from Kafka topics</h3>
 *
 * <p>KafkaIO source returns unbounded collection of Kafka records as
 * {@code PCollection<KafkaRecord<K, V>>}. A {@link KafkaRecord} includes basic
 * metadata like topic-partition and offset, along with key and value associated with a Kafka
 * record.
 *
 * <p>Although most applications consume a single topic, the source can be configured to consume
 * multiple topics or even a specific set of {@link TopicPartition}s.
 *
 * <p>To configure a Kafka source, you must specify at the minimum Kafka <tt>bootstrapServers</tt>,
 * one or more topics to consume, and key and value deserializers. For example:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(KafkaIO.<Long, String>read()
 *       .withBootstrapServers("broker_1:9092,broker_2:9092")
 *       .withTopic("my_topic")  // use withTopics(List<String>) to read from multiple topics.
 *       .withKeyDeserializer(LongDeserializer.class)
 *       .withValueDeserializer(StringDeserializer.class)
 *
 *       // Above four are required configuration. returns PCollection<KafkaRecord<Long, String>>
 *
 *       // Rest of the settings are optional :
 *
 *       // you can further customize KafkaConsumer used to read the records by adding more
 *       // settings for ConsumerConfig. e.g :
 *       .updateConsumerProperties(ImmutableMap.of("group.id", "my_beam_app_1"))
 *
 *       // set event times and watermark based on LogAppendTime. To provide a custom
 *       // policy see withTimestampPolicyFactory(). withProcessingTime() is the default.
 *       .withLogAppendTime()
 *
 *       // restrict reader to committed messages on Kafka (see method documentation).
 *       .withReadCommitted()
 *
 *       // offset consumed by the pipeline can be committed back.
 *       .commitOffsetsInFinalize()
 *
 *       // finally, if you don't need Kafka metadata, you can drop it.g
 *       .withoutMetadata() // PCollection<KV<Long, String>>
 *    )
 *    .apply(Values.<String>create()) // PCollection<String>
 *     ...
 * }</pre>
 *
 * <p>Kafka provides deserializers for common types in
 * {@link org.apache.kafka.common.serialization}. In addition to deserializers, Beam runners need
 * {@link Coder} to materialize key and value objects if necessary.
 * In most cases, you don't need to specify {@link Coder} for key and value in the resulting
 * collection because the coders are inferred from deserializer types. However, in cases when
 * coder inference fails, they can be specified explicitly along with deserializers using
 * {@link Read#withKeyDeserializerAndCoder(Class, Coder)} and
 * {@link Read#withValueDeserializerAndCoder(Class, Coder)}. Note that Kafka messages are
 * interpreted using key and value <i>deserializers</i>.
 *
 * <h3>Partition Assignment and Checkpointing</h3>
 * The Kafka partitions are evenly distributed among splits (workers).
 *
 * <p>Checkpointing is fully supported and each split can resume from previous checkpoint
 * (to the extent supported by runner).
 * See {@link KafkaUnboundedSource#split(int, PipelineOptions)} for more details on
 * splits and checkpoint support.
 *
 * <p>When the pipeline starts for the first time, or without any checkpoint, the source starts
 * consuming from the <em>latest</em> offsets. You can override this behavior to consume from the
 * beginning by setting appropriate appropriate properties in {@link ConsumerConfig}, through
 * {@link Read#updateConsumerProperties(Map)}.
 * You can also enable offset auto_commit in Kafka to resume from last committed.
 *
 * <p>In summary, KafkaIO.read follows below sequence to set initial offset:<br>
 * 1. {@link KafkaCheckpointMark} provided by runner;<br>
 * 2. Consumer offset stored in Kafka when
 * {@code ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG = true};<br>
 * 3. Start from <em>latest</em> offset by default;
 *
 * <h3>Writing to Kafka</h3>
 *
 * <p>KafkaIO sink supports writing key-value pairs to a Kafka topic. Users can also write
 * just the values. To configure a Kafka sink, you must specify at the minimum Kafka
 * <tt>bootstrapServers</tt>, the topic to write to, and key and value serializers. For example:
 *
 * <pre>{@code
 *
 *  PCollection<KV<Long, String>> kvColl = ...;
 *  kvColl.apply(KafkaIO.<Long, String>write()
 *       .withBootstrapServers("broker_1:9092,broker_2:9092")
 *       .withTopic("results")
 *
 *       .withKeySerializer(LongSerializer.class)
 *       .withValueSerializer(StringSerializer.class)
 *
 *       // you can further customize KafkaProducer used to write the records by adding more
 *       // settings for ProducerConfig. e.g, to enable compression :
 *       .updateProducerProperties(ImmutableMap.of("compression.type", "gzip"))
 *
 *       // Optionally enable exactly-once sink (on supported runners). See JavaDoc for withEOS().
 *       .withEOS(20, "eos-sink-group-id");
 *    );
 * }</pre>
 *
 * <p>Often you might want to write just values without any keys to Kafka. Use {@code values()} to
 * write records with default empty(null) key:
 *
 * <pre>{@code
 *  PCollection<String> strings = ...;
 *  strings.apply(KafkaIO.<Void, String>write()
 *      .withBootstrapServers("broker_1:9092,broker_2:9092")
 *      .withTopic("results")
 *      .withValueSerializer(StringSerializer.class) // just need serializer for value
 *      .values()
 *    );
 * }</pre>
 *
 * <h3>Advanced Kafka Configuration</h3>
 * KafkaIO allows setting most of the properties in {@link ConsumerConfig} for source or in
 * {@link ProducerConfig} for sink. E.g. if you would like to enable offset
 * <em>auto commit</em> (for external monitoring or other purposes), you can set
 * <tt>"group.id"</tt>, <tt>"enable.auto.commit"</tt>, etc.
 *
 * <h3>Event Timestamps and Watermark</h3>
 * By default, record timestamp (event time) is set to processing time in KafkaIO reader and
 * source watermark is current wall time. If a topic has Kafka server-side ingestion timestamp
 * enabled ('LogAppendTime'), it can enabled with {@link Read#withLogAppendTime()}.
 * A custom timestamp policy can be provided by implementing {@link TimestampPolicyFactory}. See
 * {@link Read#withTimestampPolicyFactory(TimestampPolicyFactory)} for more information.
 *
 * <h3>Supported Kafka Client Versions</h3>
 * KafkaIO relies on <i>kafka-clients</i> for all its interactions with the Kafka cluster.
 * <i>kafka-clients</i> versions 0.10.1 and newer are supported at runtime. The older versions
 * 0.9.x - 0.10.0.0 are also supported, but are deprecated and likely be removed in near future.
 * Please ensure that the version included with the application is compatible with the version of
 * your Kafka cluster. Kafka client usually fails to initialize with a clear error message in
 * case of incompatibility.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class KafkaIO {

  /**
   * A specific instance of uninitialized {@link #read()} where key and values are bytes.
   * See #read().
   */
  public static Read<byte[], byte[]> readBytes() {
    return KafkaIO.<byte[], byte[]>read()
      .withKeyDeserializer(ByteArrayDeserializer.class)
      .withValueDeserializer(ByteArrayDeserializer.class);
  }

  /**
   * Creates an uninitialized {@link Read} {@link PTransform}. Before use, basic Kafka
   * configuration should set with {@link Read#withBootstrapServers(String)} and
   * {@link Read#withTopics(List)}. Other optional settings include key and value
   * {@link Deserializer}s, custom timestamp and watermark functions.
   */
  public static <K, V> Read<K, V> read() {
    return new AutoValue_KafkaIO_Read.Builder<K, V>()
        .setTopics(new ArrayList<>())
        .setTopicPartitions(new ArrayList<>())
        .setConsumerFactoryFn(Read.KAFKA_CONSUMER_FACTORY_FN)
        .setConsumerConfig(Read.DEFAULT_CONSUMER_PROPERTIES)
        .setMaxNumRecords(Long.MAX_VALUE)
        .setCommitOffsetsInFinalizeEnabled(false)
        .setTimestampPolicyFactory(TimestampPolicyFactory.withProcessingTime())
        .build();
  }

  /**
   * Creates an uninitialized {@link Write} {@link PTransform}. Before use, Kafka configuration
   * should be set with {@link Write#withBootstrapServers(String)} and {@link Write#withTopic}
   * along with {@link Deserializer}s for (optional) key and values.
   */
  public static <K, V> Write<K, V> write() {
    return new AutoValue_KafkaIO_Write.Builder<K, V>()
        .setProducerConfig(Write.DEFAULT_PRODUCER_PROPERTIES)
        .setEOS(false)
        .setNumShards(0)
        .setConsumerFactoryFn(Read.KAFKA_CONSUMER_FACTORY_FN)
        .build();
  }

  ///////////////////////// Read Support \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  /**
   * A {@link PTransform} to read from Kafka topics. See {@link KafkaIO} for more information on
   * usage and configuration.
   */
  @AutoValue
  public abstract static class Read<K, V>
      extends PTransform<PBegin, PCollection<KafkaRecord<K, V>>> {
    abstract Map<String, Object> getConsumerConfig();
    abstract List<String> getTopics();
    abstract List<TopicPartition> getTopicPartitions();
    @Nullable abstract Coder<K> getKeyCoder();
    @Nullable abstract Coder<V> getValueCoder();
    @Nullable abstract Class<? extends Deserializer<K>> getKeyDeserializer();
    @Nullable abstract Class<? extends Deserializer<V>> getValueDeserializer();
    abstract SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
        getConsumerFactoryFn();
    @Nullable abstract SerializableFunction<KafkaRecord<K, V>, Instant> getWatermarkFn();

    abstract long getMaxNumRecords();
    @Nullable abstract Duration getMaxReadTime();

    @Nullable abstract Instant getStartReadTime();
    abstract boolean isCommitOffsetsInFinalizeEnabled();

    abstract TimestampPolicyFactory<K, V> getTimestampPolicyFactory();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setConsumerConfig(Map<String, Object> config);
      abstract Builder<K, V> setTopics(List<String> topics);
      abstract Builder<K, V> setTopicPartitions(List<TopicPartition> topicPartitions);
      abstract Builder<K, V> setKeyCoder(Coder<K> keyCoder);
      abstract Builder<K, V> setValueCoder(Coder<V> valueCoder);
      abstract Builder<K, V> setKeyDeserializer(Class<? extends Deserializer<K>> keyDeserializer);
      abstract Builder<K, V> setValueDeserializer(
          Class<? extends Deserializer<V>> valueDeserializer);
      abstract Builder<K, V> setConsumerFactoryFn(
          SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn);
      abstract Builder<K, V> setWatermarkFn(SerializableFunction<KafkaRecord<K, V>, Instant> fn);
      abstract Builder<K, V> setMaxNumRecords(long maxNumRecords);
      abstract Builder<K, V> setMaxReadTime(Duration maxReadTime);
      abstract Builder<K, V> setStartReadTime(Instant startReadTime);
      abstract Builder<K, V> setCommitOffsetsInFinalizeEnabled(boolean commitOffsetInFinalize);
      abstract Builder<K, V> setTimestampPolicyFactory(
        TimestampPolicyFactory<K, V> timestampPolicyFactory);

      abstract Read<K, V> build();
    }

    /**
     * Sets the bootstrap servers for the Kafka consumer.
     */
    public Read<K, V> withBootstrapServers(String bootstrapServers) {
      return updateConsumerProperties(
          ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /**
     * Sets the topic to read from.
     *
     * <p>See {@link KafkaUnboundedSource#split(int, PipelineOptions)} for description
     * of how the partitions are distributed among the splits.
     */
    public Read<K, V> withTopic(String topic) {
      return withTopics(ImmutableList.of(topic));
    }

    /**
     * Sets a list of topics to read from. All the partitions from each
     * of the topics are read.
     *
     * <p>See {@link KafkaUnboundedSource#split(int, PipelineOptions)} for description
     * of how the partitions are distributed among the splits.
     */
    public Read<K, V> withTopics(List<String> topics) {
      checkState(
          getTopicPartitions().isEmpty(), "Only topics or topicPartitions can be set, not both");
      return toBuilder().setTopics(ImmutableList.copyOf(topics)).build();
    }

    /**
     * Sets a list of partitions to read from. This allows reading only a subset
     * of partitions for one or more topics when (if ever) needed.
     *
     * <p>See {@link KafkaUnboundedSource#split(int, PipelineOptions)} for description
     * of how the partitions are distributed among the splits.
     */
    public Read<K, V> withTopicPartitions(List<TopicPartition> topicPartitions) {
      checkState(getTopics().isEmpty(), "Only topics or topicPartitions can be set, not both");
      return toBuilder().setTopicPartitions(ImmutableList.copyOf(topicPartitions)).build();
    }

    /**
     * Sets a Kafka {@link Deserializer} to interpret key bytes read from Kafka.
     *
     * <p>In addition, Beam also needs a {@link Coder} to serialize and deserialize key objects at
     * runtime. KafkaIO tries to infer a coder for the key based on the {@link Deserializer} class,
     * however in case that fails, you can use {@link #withKeyDeserializerAndCoder(Class, Coder)} to
     * provide the key coder explicitly.
     */
    public Read<K, V> withKeyDeserializer(Class<? extends Deserializer<K>> keyDeserializer) {
      return toBuilder().setKeyDeserializer(keyDeserializer).build();
    }

    /**
     * Sets a Kafka {@link Deserializer} for interpreting key bytes read from Kafka along with a
     * {@link Coder} for helping the Beam runner materialize key objects at runtime if necessary.
     *
     * <p>Use this method only if your pipeline doesn't work with plain {@link
     * #withKeyDeserializer(Class)}.
     */
    public Read<K, V> withKeyDeserializerAndCoder(
        Class<? extends Deserializer<K>> keyDeserializer, Coder<K> keyCoder) {
      return toBuilder().setKeyDeserializer(keyDeserializer).setKeyCoder(keyCoder).build();
    }

    /**
     * Sets a Kafka {@link Deserializer} to interpret value bytes read from Kafka.
     *
     * <p>In addition, Beam also needs a {@link Coder} to serialize and deserialize value objects at
     * runtime. KafkaIO tries to infer a coder for the value based on the {@link Deserializer}
     * class, however in case that fails, you can use {@link #withValueDeserializerAndCoder(Class,
     * Coder)} to provide the value coder explicitly.
     */
    public Read<K, V> withValueDeserializer(Class<? extends Deserializer<V>> valueDeserializer) {
      return toBuilder().setValueDeserializer(valueDeserializer).build();
    }

    /**
     * Sets a Kafka {@link Deserializer} for interpreting value bytes read from Kafka along with a
     * {@link Coder} for helping the Beam runner materialize value objects at runtime if necessary.
     *
     * <p>Use this method only if your pipeline doesn't work with plain {@link
     * #withValueDeserializer(Class)}.
     */
    public Read<K, V> withValueDeserializerAndCoder(
        Class<? extends Deserializer<V>> valueDeserializer, Coder<V> valueCoder) {
      return toBuilder().setValueDeserializer(valueDeserializer).setValueCoder(valueCoder).build();
    }

    /**
     * A factory to create Kafka {@link Consumer} from consumer configuration.
     * This is useful for supporting another version of Kafka consumer.
     * Default is {@link KafkaConsumer}.
     */
    public Read<K, V> withConsumerFactoryFn(
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn) {
      return toBuilder().setConsumerFactoryFn(consumerFactoryFn).build();
    }

    /**
     * Update consumer configuration with new properties.
     */
    public Read<K, V> updateConsumerProperties(Map<String, Object> configUpdates) {
      Map<String, Object> config = updateKafkaProperties(getConsumerConfig(),
          IGNORED_CONSUMER_PROPERTIES, configUpdates);
      return toBuilder().setConsumerConfig(config).build();
    }

    /**
     * Similar to {@link org.apache.beam.sdk.io.Read.Unbounded#withMaxNumRecords(long)}.
     * Mainly used for tests and demo applications.
     */
    public Read<K, V> withMaxNumRecords(long maxNumRecords) {
      return toBuilder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Use timestamp to set up start offset.
     * It is only supported by Kafka Client 0.10.1.0 onwards and the message format version
     * after 0.10.0.
     *
     * <p>Note that this take priority over start offset configuration
     * {@code ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} and any auto committed offsets.
     *
     * <p>This results in hard failures in either of the following two cases :
     * 1. If one of more partitions do not contain any messages with timestamp larger than or
     * equal to desired timestamp.
     * 2. If the message format version in a partition is before 0.10.0, i.e. the messages do
     * not have timestamps.
     */
    public Read<K, V> withStartReadTime(Instant startReadTime) {
      return toBuilder().setStartReadTime(startReadTime).build();
    }

    /**
     * Similar to
     * {@link org.apache.beam.sdk.io.Read.Unbounded#withMaxReadTime(Duration)}.
     * Mainly used for tests and demo
     * applications.
     */
    public Read<K, V> withMaxReadTime(Duration maxReadTime) {
      return toBuilder().setMaxReadTime(maxReadTime).build();
    }

    /**
     * Sets {@link TimestampPolicy} to {@link TimestampPolicyFactory.LogAppendTimePolicy}.
     * The policy assigns Kafka's log append time (server side ingestion time)
     * to each record. The watermark for each Kafka partition is the timestamp of the last record
     * read. If a partition is idle, the watermark advances to couple of seconds behind wall time.
     * Every record consumed from Kafka is expected to have its timestamp type set to
     * 'LOG_APPEND_TIME'.
     *
     * <p>In Kafka, log append time needs to be enabled for each topic, and all the subsequent
     * records wil have their timestamp set to log append time. If a record does not have its
     * timestamp type set to 'LOG_APPEND_TIME' for any reason, it's timestamp is set to previous
     * record timestamp or latest watermark, whichever is larger.
     *
     * <p>The watermark for the entire source is the oldest of each partition's watermark.
     * If one of the readers falls behind possibly due to uneven distribution of records among
     * Kafka partitions, it ends up holding the watermark for the entire source.
     */
    public Read<K, V> withLogAppendTime(){
      return withTimestampPolicyFactory(TimestampPolicyFactory.withLogAppendTime());
    }


    /**
     * Sets {@link TimestampPolicy} to {@link TimestampPolicyFactory.ProcessingTimePolicy}.
     * This is the default timestamp policy. It assigns processing time to each record.
     * Specifically, this is the timestamp when the record becomes 'current' in the reader.
     * The watermark aways advances to current time. If servicer side time (log append time) is
     * enabled in Kafka, {@link #withLogAppendTime()} is recommended over this.
     */
    public Read<K, V> withProcessingTime() {
      return withTimestampPolicyFactory(TimestampPolicyFactory.withProcessingTime());
    }

    /**
     * Provide custom {@link TimestampPolicyFactory} to set event times and watermark for each
     * partition. {@link TimestampPolicyFactory#createTimestampPolicy(TopicPartition, Optional)}
     * is invoked for each partition when the reader starts.
     * @see #withLogAppendTime() and {@link #withProcessingTime()}
     */
    public Read<K, V> withTimestampPolicyFactory(
      TimestampPolicyFactory<K, V> timestampPolicyFactory) {
      return toBuilder().setTimestampPolicyFactory(timestampPolicyFactory).build();
    }

    /**
     * A function to assign a timestamp to a record. Default is processing timestamp.
     * @deprecated as of version 2.4.
     * Use {@link #withTimestampPolicyFactory(TimestampPolicyFactory)} instead.
     */
    @Deprecated
    public Read<K, V> withTimestampFn2(
        SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn) {
      checkArgument(timestampFn != null, "timestampFn can not be null");
      return toBuilder().setTimestampPolicyFactory(
        TimestampPolicyFactory.withTimestampFn(timestampFn)).build();
    }

    /**
     * A function to calculate watermark after a record. Default is last record timestamp.
     * @see #withTimestampFn(SerializableFunction)
     * @deprecated as of version 2.4.
     * Use {@link #withTimestampPolicyFactory(TimestampPolicyFactory)} instead.
     */
    @Deprecated
    public Read<K, V> withWatermarkFn2(
        SerializableFunction<KafkaRecord<K, V>, Instant> watermarkFn) {
      checkArgument(watermarkFn != null, "watermarkFn can not be null");
      return toBuilder().setWatermarkFn(watermarkFn).build();
    }

    /**
     * A function to assign a timestamp to a record. Default is processing timestamp.
     * @deprecated as of version 2.4.
     * Use {@link #withTimestampPolicyFactory(TimestampPolicyFactory)} instead.
     */
    @Deprecated
    public Read<K, V> withTimestampFn(SerializableFunction<KV<K, V>, Instant> timestampFn) {
      checkArgument(timestampFn != null, "timestampFn can not be null");
      return withTimestampFn2(unwrapKafkaAndThen(timestampFn));
    }

    /**
     * A function to calculate watermark after a record. Default is last record timestamp.
     * @see #withTimestampFn(SerializableFunction)
     * @deprecated as of version 2.4.
     * Use {@link #withTimestampPolicyFactory(TimestampPolicyFactory)} instead.
     */
    @Deprecated
    public Read<K, V> withWatermarkFn(SerializableFunction<KV<K, V>, Instant> watermarkFn) {
      checkArgument(watermarkFn != null, "watermarkFn can not be null");
      return withWatermarkFn2(unwrapKafkaAndThen(watermarkFn));
    }

    /**
     * Sets "isolation_level" to "read_committed" in Kafka consumer configuration. This is
     * ensures that the consumer does not read uncommitted messages. Kafka version 0.11
     * introduced transactional writes. Applications requiring end-to-end exactly-once
     * semantics should only read committed messages. See JavaDoc for {@link KafkaConsumer}
     * for more description.
     */
    public Read<K, V> withReadCommitted() {
      return updateConsumerProperties(ImmutableMap.of("isolation.level", "read_committed"));
    }

    /**
     * Finalized offsets are committed to Kafka. See {@link CheckpointMark#finalizeCheckpoint()}.
     * It helps with minimizing gaps or duplicate processing of records while restarting a pipeline
     * from scratch. But it does not provide hard processing guarantees. There
     * could be a short delay to commit after {@link CheckpointMark#finalizeCheckpoint()} is
     * invoked, as reader might be blocked on reading from Kafka. Note that it is independent of
     * 'AUTO_COMMIT' Kafka consumer configuration. Usually either this or AUTO_COMMIT in Kafka
     * consumer is enabled, but not both.
     */
    public Read<K, V> commitOffsetsInFinalize() {
      return toBuilder().setCommitOffsetsInFinalizeEnabled(true).build();
    }

    /**
     * Returns a {@link PTransform} for PCollection of {@link KV}, dropping Kafka metatdata.
     */
    public PTransform<PBegin, PCollection<KV<K, V>>> withoutMetadata() {
      return new TypedWithoutMetadata<>(this);
    }

    @Override
    public PCollection<KafkaRecord<K, V>> expand(PBegin input) {
      checkArgument(
          getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) != null,
          "withBootstrapServers() is required");
      checkArgument(getTopics().size() > 0 || getTopicPartitions().size() > 0,
          "Either withTopic(), withTopics() or withTopicPartitions() is required");
      checkArgument(getKeyDeserializer() != null, "withKeyDeserializer() is required");
      checkArgument(getValueDeserializer() != null, "withValueDeserializer() is required");
      ConsumerSpEL consumerSpEL = new ConsumerSpEL();

      if (!consumerSpEL.hasOffsetsForTimes()) {
        LOG.warn("Kafka client version {} is too old. Versions before 0.10.1.0 are deprecated and "
                   + "may not be supported in next release of Apache Beam. "
                   + "Please upgrade your Kafka client version.", AppInfoParser.getVersion());
      }
      if (getStartReadTime() != null) {
        checkArgument(consumerSpEL.hasOffsetsForTimes(),
            "Consumer.offsetsForTimes is only supported by Kafka Client 0.10.1.0 onwards, "
                + "current version of Kafka Client is " + AppInfoParser.getVersion()
                + ". If you are building with maven, set \"kafka.clients.version\" "
                + "maven property to 0.10.1.0 or newer.");
      }
      if (isCommitOffsetsInFinalizeEnabled()) {
        checkArgument(getConsumerConfig().get(ConsumerConfig.GROUP_ID_CONFIG) != null,
          "commitOffsetsInFinalize() is enabled, but group.id in Kafka consumer config "
              + "is not set. Offset management requires group.id.");
        if (Boolean.TRUE.equals(
          getConsumerConfig().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))) {
          LOG.warn("'{}' in consumer config is enabled even though commitOffsetsInFinalize() "
              + "is set. You need only one of them.", ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        }
      }

      // Infer key/value coders if not specified explicitly
      CoderRegistry registry = input.getPipeline().getCoderRegistry();

      Coder<K> keyCoder =
          getKeyCoder() != null ? getKeyCoder() : inferCoder(registry, getKeyDeserializer());
      checkArgument(
          keyCoder != null,
          "Key coder could not be inferred from key deserializer. Please provide"
              + "key coder explicitly using withKeyDeserializerAndCoder()");

      Coder<V> valueCoder =
          getValueCoder() != null ? getValueCoder() : inferCoder(registry, getValueDeserializer());
      checkArgument(
          valueCoder != null,
          "Value coder could not be inferred from value deserializer. Please provide"
              + "value coder explicitly using withValueDeserializerAndCoder()");

      // Handles unbounded source to bounded conversion if maxNumRecords or maxReadTime is set.
      Unbounded<KafkaRecord<K, V>> unbounded =
          org.apache.beam.sdk.io.Read.from(
              toBuilder()
                  .setKeyCoder(keyCoder)
                  .setValueCoder(valueCoder)
                  .build()
                  .makeSource());

      PTransform<PBegin, PCollection<KafkaRecord<K, V>>> transform = unbounded;

      if (getMaxNumRecords() < Long.MAX_VALUE || getMaxReadTime() != null) {
        transform = unbounded
            .withMaxReadTime(getMaxReadTime())
            .withMaxNumRecords(getMaxNumRecords());
      }

      return input.getPipeline().apply(transform);
    }

    /**
     * Creates an {@link UnboundedSource UnboundedSource&lt;KafkaRecord&lt;K, V&gt;, ?&gt;} with the
     * configuration in {@link Read}. Primary use case is unit tests, should not be used in an
     * application.
     */
    @VisibleForTesting
    UnboundedSource<KafkaRecord<K, V>, KafkaCheckpointMark> makeSource() {
      return new KafkaUnboundedSource<>(this, -1);
    }

    // utility method to convert KafkaRecord<K, V> to user KV<K, V> before applying user functions
    private static <KeyT, ValueT, OutT> SerializableFunction<KafkaRecord<KeyT, ValueT>, OutT>
    unwrapKafkaAndThen(final SerializableFunction<KV<KeyT, ValueT>, OutT> fn) {
      return record -> fn.apply(record.getKV());
    }
    ///////////////////////////////////////////////////////////////////////////////////////

    /**
     * A set of properties that are not required or don't make sense for our consumer.
     */
    private static final Map<String, String> IGNORED_CONSUMER_PROPERTIES = ImmutableMap.of(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "Set keyDeserializer instead",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "Set valueDeserializer instead"
        // "group.id", "enable.auto.commit", "auto.commit.interval.ms" :
        //     lets allow these, applications can have better resume point for restarts.
        );

    // set config defaults
    private static final Map<String, Object> DEFAULT_CONSUMER_PROPERTIES =
        ImmutableMap.of(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName(),

            // Use large receive buffer. Once KAFKA-3135 is fixed, this _may_ not be required.
            // with default value of of 32K, It takes multiple seconds between successful polls.
            // All the consumer work is done inside poll(), with smaller send buffer size, it
            // takes many polls before a 1MB chunk from the server is fully read. In my testing
            // about half of the time select() inside kafka consumer waited for 20-30ms, though
            // the server had lots of data in tcp send buffers on its side. Compared to default,
            // this setting increased throughput by many fold (3-4x).
            ConsumerConfig.RECEIVE_BUFFER_CONFIG,
            512 * 1024,

            // default to latest offset when we are not resuming.
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "latest",
            // disable auto commit of offsets. we don't require group_id. could be enabled by user.
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            false);

    // default Kafka 0.9 Consumer supplier.
    private static final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
        KAFKA_CONSUMER_FACTORY_FN = KafkaConsumer::new;

    @SuppressWarnings("unchecked")
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      List<String> topics = getTopics();
      List<TopicPartition> topicPartitions = getTopicPartitions();
      if (topics.size() > 0) {
        builder.add(DisplayData.item("topics", Joiner.on(",").join(topics)).withLabel("Topic/s"));
      } else if (topicPartitions.size() > 0) {
        builder.add(DisplayData.item("topicPartitions", Joiner.on(",").join(topicPartitions))
            .withLabel("Topic Partition/s"));
      }
      Set<String> ignoredConsumerPropertiesKeys = IGNORED_CONSUMER_PROPERTIES.keySet();
      for (Map.Entry<String, Object> conf : getConsumerConfig().entrySet()) {
        String key = conf.getKey();
        if (!ignoredConsumerPropertiesKeys.contains(key)) {
          Object value = DisplayData.inferType(conf.getValue()) != null
              ? conf.getValue() : String.valueOf(conf.getValue());
          builder.add(DisplayData.item(key, ValueProvider.StaticValueProvider.of(value)));
        }
      }
    }
  }

  /**
   * A {@link PTransform} to read from Kafka topics. Similar to {@link KafkaIO.Read}, but
   * removes Kafka metatdata and returns a {@link PCollection} of {@link KV}.
   * See {@link KafkaIO} for more information on usage and configuration of reader.
   */
  public static class TypedWithoutMetadata<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {
    private final Read<K, V> read;

    TypedWithoutMetadata(Read<K, V> read) {
      super("KafkaIO.Read");
      this.read = read;
    }

    @Override
    public PCollection<KV<K, V>> expand(PBegin begin) {
      return begin
          .apply(read)
          .apply("Remove Kafka Metadata",
              ParDo.of(new DoFn<KafkaRecord<K, V>, KV<K, V>>() {
                @ProcessElement
                public void processElement(ProcessContext ctx) {
                  ctx.output(ctx.element().getKV());
                }
              }));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      read.populateDisplayData(builder);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  private static final Logger LOG = LoggerFactory.getLogger(KafkaIO.class);

  /**
   * Returns a new config map which is merge of current config and updates.
   * Verifies the updates do not includes ignored properties.
   */
  private static Map<String, Object> updateKafkaProperties(
      Map<String, Object> currentConfig,
      Map<String, String> ignoredProperties,
      Map<String, Object> updates) {

    for (String key : updates.keySet()) {
      checkArgument(!ignoredProperties.containsKey(key),
          "No need to configure '%s'. %s", key, ignoredProperties.get(key));
    }

    Map<String, Object> config = new HashMap<>(currentConfig);
    config.putAll(updates);

    return config;
  }

  /** Static class, prevent instantiation. */
  private KafkaIO() {}


  //////////////////////// Sink Support \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  /**
   * A {@link PTransform} to write to a Kafka topic. See {@link KafkaIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Write<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {
    @Nullable abstract String getTopic();
    abstract Map<String, Object> getProducerConfig();
    @Nullable
    abstract SerializableFunction<Map<String, Object>, Producer<K, V>> getProducerFactoryFn();

    @Nullable abstract Class<? extends Serializer<K>> getKeySerializer();
    @Nullable abstract Class<? extends Serializer<V>> getValueSerializer();

    // Configuration for EOS sink
    abstract boolean isEOS();
    @Nullable abstract String getSinkGroupId();
    abstract int getNumShards();
    @Nullable abstract
    SerializableFunction<Map<String, Object>, ? extends Consumer<?, ?>> getConsumerFactoryFn();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setTopic(String topic);
      abstract Builder<K, V> setProducerConfig(Map<String, Object> producerConfig);
      abstract Builder<K, V> setProducerFactoryFn(
          SerializableFunction<Map<String, Object>, Producer<K, V>> fn);
      abstract Builder<K, V> setKeySerializer(Class<? extends Serializer<K>> serializer);
      abstract Builder<K, V> setValueSerializer(Class<? extends Serializer<V>> serializer);
      abstract Builder<K, V> setEOS(boolean eosEnabled);
      abstract Builder<K, V> setSinkGroupId(String sinkGroupId);
      abstract Builder<K, V> setNumShards(int numShards);
      abstract Builder<K, V> setConsumerFactoryFn(
          SerializableFunction<Map<String, Object>, ? extends Consumer<?, ?>> fn);
      abstract Write<K, V> build();
    }

    /**
     * Returns a new {@link Write} transform with Kafka producer pointing to
     * {@code bootstrapServers}.
     */
    public Write<K, V> withBootstrapServers(String bootstrapServers) {
      return updateProducerProperties(
          ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /**
     * Sets the Kafka topic to write to.
     */
    public Write<K, V> withTopic(String topic) {
      return toBuilder().setTopic(topic).build();
    }

    /**
     * Sets a {@link Serializer} for serializing key (if any) to bytes.
     *
     * <p>A key is optional while writing to Kafka. Note when a key is set, its hash is used to
     * determine partition in Kafka (see {@link ProducerRecord} for more details).
     */
    public Write<K, V> withKeySerializer(Class<? extends Serializer<K>> keySerializer) {
      return toBuilder().setKeySerializer(keySerializer).build();
    }

    /**
     * Sets a {@link Serializer} for serializing value to bytes.
     */
    public Write<K, V> withValueSerializer(Class<? extends Serializer<V>> valueSerializer) {
      return toBuilder().setValueSerializer(valueSerializer).build();
    }

    /**
     * Adds the given producer properties, overriding old values of properties with the same key.
     */
    public Write<K, V> updateProducerProperties(Map<String, Object> configUpdates) {
      Map<String, Object> config = updateKafkaProperties(getProducerConfig(),
          IGNORED_PRODUCER_PROPERTIES, configUpdates);
      return toBuilder().setProducerConfig(config).build();
    }

    /**
     * Sets a custom function to create Kafka producer. Primarily used
     * for tests. Default is {@link KafkaProducer}
     */
    public Write<K, V> withProducerFactoryFn(
        SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn) {
      return toBuilder().setProducerFactoryFn(producerFactoryFn).build();
    }

    /**
     * Provides exactly-once semantics while writing to Kafka, which enables applications with
     * end-to-end exactly-once guarantees on top of exactly-once semantics <i>within</i> Beam
     * pipelines. It ensures that records written to sink are committed on Kafka exactly once,
     * even in the case of retries during pipeline execution even when some processing is retried.
     * Retries typically occur when workers restart (as in failure recovery), or when the work is
     * redistributed (as in an autoscaling event).
     *
     * <p>Beam runners typically provide exactly-once semantics for results of a pipeline, but not
     * for side effects from user code in transform.  If a transform such as Kafka sink writes
     * to an external system, those writes might occur more than once. When EOS is enabled here,
     * the sink transform ties checkpointing semantics in compatible Beam runners and transactions
     * in Kafka (version 0.11+) to ensure a record is written only once. As the implementation
     * relies on runners checkpoint semantics, not all the runners are compatible. The sink throws
     * an exception during initialization if the runner is not whitelisted. Flink runner is
     * one of the runners whose checkpoint semantics are not compatible with current
     * implementation (hope to provide a solution in near future). Dataflow runner and Spark
     * runners are whitelisted as compatible.
     *
     * <p>Note on performance: Exactly-once sink involves two shuffles of the records. In addition
     * to cost of shuffling the records among workers, the records go through 2
     * serialization-deserialization cycles. Depending on volume and cost of serialization,
     * the CPU cost might be noticeable. The CPU cost can be reduced by writing byte arrays
     * (i.e. serializing them to byte before writing to Kafka sink).
     *
     * @param numShards Sets sink parallelism. The state metadata stored on Kafka is stored across
     *    this many virtual partitions using {@code sinkGroupId}. A good rule of thumb is to set
     *    this to be around number of partitions in Kafka topic.
     *
     * @param sinkGroupId The <i>group id</i> used to store small amount of state as metadata on
     *    Kafka. It is similar to <i>consumer group id</i> used with a {@link KafkaConsumer}. Each
     *    job should use a unique group id so that restarts/updates of job preserve the state to
     *    ensure exactly-once semantics. The state is committed atomically with sink transactions
     *    on Kafka. See {@link KafkaProducer#sendOffsetsToTransaction(Map, String)} for more
     *    information. The sink performs multiple sanity checks during initialization to catch
     *    common mistakes so that it does not end up using state that does not <i>seem</i> to
     *    be written by the same job.
     */
    public Write<K, V> withEOS(int numShards, String sinkGroupId) {
      KafkaExactlyOnceSink.ensureEOSSupport();
      checkArgument(numShards >= 1, "numShards should be >= 1");
      checkArgument(sinkGroupId != null, "sinkGroupId is required for exactly-once sink");
      return toBuilder()
        .setEOS(true)
        .setNumShards(numShards)
        .setSinkGroupId(sinkGroupId)
        .build();
    }

    /**
     * When exactly-once semantics are enabled (see {@link #withEOS(int, String)}), the sink needs
     * to fetch previously stored state with Kafka topic. Fetching the metadata requires a
     * consumer. Similar to {@link Read#withConsumerFactoryFn(SerializableFunction)}, a factory
     * function can be supplied if required in a specific case.
     * The default is {@link KafkaConsumer}.
     */
    public Write<K, V> withConsumerFactoryFn(
        SerializableFunction<Map<String, Object>, ? extends Consumer<?, ?>> consumerFactoryFn) {
      return toBuilder().setConsumerFactoryFn(consumerFactoryFn).build();
    }

    /**
     * Writes just the values to Kafka. This is useful for writing collections of values rather
     * thank {@link KV}s.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public PTransform<PCollection<V>, PDone> values() {
      return new KafkaValueWrite<>(
          toBuilder()
          .setKeySerializer((Class) StringSerializer.class)
          .build()
      );
    }

    @Override
    public PDone expand(PCollection<KV<K, V>> input) {
      checkArgument(
        getProducerConfig().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) != null,
        "withBootstrapServers() is required");
      checkArgument(getTopic() != null, "withTopic() is required");
      checkArgument(getKeySerializer() != null, "withKeySerializer() is required");
      checkArgument(getValueSerializer() != null, "withValueSerializer() is required");

      if (isEOS()) {
        KafkaExactlyOnceSink.ensureEOSSupport();

        // TODO: Verify that the group_id does not have existing state stored on Kafka unless
        //       this is an upgrade. This avoids issues with simple mistake of reusing group_id
        //       across multiple runs or across multiple jobs. This is checked when the sink
        //       transform initializes while processing the output. It might be better to
        //       check here to catch common mistake.

        input.apply(new KafkaExactlyOnceSink<>(this));
      } else {
        input.apply(ParDo.of(new KafkaWriter<>(this)));
      }
      return PDone.in(input.getPipeline());
    }

    public void validate(PipelineOptions options) {
      if (isEOS()) {
        String runner = options.getRunner().getName();
        if (runner.equals("org.apache.beam.runners.direct.DirectRunner")
          || runner.startsWith("org.apache.beam.runners.dataflow.")
          || runner.startsWith("org.apache.beam.runners.spark.")) {
          return;
        }
        throw new UnsupportedOperationException(
          runner + " is not whitelisted among runners compatible with Kafka exactly-once sink. "
          + "This implementation of exactly-once sink relies on specific checkpoint guarantees. "
          + "Only the runners with known to have compatible checkpoint semantics are whitelisted."
        );
      }
    }

    // set config defaults
    private static final Map<String, Object> DEFAULT_PRODUCER_PROPERTIES =
        ImmutableMap.of(ProducerConfig.RETRIES_CONFIG, 3);

    /**
     * A set of properties that are not required or don't make sense for our producer.
     */
    private static final Map<String, String> IGNORED_PRODUCER_PROPERTIES = ImmutableMap.of(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "Use withKeySerializer instead",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "Use withValueSerializer instead"
     );

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("topic", getTopic()).withLabel("Topic"));
      Set<String> ignoredProducerPropertiesKeys = IGNORED_PRODUCER_PROPERTIES.keySet();
      for (Map.Entry<String, Object> conf : getProducerConfig().entrySet()) {
        String key = conf.getKey();
        if (!ignoredProducerPropertiesKeys.contains(key)) {
          Object value = DisplayData.inferType(conf.getValue()) != null
              ? conf.getValue() : String.valueOf(conf.getValue());
          builder.add(DisplayData.item(key, ValueProvider.StaticValueProvider.of(value)));
        }
      }
    }
  }

  /**
   * Same as {@code Write<K, V>} without a Key. Null is used for key as it is the convention is
   * Kafka when there is no key specified. Majority of Kafka writers don't specify a key.
   */
  private static class KafkaValueWrite<K, V> extends PTransform<PCollection<V>, PDone> {
    private final Write<K, V> kvWriteTransform;

    private KafkaValueWrite(Write<K, V> kvWriteTransform) {
      this.kvWriteTransform = kvWriteTransform;
    }

    @Override
    public PDone expand(PCollection<V> input) {
      return input
          .apply(
              "Kafka values with default key",
              MapElements.via(
                  new SimpleFunction<V, KV<K, V>>() {
                    @Override
                    public KV<K, V> apply(V element) {
                      return KV.of(null, element);
                    }
                  }))
          .setCoder(KvCoder.of(new NullOnlyCoder<>(), input.getCoder()))
          .apply(kvWriteTransform);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      kvWriteTransform.populateDisplayData(builder);
    }
  }


  private static class NullOnlyCoder<T> extends AtomicCoder<T> {
    @Override
    public void encode(T value, OutputStream outStream) {
      checkArgument(value == null, "Can only encode nulls");
      // Encode as no bytes.
    }

    @Override
    public T decode(InputStream inStream) {
      return null;
    }
  }


  /**
   * Attempt to infer a {@link Coder} by extracting the type of the deserialized-class from the
   * deserializer argument using the {@link Coder} registry.
   */
  @VisibleForTesting
  static <T> NullableCoder<T> inferCoder(
      CoderRegistry coderRegistry, Class<? extends Deserializer<T>> deserializer) {
    checkNotNull(deserializer);

    for (Type type : deserializer.getGenericInterfaces()) {
      if (!(type instanceof ParameterizedType)) {
        continue;
      }

      // This does not recurse: we will not infer from a class that extends
      // a class that extends Deserializer<T>.
      ParameterizedType parameterizedType = (ParameterizedType) type;

      if (parameterizedType.getRawType() == Deserializer.class) {
        Type parameter = parameterizedType.getActualTypeArguments()[0];

        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) parameter;

        try {
          return NullableCoder.of(coderRegistry.getCoder(clazz));
        } catch (CannotProvideCoderException e) {
          throw new RuntimeException(
              String.format("Unable to automatically infer a Coder for "
                                + "the Kafka Deserializer %s: no coder registered for type %s",
                            deserializer, clazz));
        }
      }
    }

    throw new RuntimeException(String.format(
        "Could not extract the Kafka Deserializer type from %s", deserializer));
  }
}
