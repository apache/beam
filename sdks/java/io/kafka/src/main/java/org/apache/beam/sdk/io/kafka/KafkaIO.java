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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.io.kafka.KafkaCheckpointMark.PartitionMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.ExposedByteArrayInputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded source and a sink for <a href="http://kafka.apache.org/">Kafka</a> topics.
 * Kafka version 0.9 and 0.10 are supported. If you need a specific version of Kafka
 * client(e.g. 0.9 for 0.9 servers, or 0.10 for security features), specify explicit
 * kafka-client dependency.
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
 * <p>To configure a Kafka source, you must specify at the minimum Kafka <tt>bootstrapServers</tt>
 * and one or more topics to consume. The following example illustrates various options for
 * configuring the source :
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(KafkaIO.<Long, String>read()
 *       .withBootstrapServers("broker_1:9092,broker_2:9092")
 *       .withTopic("my_topic")  // use withTopics(List<String>) to read from multiple topics.
 *       // set a Coder for Key and Value
 *       .withKeyCoder(BigEndianLongCoder.of())
 *       .withValueCoder(StringUtf8Coder.of())
 *       // above four are required configuration. returns PCollection<KafkaRecord<Long, String>>
 *
 *       // rest of the settings are optional :
 *
 *       // you can further customize KafkaConsumer used to read the records by adding more
 *       // settings for ConsumerConfig. e.g :
 *       .updateConsumerProperties(ImmutableMap.of("receive.buffer.bytes", 1024 * 1024))
 *
 *       // custom function for calculating record timestamp (default is processing time)
 *       .withTimestampFn(new MyTypestampFunction())
 *
 *       // custom function for watermark (default is record timestamp)
 *       .withWatermarkFn(new MyWatermarkFunction())
 *
 *       // finally, if you don't need Kafka metadata, you can drop it
 *       .withoutMetadata() // PCollection<KV<Long, String>>
 *    )
 *    .apply(Values.<String>create()) // PCollection<String>
 *     ...
 * }</pre>
 *
 * <h3>Partition Assignment and Checkpointing</h3>
 * The Kafka partitions are evenly distributed among splits (workers).
 * Checkpointing is fully supported and each split can resume from previous checkpoint. See
 * {@link UnboundedKafkaSource#split(int, PipelineOptions)} for more details on
 * splits and checkpoint support.
 *
 * <p>When the pipeline starts for the first time without any checkpoint, the source starts
 * consuming from the <em>latest</em> offsets. You can override this behavior to consume from the
 * beginning by setting appropriate appropriate properties in {@link ConsumerConfig}, through
 * {@link Read#updateConsumerProperties(Map)}.
 *
 * <h3>Writing to Kafka</h3>
 *
 * <p>KafkaIO sink supports writing key-value pairs to a Kafka topic. Users can also write
 * just the values. To configure a Kafka sink, you must specify at the minimum Kafka
 * <tt>bootstrapServers</tt> and the topic to write to. The following example illustrates various
 * options for configuring the sink:
 *
 * <pre>{@code
 *
 *  PCollection<KV<Long, String>> kvColl = ...;
 *  kvColl.apply(KafkaIO.<Long, String>write()
 *       .withBootstrapServers("broker_1:9092,broker_2:9092")
 *       .withTopic("results")
 *
 *       // set Coder for Key and Value
 *       .withKeyCoder(BigEndianLongCoder.of())
 *       .withValueCoder(StringUtf8Coder.of())
 *
 *       // you can further customize KafkaProducer used to write the records by adding more
 *       // settings for ProducerConfig. e.g, to enable compression :
 *       .updateProducerProperties(ImmutableMap.of("compression.type", "gzip"))
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
 *      .withValueCoder(StringUtf8Coder.of()) // just need coder for value
 *      .values()
 *    );
 * }</pre>
 *
 * <h3>Advanced Kafka Configuration</h3>
 * KafakIO allows setting most of the properties in {@link ConsumerConfig} for source or in
 * {@link ProducerConfig} for sink. E.g. if you would like to enable offset
 * <em>auto commit</em> (for external monitoring or other purposes), you can set
 * <tt>"group.id"</tt>, <tt>"enable.auto.commit"</tt>, etc.
 *
 * <h3>Event Timestamp and Watermark</h3>
 * By default record timestamp and watermark are based on processing time in KafkaIO reader.
 * This can be overridden by providing {@code WatermarkFn} with
 * {@link Read#withWatermarkFn(SerializableFunction)}, and {@code TimestampFn} with
 * {@link Read#withTimestampFn(SerializableFunction)}.<br>
 * Note that {@link KafkaRecord#getTimestamp()} reflects timestamp provided by Kafka if any,
 * otherwise it is set to processing time.
 */
@Experimental
public class KafkaIO {
  /**
   * Creates an uninitialized {@link Read} {@link PTransform}. Before use, basic Kafka
   * configuration should set with {@link Read#withBootstrapServers(String)} and
   * {@link Read#withTopics(List)}. Other optional settings include key and value coders,
   * custom timestamp and watermark functions.
   */
  public static Read<byte[], byte[]> readBytes() {
    return new AutoValue_KafkaIO_Read.Builder<byte[], byte[]>()
        .setTopics(new ArrayList<String>())
        .setTopicPartitions(new ArrayList<TopicPartition>())
        .setKeyCoder(ByteArrayCoder.of())
        .setValueCoder(ByteArrayCoder.of())
        .setConsumerFactoryFn(Read.KAFKA_CONSUMER_FACTORY_FN)
        .setConsumerConfig(Read.DEFAULT_CONSUMER_PROPERTIES)
        .setMaxNumRecords(Long.MAX_VALUE)
        .build();
  }

  /**
   * Creates an uninitialized {@link Read} {@link PTransform}. Before use, basic Kafka
   * configuration should set with {@link Read#withBootstrapServers(String)} and
   * {@link Read#withTopics(List)}. Other optional settings include key and value coders,
   * custom timestamp and watermark functions.
   */
  public static <K, V> Read<K, V> read() {
    return new AutoValue_KafkaIO_Read.Builder<K, V>()
        .setTopics(new ArrayList<String>())
        .setTopicPartitions(new ArrayList<TopicPartition>())
        .setConsumerFactoryFn(Read.KAFKA_CONSUMER_FACTORY_FN)
        .setConsumerConfig(Read.DEFAULT_CONSUMER_PROPERTIES)
        .setMaxNumRecords(Long.MAX_VALUE)
        .build();
  }

  /**
   * Creates an uninitialized {@link Write} {@link PTransform}. Before use, Kafka configuration
   * should be set with {@link Write#withBootstrapServers(String)} and {@link Write#withTopic}
   * along with {@link Coder}s for (optional) key and values.
   */
  public static <K, V> Write<K, V> write() {
    return new AutoValue_KafkaIO_Write.Builder<K, V>()
        .setProducerConfig(Write.DEFAULT_PRODUCER_PROPERTIES)
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
    abstract SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
        getConsumerFactoryFn();
    @Nullable abstract SerializableFunction<KafkaRecord<K, V>, Instant> getTimestampFn();
    @Nullable abstract SerializableFunction<KafkaRecord<K, V>, Instant> getWatermarkFn();

    abstract long getMaxNumRecords();
    @Nullable abstract Duration getMaxReadTime();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setConsumerConfig(Map<String, Object> config);
      abstract Builder<K, V> setTopics(List<String> topics);
      abstract Builder<K, V> setTopicPartitions(List<TopicPartition> topicPartitions);
      abstract Builder<K, V> setKeyCoder(Coder<K> keyCoder);
      abstract Builder<K, V> setValueCoder(Coder<V> valueCoder);
      abstract Builder<K, V> setConsumerFactoryFn(
          SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn);
      abstract Builder<K, V> setTimestampFn(SerializableFunction<KafkaRecord<K, V>, Instant> fn);
      abstract Builder<K, V> setWatermarkFn(SerializableFunction<KafkaRecord<K, V>, Instant> fn);
      abstract Builder<K, V> setMaxNumRecords(long maxNumRecords);
      abstract Builder<K, V> setMaxReadTime(Duration maxReadTime);

      abstract Read<K, V> build();
    }

    /**
     * Returns a new {@link Read} with Kafka consumer pointing to {@code bootstrapServers}.
     */
    public Read<K, V> withBootstrapServers(String bootstrapServers) {
      return updateConsumerProperties(
          ImmutableMap.<String, Object>of(
              ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /**
     * Returns a new {@link Read} that reads from the topic.
     * See {@link UnboundedKafkaSource#split(int, PipelineOptions)} for description
     * of how the partitions are distributed among the splits.
     */
    public Read<K, V> withTopic(String topic) {
      return withTopics(ImmutableList.of(topic));
    }

    /**
     * Returns a new {@link Read} that reads from the topics. All the partitions from each
     * of the topics are read.
     * See {@link UnboundedKafkaSource#split(int, PipelineOptions)} for description
     * of how the partitions are distributed among the splits.
     */
    public Read<K, V> withTopics(List<String> topics) {
      checkState(
          getTopicPartitions().isEmpty(), "Only topics or topicPartitions can be set, not both");
      return toBuilder().setTopics(ImmutableList.copyOf(topics)).build();
    }

    /**
     * Returns a new {@link Read} that reads from the partitions. This allows reading only a subset
     * of partitions for one or more topics when (if ever) needed.
     * See {@link UnboundedKafkaSource#split(int, PipelineOptions)} for description
     * of how the partitions are distributed among the splits.
     */
    public Read<K, V> withTopicPartitions(List<TopicPartition> topicPartitions) {
      checkState(getTopics().isEmpty(), "Only topics or topicPartitions can be set, not both");
      return toBuilder().setTopicPartitions(ImmutableList.copyOf(topicPartitions)).build();
    }

    /**
     * Returns a new {@link Read} with {@link Coder} for key bytes.
     */
    public Read<K, V> withKeyCoder(Coder<K> keyCoder) {
      return toBuilder().setKeyCoder(keyCoder).build();
    }

    /**
     * Returns a new {@link Read} with {@link Coder} for value bytes.
     */
    public Read<K, V> withValueCoder(Coder<V> valueCoder) {
      return toBuilder().setValueCoder(valueCoder).build();
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
      return toBuilder().setMaxNumRecords(maxNumRecords).setMaxReadTime(null).build();
    }

    /**
     * Similar to
     * {@link org.apache.beam.sdk.io.Read.Unbounded#withMaxReadTime(Duration)}.
     * Mainly used for tests and demo
     * applications.
     */
    public Read<K, V> withMaxReadTime(Duration maxReadTime) {
      return toBuilder().setMaxNumRecords(Long.MAX_VALUE).setMaxReadTime(maxReadTime).build();
    }

    /**
     * A function to assign a timestamp to a record. Default is processing timestamp.
     */
    public Read<K, V> withTimestampFn2(
        SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn) {
      checkNotNull(timestampFn);
      return toBuilder().setTimestampFn(timestampFn).build();
    }

    /**
     * A function to calculate watermark after a record. Default is last record timestamp
     * @see #withTimestampFn(SerializableFunction)
     */
    public Read<K, V> withWatermarkFn2(
        SerializableFunction<KafkaRecord<K, V>, Instant> watermarkFn) {
      checkNotNull(watermarkFn);
      return toBuilder().setWatermarkFn(watermarkFn).build();
    }

    /**
     * A function to assign a timestamp to a record. Default is processing timestamp.
     */
    public Read<K, V> withTimestampFn(SerializableFunction<KV<K, V>, Instant> timestampFn) {
      checkNotNull(timestampFn);
      return withTimestampFn2(unwrapKafkaAndThen(timestampFn));
    }

    /**
     * A function to calculate watermark after a record. Default is last record timestamp
     * @see #withTimestampFn(SerializableFunction)
     */
    public Read<K, V> withWatermarkFn(SerializableFunction<KV<K, V>, Instant> watermarkFn) {
      checkNotNull(watermarkFn);
      return withWatermarkFn2(unwrapKafkaAndThen(watermarkFn));
    }

    /**
     * Returns a {@link PTransform} for PCollection of {@link KV}, dropping Kafka metatdata.
     */
    public PTransform<PBegin, PCollection<KV<K, V>>> withoutMetadata() {
      return new TypedWithoutMetadata<K, V>(this);
    }

    @Override
    public void validate(PBegin input) {
      checkNotNull(getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
          "Kafka bootstrap servers should be set");
      checkArgument(getTopics().size() > 0 || getTopicPartitions().size() > 0,
          "Kafka topics or topic_partitions are required");
      checkNotNull(getKeyCoder(), "Key coder must be set");
      checkNotNull(getValueCoder(), "Value coder must be set");
    }

    @Override
    public PCollection<KafkaRecord<K, V>> expand(PBegin input) {
     // Handles unbounded source to bounded conversion if maxNumRecords or maxReadTime is set.
      Unbounded<KafkaRecord<K, V>> unbounded =
          org.apache.beam.sdk.io.Read.from(makeSource());

      PTransform<PBegin, PCollection<KafkaRecord<K, V>>> transform = unbounded;

      if (getMaxNumRecords() < Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(getMaxNumRecords());
      } else if (getMaxReadTime() != null) {
        transform = unbounded.withMaxReadTime(getMaxReadTime());
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
      return new UnboundedKafkaSource<K, V>(this, -1);
    }

    // utility method to convert KafkRecord<K, V> to user KV<K, V> before applying user functions
    private static <KeyT, ValueT, OutT> SerializableFunction<KafkaRecord<KeyT, ValueT>, OutT>
    unwrapKafkaAndThen(final SerializableFunction<KV<KeyT, ValueT>, OutT> fn) {
      return new SerializableFunction<KafkaRecord<KeyT, ValueT>, OutT>() {
        @Override
        public OutT apply(KafkaRecord<KeyT, ValueT> record) {
          return fn.apply(record.getKV());
        }
      };
    }
    ///////////////////////////////////////////////////////////////////////////////////////

    /**
     * A set of properties that are not required or don't make sense for our consumer.
     */
    private static final Map<String, String> IGNORED_CONSUMER_PROPERTIES = ImmutableMap.of(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "Set keyCoder instead",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "Set valueCoder instead"
        // "group.id", "enable.auto.commit", "auto.commit.interval.ms" :
        //     lets allow these, applications can have better resume point for restarts.
        );

    // set config defaults
    private static final Map<String, Object> DEFAULT_CONSUMER_PROPERTIES =
        ImmutableMap.<String, Object>of(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),

            // Use large receive buffer. Once KAFKA-3135 is fixed, this _may_ not be required.
            // with default value of of 32K, It takes multiple seconds between successful polls.
            // All the consumer work is done inside poll(), with smaller send buffer size, it
            // takes many polls before a 1MB chunk from the server is fully read. In my testing
            // about half of the time select() inside kafka consumer waited for 20-30ms, though
            // the server had lots of data in tcp send buffers on its side. Compared to default,
            // this setting increased throughput increased by many fold (3-4x).
            ConsumerConfig.RECEIVE_BUFFER_CONFIG, 512 * 1024,

            // default to latest offset when we are not resuming.
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
            // disable auto commit of offsets. we don't require group_id. could be enabled by user.
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    // default Kafka 0.9 Consumer supplier.
    private static final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
      KAFKA_CONSUMER_FACTORY_FN =
        new SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>() {
          @Override
          public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
            return new KafkaConsumer<>(config);
          }
        };

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
          builder.add(DisplayData.item(key, ValueProvider.StaticValueProvider.of(conf.getValue())));
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
      return read
          .expand(begin)
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

  private static class UnboundedKafkaSource<K, V>
      extends UnboundedSource<KafkaRecord<K, V>, KafkaCheckpointMark> {
    private Read<K, V> spec;
    private final int id; // split id, mainly for debugging

    public UnboundedKafkaSource(Read<K, V> spec, int id) {
      this.spec = spec;
      this.id = id;
    }

    /**
     * The partitions are evenly distributed among the splits. The number of splits returned is
     * {@code min(desiredNumSplits, totalNumPartitions)}, though better not to depend on the exact
     * count.
     *
     * <p>It is important to assign the partitions deterministically so that we can support
     * resuming a split from last checkpoint. The Kafka partitions are sorted by
     * {@code <topic, partition>} and then assigned to splits in round-robin order.
     */
    @Override
    public List<UnboundedKafkaSource<K, V>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {

      List<TopicPartition> partitions = new ArrayList<>(spec.getTopicPartitions());

      // (a) fetch partitions for each topic
      // (b) sort by <topic, partition>
      // (c) round-robin assign the partitions to splits

      if (partitions.isEmpty()) {
        try (Consumer<?, ?> consumer =
            spec.getConsumerFactoryFn().apply(spec.getConsumerConfig())) {
          for (String topic : spec.getTopics()) {
            for (PartitionInfo p : consumer.partitionsFor(topic)) {
              partitions.add(new TopicPartition(p.topic(), p.partition()));
            }
          }
        }
      }

      Collections.sort(partitions, new Comparator<TopicPartition>() {
        @Override
        public int compare(TopicPartition tp1, TopicPartition tp2) {
          return ComparisonChain
              .start()
              .compare(tp1.topic(), tp2.topic())
              .compare(tp1.partition(), tp2.partition())
              .result();
        }
      });

      checkArgument(desiredNumSplits > 0);
      checkState(partitions.size() > 0,
          "Could not find any partitions. Please check Kafka configuration and topic names");

      int numSplits = Math.min(desiredNumSplits, partitions.size());
      List<List<TopicPartition>> assignments = new ArrayList<>(numSplits);

      for (int i = 0; i < numSplits; i++) {
        assignments.add(new ArrayList<TopicPartition>());
      }
      for (int i = 0; i < partitions.size(); i++) {
        assignments.get(i % numSplits).add(partitions.get(i));
      }

      List<UnboundedKafkaSource<K, V>> result = new ArrayList<>(numSplits);

      for (int i = 0; i < numSplits; i++) {
        List<TopicPartition> assignedToSplit = assignments.get(i);

        LOG.info("Partitions assigned to split {} (total {}): {}",
            i, assignedToSplit.size(), Joiner.on(",").join(assignedToSplit));

        result.add(
            new UnboundedKafkaSource<>(
                spec.toBuilder()
                    .setTopics(Collections.<String>emptyList())
                    .setTopicPartitions(assignedToSplit)
                    .build(),
                i));
      }

      return result;
    }

    @Override
    public UnboundedKafkaReader<K, V> createReader(PipelineOptions options,
                                                   KafkaCheckpointMark checkpointMark) {
      if (spec.getTopicPartitions().isEmpty()) {
        LOG.warn("Looks like generateSplits() is not called. Generate single split.");
        try {
          return new UnboundedKafkaReader<K, V>(
              split(1, options).get(0), checkpointMark);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return new UnboundedKafkaReader<K, V>(this, checkpointMark);
    }

    @Override
    public Coder<KafkaCheckpointMark> getCheckpointMarkCoder() {
      return AvroCoder.of(KafkaCheckpointMark.class);
    }

    @Override
    public boolean requiresDeduping() {
      // Kafka records are ordered with in partitions. In addition checkpoint guarantees
      // records are not consumed twice.
      return false;
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public Coder<KafkaRecord<K, V>> getDefaultOutputCoder() {
      return KafkaRecordCoder.of(spec.getKeyCoder(), spec.getValueCoder());
    }
  }

  private static class UnboundedKafkaReader<K, V> extends UnboundedReader<KafkaRecord<K, V>> {

    private final UnboundedKafkaSource<K, V> source;
    private final String name;
    private Consumer<byte[], byte[]> consumer;
    private final List<PartitionState> partitionStates;
    private KafkaRecord<K, V> curRecord;
    private Instant curTimestamp;
    private Iterator<PartitionState> curBatch = Collections.emptyIterator();

    private static final Duration KAFKA_POLL_TIMEOUT = Duration.millis(1000);
    private static final Duration NEW_RECORDS_POLL_TIMEOUT = Duration.millis(10);

    // Use a separate thread to read Kafka messages. Kafka Consumer does all its work including
    // network I/O inside poll(). Polling only inside #advance(), especially with a small timeout
    // like 100 milliseconds does not work well. This along with large receive buffer for
    // consumer achieved best throughput in tests (see `defaultConsumerProperties`).
    private final ExecutorService consumerPollThread = Executors.newSingleThreadExecutor();
    private final SynchronousQueue<ConsumerRecords<byte[], byte[]>> availableRecordsQueue =
        new SynchronousQueue<>();
    private AtomicBoolean closed = new AtomicBoolean(false);

    // Backlog support :
    // Kafka consumer does not have an API to fetch latest offset for topic. We need to seekToEnd()
    // then look at position(). Use another consumer to do this so that the primary consumer does
    // not need to be interrupted. The latest offsets are fetched periodically on another thread.
    // This is still a hack. There could be unintended side effects, e.g. if user enabled offset
    // auto commit in consumer config, this could interfere with the primary consumer (we will
    // handle this particular problem). We might have to make this optional.
    private Consumer<byte[], byte[]> offsetConsumer;
    private final ScheduledExecutorService offsetFetcherThread =
        Executors.newSingleThreadScheduledExecutor();
    private static final int OFFSET_UPDATE_INTERVAL_SECONDS = 5;

    private static final long UNINITIALIZED_OFFSET = -1;

    //Add SpEL instance to cover the interface difference of Kafka client
    private transient ConsumerSpEL consumerSpEL;

    /** watermark before any records have been read. */
    private static Instant initialWatermark = new Instant(Long.MIN_VALUE);

    @Override
    public String toString() {
      return name;
    }

    // maintains state of each assigned partition (buffered records, consumed offset, etc)
    private static class PartitionState {
      private final TopicPartition topicPartition;
      private long nextOffset;
      private long latestOffset;
      private Iterator<ConsumerRecord<byte[], byte[]>> recordIter = Collections.emptyIterator();

      // simple moving average for size of each record in bytes
      private double avgRecordSize = 0;
      private static final int movingAvgWindow = 1000; // very roughly avg of last 1000 elements

      PartitionState(TopicPartition partition, long nextOffset) {
        this.topicPartition = partition;
        this.nextOffset = nextOffset;
        this.latestOffset = UNINITIALIZED_OFFSET;
      }

      // update consumedOffset and avgRecordSize
      void recordConsumed(long offset, int size) {
        nextOffset = offset + 1;

        // this is always updated from single thread. probably not worth making it an AtomicDouble
        if (avgRecordSize <= 0) {
          avgRecordSize = size;
        } else {
          // initially, first record heavily contributes to average.
          avgRecordSize += ((size - avgRecordSize) / movingAvgWindow);
        }
      }

      synchronized void setLatestOffset(long latestOffset) {
        this.latestOffset = latestOffset;
      }

      synchronized long approxBacklogInBytes() {
        // Note that is an an estimate of uncompressed backlog.
        if (latestOffset < 0 || nextOffset < 0) {
          return UnboundedReader.BACKLOG_UNKNOWN;
        }
        return Math.max(0, (long) ((latestOffset - nextOffset) * avgRecordSize));
      }
    }

    public UnboundedKafkaReader(
        UnboundedKafkaSource<K, V> source,
        @Nullable KafkaCheckpointMark checkpointMark) {
      this.consumerSpEL = new ConsumerSpEL();
      this.source = source;
      this.name = "Reader-" + source.id;

      List<TopicPartition> partitions = source.spec.getTopicPartitions();
      partitionStates = ImmutableList.copyOf(Lists.transform(partitions,
          new Function<TopicPartition, PartitionState>() {
            @Override
            public PartitionState apply(TopicPartition tp) {
              return new PartitionState(tp, UNINITIALIZED_OFFSET);
            }
        }));

      if (checkpointMark != null) {
        // a) verify that assigned and check-pointed partitions match exactly
        // b) set consumed offsets

        checkState(checkpointMark.getPartitions().size() == partitions.size(),
            "checkPointMark and assignedPartitions should match");

        for (int i = 0; i < partitions.size(); i++) {
          PartitionMark ckptMark = checkpointMark.getPartitions().get(i);
          TopicPartition assigned = partitions.get(i);
          TopicPartition partition = new TopicPartition(ckptMark.getTopic(),
                                                        ckptMark.getPartition());
          checkState(partition.equals(assigned),
                     "checkpointed partition %s and assigned partition %s don't match",
                     partition, assigned);

          partitionStates.get(i).nextOffset = ckptMark.getNextOffset();
        }
      }
    }

    private void consumerPollLoop() {
      // Read in a loop and enqueue the batch of records, if any, to availableRecordsQueue
      while (!closed.get()) {
        try {
          ConsumerRecords<byte[], byte[]> records = consumer.poll(KAFKA_POLL_TIMEOUT.getMillis());
          if (!records.isEmpty() && !closed.get()) {
            availableRecordsQueue.put(records); // blocks until dequeued.
          }
        } catch (InterruptedException e) {
          LOG.warn("{}: consumer thread is interrupted", this, e); // not expected
          break;
        } catch (WakeupException e) {
          break;
        }
      }

      LOG.info("{}: Returning from consumer pool loop", this);
    }

    private void nextBatch() {
      curBatch = Collections.emptyIterator();

      ConsumerRecords<byte[], byte[]> records;
      try {
        // poll available records, wait (if necessary) up to the specified timeout.
        records = availableRecordsQueue.poll(NEW_RECORDS_POLL_TIMEOUT.getMillis(),
                                             TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("{}: Unexpected", this, e);
        return;
      }

      if (records == null) {
        return;
      }

      List<PartitionState> nonEmpty = new LinkedList<>();

      for (PartitionState p : partitionStates) {
        p.recordIter = records.records(p.topicPartition).iterator();
        if (p.recordIter.hasNext()) {
          nonEmpty.add(p);
        }
      }

      // cycle through the partitions in order to interleave records from each.
      curBatch = Iterators.cycle(nonEmpty);
    }

    @Override
    public boolean start() throws IOException {
      Read<K, V> spec = source.spec;
      consumer = spec.getConsumerFactoryFn().apply(spec.getConsumerConfig());
      consumerSpEL.evaluateAssign(consumer, spec.getTopicPartitions());

      for (PartitionState p : partitionStates) {
        if (p.nextOffset != UNINITIALIZED_OFFSET) {
          consumer.seek(p.topicPartition, p.nextOffset);
        } else {
          // nextOffset is unininitialized here, meaning start reading from latest record as of now
          // ('latest' is the default, and is configurable). Remember the current position without
          // waiting until the first record read. This ensures checkpoint is accurate even if the
          // reader is closed before reading any records.
          p.nextOffset = consumer.position(p.topicPartition);
        }

        LOG.info("{}: reading from {} starting at offset {}", name, p.topicPartition, p.nextOffset);
      }

      // Start consumer read loop.
      // Note that consumer is not thread safe, should not be accessed out side consumerPollLoop().
      consumerPollThread.submit(
          new Runnable() {
            @Override
            public void run() {
              consumerPollLoop();
            }
          });

      // offsetConsumer setup :

      Object groupId = spec.getConsumerConfig().get(ConsumerConfig.GROUP_ID_CONFIG);
      // override group_id and disable auto_commit so that it does not interfere with main consumer
      String offsetGroupId = String.format("%s_offset_consumer_%d_%s", name,
          (new Random()).nextInt(Integer.MAX_VALUE), (groupId == null ? "none" : groupId));
      Map<String, Object> offsetConsumerConfig = new HashMap<>(spec.getConsumerConfig());
      offsetConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, offsetGroupId);
      offsetConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

      offsetConsumer = spec.getConsumerFactoryFn().apply(offsetConsumerConfig);
      consumerSpEL.evaluateAssign(offsetConsumer, spec.getTopicPartitions());

      offsetFetcherThread.scheduleAtFixedRate(
          new Runnable() {
            @Override
            public void run() {
              updateLatestOffsets();
            }
          }, 0, OFFSET_UPDATE_INTERVAL_SECONDS, TimeUnit.SECONDS);

      nextBatch();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      /* Read first record (if any). we need to loop here because :
       *  - (a) some records initially need to be skipped if they are before consumedOffset
       *  - (b) if curBatch is empty, we want to fetch next batch and then advance.
       *  - (c) curBatch is an iterator of iterators. we interleave the records from each.
       *        curBatch.next() might return an empty iterator.
       */
      while (true) {
        if (curBatch.hasNext()) {
          PartitionState pState = curBatch.next();

          if (!pState.recordIter.hasNext()) { // -- (c)
            pState.recordIter = Collections.emptyIterator(); // drop ref
            curBatch.remove();
            continue;
          }

          ConsumerRecord<byte[], byte[]> rawRecord = pState.recordIter.next();
          long expected = pState.nextOffset;
          long offset = rawRecord.offset();

          if (offset < expected) { // -- (a)
            // this can happen when compression is enabled in Kafka (seems to be fixed in 0.10)
            // should we check if the offset is way off from consumedOffset (say > 1M)?
            LOG.warn("{}: ignoring already consumed offset {} for {}",
                this, offset, pState.topicPartition);
            continue;
          }

          // sanity check
          if (offset != expected) {
            LOG.warn("{}: gap in offsets for {} at {}. {} records missing.",
                this, pState.topicPartition, expected, offset - expected);
          }

          if (curRecord == null) {
            LOG.info("{}: first record offset {}", name, offset);
          }

          curRecord = null; // user coders below might throw.

          // apply user coders. might want to allow skipping records that fail to decode.
          // TODO: wrap exceptions from coders to make explicit to users
          KafkaRecord<K, V> record = new KafkaRecord<K, V>(
              rawRecord.topic(),
              rawRecord.partition(),
              rawRecord.offset(),
              consumerSpEL.getRecordTimestamp(rawRecord),
              decode(rawRecord.key(), source.spec.getKeyCoder()),
              decode(rawRecord.value(), source.spec.getValueCoder()));

          curTimestamp = (source.spec.getTimestampFn() == null)
              ? Instant.now() : source.spec.getTimestampFn().apply(record);
          curRecord = record;

          int recordSize = (rawRecord.key() == null ? 0 : rawRecord.key().length)
              + (rawRecord.value() == null ? 0 : rawRecord.value().length);
          pState.recordConsumed(offset, recordSize);
          return true;

        } else { // -- (b)
          nextBatch();

          if (!curBatch.hasNext()) {
            return false;
          }
        }
      }
    }

    private static byte[] nullBytes = new byte[0];
    private static <T> T decode(byte[] bytes, Coder<T> coder) throws IOException {
      // If 'bytes' is null, use byte[0]. It is common for key in Kakfa record to be null.
      // This makes it impossible for user to distinguish between zero length byte and null.
      // Alternately, we could have a ByteArrayCoder that handles nulls, and use that for default
      // coder.
      byte[] toDecode = bytes == null ? nullBytes : bytes;
      return coder.decode(new ExposedByteArrayInputStream(toDecode), Coder.Context.OUTER);
    }

    // update latest offset for each partition.
    // called from offsetFetcher thread
    private void updateLatestOffsets() {
      for (PartitionState p : partitionStates) {
        try {
          consumerSpEL.evaluateSeek2End(offsetConsumer, p.topicPartition);
          long offset = offsetConsumer.position(p.topicPartition);
          p.setLatestOffset(offset);
        } catch (Exception e) {
          // An exception is expected if we've closed the reader in another thread. Ignore and exit.
          if (closed.get()) {
            break;
          }
          LOG.warn("{}: exception while fetching latest offset for partition {}. will be retried.",
              this, p.topicPartition, e);
          p.setLatestOffset(UNINITIALIZED_OFFSET); // reset
        }

        LOG.debug("{}: latest offset update for {} : {} (consumer offset {}, avg record size {})",
            this, p.topicPartition, p.latestOffset, p.nextOffset, p.avgRecordSize);
      }

      LOG.debug("{}:  backlog {}", this, getSplitBacklogBytes());
    }

    @Override
    public Instant getWatermark() {
      if (curRecord == null) {
        LOG.debug("{}: getWatermark() : no records have been read yet.", name);
        return initialWatermark;
      }

      return source.spec.getWatermarkFn() != null
          ? source.spec.getWatermarkFn().apply(curRecord) : curTimestamp;
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new KafkaCheckpointMark(ImmutableList.copyOf(// avoid lazy (consumedOffset can change)
          Lists.transform(partitionStates,
              new Function<PartitionState, PartitionMark>() {
                @Override
                public PartitionMark apply(PartitionState p) {
                  return new PartitionMark(p.topicPartition.topic(),
                                           p.topicPartition.partition(),
                                           p.nextOffset);
                }
              }
          )));
    }

    @Override
    public UnboundedSource<KafkaRecord<K, V>, ?> getCurrentSource() {
      return source;
    }

    @Override
    public KafkaRecord<K, V> getCurrent() throws NoSuchElementException {
      // should we delay updating consumed offset till this point? Mostly not required.
      return curRecord;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return curTimestamp;
    }


    @Override
    public long getSplitBacklogBytes() {
      long backlogBytes = 0;

      for (PartitionState p : partitionStates) {
        long pBacklog = p.approxBacklogInBytes();
        if (pBacklog == UnboundedReader.BACKLOG_UNKNOWN) {
          return UnboundedReader.BACKLOG_UNKNOWN;
        }
        backlogBytes += pBacklog;
      }

      return backlogBytes;
    }

    @Override
    public void close() throws IOException {
      closed.set(true);
      consumerPollThread.shutdown();
      offsetFetcherThread.shutdown();

      boolean isShutdown = false;

      // Wait for threads to shutdown. Trying this as a loop to handle a tiny race where poll thread
      // might block to enqueue right after availableRecordsQueue.poll() below.
      while (!isShutdown) {

        consumer.wakeup();
        offsetConsumer.wakeup();
        availableRecordsQueue.poll(); // drain unread batch, this unblocks consumer thread.
        try {
          isShutdown = consumerPollThread.awaitTermination(10, TimeUnit.SECONDS)
              && offsetFetcherThread.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e); // not expected
        }

        if (!isShutdown) {
          LOG.warn("An internal thread is taking a long time to shutdown. will retry.");
        }
      }

      Closeables.close(offsetConsumer, true);
      Closeables.close(consumer, true);
    }
  }

  //////////////////////// Sink Support \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  /**
   * A {@link PTransform} to write to a Kafka topic. See {@link KafkaIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Write<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {
    @Nullable abstract String getTopic();
    @Nullable abstract Coder<K> getKeyCoder();
    @Nullable abstract Coder<V> getValueCoder();
    abstract Map<String, Object> getProducerConfig();
    @Nullable
    abstract SerializableFunction<Map<String, Object>, Producer<K, V>> getProducerFactoryFn();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setTopic(String topic);
      abstract Builder<K, V> setKeyCoder(Coder<K> keyCoder);
      abstract Builder<K, V> setValueCoder(Coder<V> valueCoder);
      abstract Builder<K, V> setProducerConfig(Map<String, Object> producerConfig);
      abstract Builder<K, V> setProducerFactoryFn(
          SerializableFunction<Map<String, Object>, Producer<K, V>> fn);
      abstract Write<K, V> build();
    }

    /**
     * Returns a new {@link Write} transform with Kafka producer pointing to
     * {@code bootstrapServers}.
     */
    public Write<K, V> withBootstrapServers(String bootstrapServers) {
      return updateProducerProperties(
          ImmutableMap.<String, Object>of(
              ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /**
     * Returns a new {@link Write} transform that writes to given topic.
     */
    public Write<K, V> withTopic(String topic) {
      return toBuilder().setTopic(topic).build();
    }

    /**
     * Returns a new {@link Write} with {@link Coder} for serializing key (if any) to bytes.
     * A key is optional while writing to Kafka. Note when a key is set, its hash is used to
     * determine partition in Kafka (see {@link ProducerRecord} for more details).
     */
    public Write<K, V> withKeyCoder(Coder<K> keyCoder) {
      return toBuilder().setKeyCoder(keyCoder).build();
    }

    /**
     * Returns a new {@link Write} with {@link Coder} for serializing value to bytes.
     */
    public Write<K, V> withValueCoder(Coder<V> valueCoder) {
      return toBuilder().setValueCoder(valueCoder).build();
    }

    public Write<K, V> updateProducerProperties(Map<String, Object> configUpdates) {
      Map<String, Object> config = updateKafkaProperties(getProducerConfig(),
          IGNORED_PRODUCER_PROPERTIES, configUpdates);
      return toBuilder().setProducerConfig(config).build();
    }

    /**
     * Returns a new {@link Write} with a custom function to create Kafka producer. Primarily used
     * for tests. Default is {@link KafkaProducer}
     */
    public Write<K, V> withProducerFactoryFn(
        SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn) {
      return toBuilder().setProducerFactoryFn(producerFactoryFn).build();
    }

    /**
     * Returns a new transform that writes just the values to Kafka. This is useful for writing
     * collections of values rather thank {@link KV}s.
     */
    public PTransform<PCollection<V>, PDone> values() {
      return new KafkaValueWrite<>(withKeyCoder(new NullOnlyCoder<K>()).toBuilder().build());
    }

    @Override
    public PDone expand(PCollection<KV<K, V>> input) {
      input.apply(ParDo.of(new KafkaWriter<>(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<KV<K, V>> input) {
      checkNotNull(getProducerConfig().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
          "Kafka bootstrap servers should be set");
      checkNotNull(getTopic(), "Kafka topic should be set");
      checkNotNull(getKeyCoder(), "Key coder should be set");
      checkNotNull(getValueCoder(), "Value coder should be set");
    }

    // set config defaults
    private static final Map<String, Object> DEFAULT_PRODUCER_PROPERTIES =
        ImmutableMap.<String, Object>of(
            ProducerConfig.RETRIES_CONFIG, 3,
            // See comment about custom serializers in KafkaWriter constructor.
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, CoderBasedKafkaSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CoderBasedKafkaSerializer.class);

    /**
     * A set of properties that are not required or don't make sense for our producer.
     */
    private static final Map<String, String> IGNORED_PRODUCER_PROPERTIES = ImmutableMap.of(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "Set keyCoder instead",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "Set valueCoder instead",
        configForKeySerializer(), "Reserved for internal serializer",
        configForValueSerializer(), "Reserved for internal serializer"
     );

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("topic", getTopic()).withLabel("Topic"));
      Set<String> ignoredProducerPropertiesKeys = IGNORED_PRODUCER_PROPERTIES.keySet();
      for (Map.Entry<String, Object> conf : getProducerConfig().entrySet()) {
        String key = conf.getKey();
        if (!ignoredProducerPropertiesKeys.contains(key)) {
          builder.add(DisplayData.item(key, ValueProvider.StaticValueProvider.of(conf.getValue())));
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
        .apply("Kafka values with default key",
          MapElements.via(new SimpleFunction<V, KV<K, V>>() {
            @Override
            public KV<K, V> apply(V element) {
              return KV.of(null, element);
            }
          }))
        .setCoder(KvCoder.of(new NullOnlyCoder<K>(), kvWriteTransform.getValueCoder()))
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
    public void encode(T value, OutputStream outStream, Context context) {
      checkArgument(value == null, "Can only encode nulls");
      // Encode as the empty string.
    }

    @Override
    public T decode(InputStream inStream, Context context) {
      return null;
    }
  }

  private static class KafkaWriter<K, V> extends DoFn<KV<K, V>, Void> {

    @Setup
    public void setup() {
      if (spec.getProducerFactoryFn() != null) {
        producer = spec.getProducerFactoryFn().apply(producerConfig);
      } else {
        producer = new KafkaProducer<K, V>(producerConfig);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) throws Exception {
      checkForFailures();

      KV<K, V> kv = ctx.element();
      producer.send(
          new ProducerRecord<K, V>(spec.getTopic(), kv.getKey(), kv.getValue()),
          new SendCallback());
    }

    @FinishBundle
    public void finishBundle(Context c) throws IOException {
      producer.flush();
      checkForFailures();
    }

    @Teardown
    public void teardown() {
      producer.close();
    }

    ///////////////////////////////////////////////////////////////////////////////////

    private final Write<K, V> spec;
    private final Map<String, Object> producerConfig;

    private transient Producer<K, V> producer = null;
    //private transient Callback sendCallback = new SendCallback();
    // first exception and number of failures since last invocation of checkForFailures():
    private transient Exception sendException = null;
    private transient long numSendFailures = 0;

    KafkaWriter(Write<K, V> spec) {
      this.spec = spec;

      // Set custom kafka serializers. We do not want to serialize user objects then pass the bytes
      // to producer since key and value objects are used in Kafka Partitioner interface.
      // This does not matter for default partitioner in Kafka as it uses just the serialized
      // key bytes to pick a partition. But we don't want to limit use of custom partitions.
      // We pass key and values objects the user writes directly Kafka and user supplied
      // coders to serialize them are invoked inside CoderBasedKafkaSerializer.
      // Use case : write all the events for a single session to same Kafka partition.

      this.producerConfig = new HashMap<>(spec.getProducerConfig());
      this.producerConfig.put(configForKeySerializer(), spec.getKeyCoder());
      this.producerConfig.put(configForValueSerializer(), spec.getValueCoder());
    }

    private synchronized void checkForFailures() throws IOException {
      if (numSendFailures == 0) {
        return;
      }

      String msg = String.format(
          "KafkaWriter : failed to send %d records (since last report)", numSendFailures);

      Exception e = sendException;
      sendException = null;
      numSendFailures = 0;

      LOG.warn(msg);
      throw new IOException(msg, e);
    }

    private class SendCallback implements Callback {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
          return;
        }

        synchronized (KafkaWriter.this) {
          if (sendException == null) {
            sendException = exception;
          }
          numSendFailures++;
        }
        // don't log exception stacktrace here, exception will be propagated up.
        LOG.warn("KafkaWriter send failed : '{}'", exception.getMessage());
      }
    }
  }

  /**
   * Implements Kafka's {@link Serializer} with a {@link Coder}. The coder is stored as serialized
   * value in producer configuration map.
   */
  public static class CoderBasedKafkaSerializer<T> implements Serializer<T> {

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      String configKey = isKey ? configForKeySerializer() : configForValueSerializer();
      coder = (Coder<T>) configs.get(configKey);
      checkNotNull(coder, "could not instantiate coder for Kafka serialization");
    }

    @Override
    public byte[] serialize(String topic, @Nullable T data) {
      if (data == null) {
        return null; // common for keys to be null
      }

      try {
        return CoderUtils.encodeToByteArray(coder, data);
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
    }

    private Coder<T> coder = null;
    private static final String CONFIG_FORMAT = "beam.coder.based.kafka.%s.serializer";
  }


  private static String configForKeySerializer() {
    return String.format(CoderBasedKafkaSerializer.CONFIG_FORMAT, "key");
  }

  private static String configForValueSerializer() {
    return String.format(CoderBasedKafkaSerializer.CONFIG_FORMAT, "value");
  }
}
