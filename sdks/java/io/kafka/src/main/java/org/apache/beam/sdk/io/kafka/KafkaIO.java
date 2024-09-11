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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadImplementation;
import org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadImplementationCompatibilityException;
import org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadImplementationCompatibilityResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.BadRecordErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.DefaultErrorHandler;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.MonotonicallyIncreasing;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.WallTime;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.construction.PTransformMatchers;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Comparators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded source and a sink for <a href="http://kafka.apache.org/">Kafka</a> topics.
 *
 * <h2>Read from Kafka as {@link UnboundedSource}</h2>
 *
 * <h3>Reading from Kafka topics</h3>
 *
 * <p>KafkaIO source returns unbounded collection of Kafka records as {@code
 * PCollection<KafkaRecord<K, V>>}. A {@link KafkaRecord} includes basic metadata like
 * topic-partition and offset, along with key and value associated with a Kafka record.
 *
 * <p>Although most applications consume a single topic, the source can be configured to consume
 * multiple topics or even a specific set of {@link TopicPartition}s.
 *
 * <p>To configure a Kafka source, you must specify at the minimum Kafka <tt>bootstrapServers</tt>,
 * one or more topics to consume, and key and value deserializers. For example:
 *
 * <pre>{@code
 * pipeline
 *   .apply(KafkaIO.<Long, String>read()
 *      .withBootstrapServers("broker_1:9092,broker_2:9092")
 *      .withTopic("my_topic")  // use withTopics(List<String>) to read from multiple topics.
 *      .withKeyDeserializer(LongDeserializer.class)
 *      .withValueDeserializer(StringDeserializer.class)
 *
 *      // Above four are required configuration. returns PCollection<KafkaRecord<Long, String>>
 *
 *      // Rest of the settings are optional :
 *
 *      // you can further customize KafkaConsumer used to read the records by adding more
 *      // settings for ConsumerConfig. e.g :
 *      .withConsumerConfigUpdates(ImmutableMap.of("group.id", "my_beam_app_1"))
 *
 *      // set event times and watermark based on 'LogAppendTime'. To provide a custom
 *      // policy see withTimestampPolicyFactory(). withProcessingTime() is the default.
 *      // Use withCreateTime() with topics that have 'CreateTime' timestamps.
 *      .withLogAppendTime()
 *
 *      // restrict reader to committed messages on Kafka (see method documentation).
 *      .withReadCommitted()
 *
 *      // offset consumed by the pipeline can be committed back.
 *      .commitOffsetsInFinalize()
 *
 *      // Specified a serializable function which can determine whether to stop reading from given
 *      // TopicPartition during runtime. Note that only {@link ReadFromKafkaDoFn} respect the
 *      // signal.
 *      .withCheckStopReadingFn(new SerializedFunction<TopicPartition, Boolean>() {})
 *
 *      //If you would like to send messages that fail to be parsed from Kafka to an alternate sink,
 *      //use the error handler pattern as defined in {@link ErrorHandler}
 *      .withBadRecordErrorHandler(errorHandler)
 *
 *      // finally, if you don't need Kafka metadata, you can drop it.g
 *      .withoutMetadata() // PCollection<KV<Long, String>>
 *   )
 *   .apply(Values.<String>create()) // PCollection<String>
 *    ...
 * }</pre>
 *
 * <p>Kafka provides deserializers for common types in {@link
 * org.apache.kafka.common.serialization}. In addition to deserializers, Beam runners need {@link
 * Coder} to materialize key and value objects if necessary. In most cases, you don't need to
 * specify {@link Coder} for key and value in the resulting collection because the coders are
 * inferred from deserializer types. However, in cases when coder inference fails, they can be
 * specified explicitly along with deserializers using {@link
 * Read#withKeyDeserializerAndCoder(Class, Coder)} and {@link
 * Read#withValueDeserializerAndCoder(Class, Coder)}. Note that Kafka messages are interpreted using
 * key and value <i>deserializers</i>.
 *
 * <h3>Read From Kafka Dynamically</h3>
 *
 * For a given kafka bootstrap_server, KafkaIO is also able to detect and read from available {@link
 * TopicPartition} dynamically and stop reading from un. KafkaIO uses {@link
 * WatchForKafkaTopicPartitions} to emit any new added {@link TopicPartition} and uses {@link
 * ReadFromKafkaDoFn} to read from each {@link KafkaSourceDescriptor}. Dynamic read is able to solve
 * 2 scenarios:
 *
 * <ul>
 *   <li>Certain topic or partition is added/deleted.
 *   <li>Certain topic or partition is added, then removed but added back again
 * </ul>
 *
 * Within providing {@code checkStopReadingFn}, there are 2 more cases that dynamic read can handle:
 *
 * <ul>
 *   <li>Certain topic or partition is stopped
 *   <li>Certain topic or partition is added, then stopped but added back again
 * </ul>
 *
 * Race conditions may happen under 2 supported cases:
 *
 * <ul>
 *   <li>A TopicPartition is removed, but added backed again
 *   <li>A TopicPartition is stopped, then want to read it again
 * </ul>
 *
 * When race condition happens, it will result in the stopped/removed TopicPartition failing to be
 * emitted to ReadFromKafkaDoFn again. Or ReadFromKafkaDoFn will output replicated records. The
 * major cause for such race condition is that both {@link WatchForKafkaTopicPartitions} and {@link
 * ReadFromKafkaDoFn} react to the signal from removed/stopped {@link TopicPartition} but we cannot
 * guarantee that both DoFns perform related actions at the same time.
 *
 * <p>Here is one example for failing to emit new added {@link TopicPartition}:
 *
 * <ul>
 *   <li>A {@link WatchForKafkaTopicPartitions} is configured with updating the current tracking set
 *       every 1 hour.
 *   <li>One TopicPartition A is tracked by the {@link WatchForKafkaTopicPartitions} at 10:00AM and
 *       {@link ReadFromKafkaDoFn} starts to read from TopicPartition A immediately.
 *   <li>At 10:30AM, the {@link WatchForKafkaTopicPartitions} notices that the {@link
 *       TopicPartition} has been stopped/removed, so it stops reading from it and returns {@code
 *       ProcessContinuation.stop()}.
 *   <li>At 10:45 the pipeline author wants to read from TopicPartition A again.
 *   <li>At 11:00AM when {@link WatchForKafkaTopicPartitions} is invoked by firing timer, it doesn't
 *       know that TopicPartition A has been stopped/removed. All it knows is that TopicPartition A
 *       is still an active TopicPartition and it will not emit TopicPartition A again.
 * </ul>
 *
 * Another race condition example for producing duplicate records:
 *
 * <ul>
 *   <li>At 10:00AM, {@link ReadFromKafkaDoFn} is processing TopicPartition A
 *   <li>At 10:05AM, {@link ReadFromKafkaDoFn} starts to process other TopicPartitions(sdf-initiated
 *       checkpoint or runner-issued checkpoint happens)
 *   <li>At 10:10AM, {@link WatchForKafkaTopicPartitions} knows that TopicPartition A is
 *       stopped/removed
 *   <li>At 10:15AM, {@link WatchForKafkaTopicPartitions} knows that TopicPartition A is added again
 *       and emits TopicPartition A again
 *   <li>At 10:20AM, {@link ReadFromKafkaDoFn} starts to process resumed TopicPartition A but at the
 *       same time {@link ReadFromKafkaDoFn} is also processing the new emitted TopicPartitionA.
 * </ul>
 *
 * For more design details, please refer to
 * https://docs.google.com/document/d/1FU3GxVRetHPLVizP3Mdv6mP5tpjZ3fd99qNjUI5DT5k/. To enable
 * dynamic read, you can write a pipeline like:
 *
 * <pre>{@code
 * pipeline
 *   .apply(KafkaIO.<Long, String>read()
 *      // Configure the dynamic read with 1 hour, where the pipeline will look into available
 *      // TopicPartitions and emit new added ones every 1 hour.
 *      .withDynamicRead(Duration.standardHours(1))
 *      .withCheckStopReadingFn(new SerializedFunction<TopicPartition, Boolean>() {})
 *      .withBootstrapServers("broker_1:9092,broker_2:9092")
 *      .withKeyDeserializer(LongDeserializer.class)
 *      .withValueDeserializer(StringDeserializer.class)
 *   )
 *   .apply(Values.<String>create()) // PCollection<String>
 *    ...
 * }</pre>
 *
 * <h3>Partition Assignment and Checkpointing</h3>
 *
 * The Kafka partitions are evenly distributed among splits (workers).
 *
 * <p>Checkpointing is fully supported and each split can resume from previous checkpoint (to the
 * extent supported by runner). See {@link KafkaUnboundedSource#split(int, PipelineOptions)} for
 * more details on splits and checkpoint support.
 *
 * <p>When the pipeline starts for the first time, or without any checkpoint, the source starts
 * consuming from the <em>latest</em> offsets. You can override this behavior to consume from the
 * beginning by setting properties appropriately in {@link ConsumerConfig}, through {@link
 * Read#withConsumerConfigUpdates(Map)}. You can also enable offset auto_commit in Kafka to resume
 * from last committed.
 *
 * <p>In summary, KafkaIO.read follows below sequence to set initial offset:<br>
 * 1. {@link KafkaCheckpointMark} provided by runner;<br>
 * 2. Consumer offset stored in Kafka when {@code ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG = true};
 * <br>
 * 3. Start from <em>latest</em> offset by default;
 *
 * <p>Seek to initial offset is a blocking operation in Kafka API, which can block forever for
 * certain versions of Kafka client library. This is resolved by <a
 * href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-266%3A+Fix+consumer+indefinite+blocking+behavior">KIP-266</a>
 * which provides `default.api.timeout.ms` consumer config setting to control such timeouts.
 * KafkaIO.read implements timeout itself, to not to block forever in case older Kafka client is
 * used. It does recognize `default.api.timeout.ms` setting and will honor the timeout value if it
 * is passes in consumer config.
 *
 * <h3>Use Avro schema with Confluent Schema Registry</h3>
 *
 * <p>If you want to deserialize the keys and/or values based on a schema available in Confluent
 * Schema Registry, KafkaIO can fetch this schema from a specified Schema Registry URL and use it
 * for deserialization. A {@link Coder} will be inferred automatically based on the respective
 * {@link Deserializer}.
 *
 * <p>For an Avro schema it will return a {@link PCollection} of {@link KafkaRecord}s where key
 * and/or value will be typed as {@link org.apache.avro.generic.GenericRecord}. In this case, users
 * don't need to specify key or/and value deserializers and coders since they will be set to {@link
 * KafkaAvroDeserializer} and {@link AvroCoder} by default accordingly.
 *
 * <p>For example, below topic values are serialized with Avro schema stored in Schema Registry,
 * keys are typed as {@link Long}:
 *
 * <pre>{@code
 * PCollection<KafkaRecord<Long, GenericRecord>> input = pipeline
 *   .apply(KafkaIO.<Long, GenericRecord>read()
 *      .withBootstrapServers("broker_1:9092,broker_2:9092")
 *      .withTopic("my_topic")
 *      .withKeyDeserializer(LongDeserializer.class)
 *      // Use Confluent Schema Registry, specify schema registry URL and value subject
 *      .withValueDeserializer(
 *          ConfluentSchemaRegistryDeserializerProvider.of("http://localhost:8081", "my_topic-value"))
 *    ...
 * }</pre>
 *
 * <p>You can also pass properties to the schema registry client allowing you to configure
 * authentication
 *
 * <pre>{@code
 * ImmutableMap<String, Object> csrConfig =
 *     ImmutableMap.<String, Object>builder()
 *         .put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO")
 *         .put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"<username>:<password>")
 *         .build();
 *
 * PCollection<KafkaRecord<Long, GenericRecord>> input = pipeline
 *   .apply(KafkaIO.<Long, GenericRecord>read()
 *      .withBootstrapServers("broker_1:9092,broker_2:9092")
 *      .withTopic("my_topic")
 *      .withKeyDeserializer(LongDeserializer.class)
 *      // Use Confluent Schema Registry, specify schema registry URL, value subject and schema registry client configuration
 *      .withValueDeserializer(
 *          ConfluentSchemaRegistryDeserializerProvider.of("https://localhost:8081", "my_topic-value", null, csrConfig))
 *    ...
 * }</pre>
 *
 * <h2>Read from Kafka as a {@link DoFn}</h2>
 *
 * {@link ReadSourceDescriptors} is the {@link PTransform} that takes a PCollection of {@link
 * KafkaSourceDescriptor} as input and outputs a PCollection of {@link KafkaRecord}. The core
 * implementation is based on {@code SplittableDoFn}. For more details about the concept of {@code
 * SplittableDoFn}, please refer to the <a
 * href="https://beam.apache.org/blog/splittable-do-fn/">blog post</a> and <a
 * href="https://s.apache.org/beam-fn-api">design doc</a>. The major difference from {@link
 * KafkaIO.Read} is, {@link ReadSourceDescriptors} doesn't require source descriptions(e.g., {@link
 * KafkaIO.Read#getTopicPattern()}, {@link KafkaIO.Read#getTopicPartitions()}, {@link
 * KafkaIO.Read#getTopics()}, {@link KafkaIO.Read#getStartReadTime()}, etc.) during the pipeline
 * construction time. Instead, the pipeline can populate these source descriptions during runtime.
 * For example, the pipeline can query Kafka topics from a BigQuery table and read these topics via
 * {@link ReadSourceDescriptors}.
 *
 * <h3>Common Kafka Consumer Configurations</h3>
 *
 * <p>Most Kafka consumer configurations are similar to {@link KafkaIO.Read}:
 *
 * <ul>
 *   <li>{@link ReadSourceDescriptors#getConsumerConfig()} is the same as {@link
 *       KafkaIO.Read#getConsumerConfig()}.
 *   <li>{@link ReadSourceDescriptors#getConsumerFactoryFn()} is the same as {@link
 *       KafkaIO.Read#getConsumerFactoryFn()}.
 *   <li>{@link ReadSourceDescriptors#getOffsetConsumerConfig()} is the same as {@link
 *       KafkaIO.Read#getOffsetConsumerConfig()}.
 *   <li>{@link ReadSourceDescriptors#getKeyCoder()} is the same as {@link
 *       KafkaIO.Read#getKeyCoder()}.
 *   <li>{@link ReadSourceDescriptors#getValueCoder()} is the same as {@link
 *       KafkaIO.Read#getValueCoder()}.
 *   <li>{@link ReadSourceDescriptors#getKeyDeserializerProvider()} is the same as {@link
 *       KafkaIO.Read#getKeyDeserializerProvider()}.
 *   <li>{@link ReadSourceDescriptors#getValueDeserializerProvider()} is the same as {@link
 *       KafkaIO.Read#getValueDeserializerProvider()}.
 *   <li>{@link ReadSourceDescriptors#isCommitOffsetEnabled()} has the same meaning as {@link
 *       KafkaIO.Read#isCommitOffsetsInFinalizeEnabled()}.
 * </ul>
 *
 * <p>For example, to create a basic {@link ReadSourceDescriptors} transform:
 *
 * <pre>{@code
 * pipeline
 *  .apply(Create.of(
 *    KafkaSourceDescriptor.of(
 *      new TopicPartition("topic", 1),
 *      null,
 *      null,
 *      null,
 *      null,
 *      null)))
 *   .apply(
 *     KafkaIO.<Long, String>readSourceDescriptors()
 *       .withBootstrapServers("broker_1:9092,broker_2:9092")
 *       .withKeyDeserializer(LongDeserializer.class)
 *       .withValueDeserializer(StringDeserializer.class));
 * }</pre>
 *
 * Note that the {@code bootstrapServers} can also be populated from the {@link
 * KafkaSourceDescriptor}:
 *
 * <pre>{@code
 * pipeline
 *  .apply(Create.of(
 *    KafkaSourceDescriptor.of(
 *      new TopicPartition("topic", 1),
 *      null,
 *      null,
 *      null,
 *      null,
 *      ImmutableList.of("broker_1:9092", "broker_2:9092"))))
 *  .apply(KafkaIO.<Long, String>readSourceDescriptors()
 *         .withKeyDeserializer(LongDeserializer.class).
 *         .withValueDeserializer(StringDeserializer.class));
 * }</pre>
 *
 * <h3>Configurations of {@link ReadSourceDescriptors}</h3>
 *
 * <p>Except configurations of Kafka Consumer, there are some other configurations which are related
 * to processing records.
 *
 * <p>{@link ReadSourceDescriptors#commitOffsets()} enables committing offset after processing the
 * record. Note that if {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG} is set in the consumer
 * config, the {@link ReadSourceDescriptors#commitOffsets()} will be ignored.
 *
 * <p>{@link ReadSourceDescriptors#withExtractOutputTimestampFn(SerializableFunction)} is used to
 * compute the {@code output timestamp} for a given {@link KafkaRecord} and controls the watermark
 * advancement. There are three built-in types:
 *
 * <ul>
 *   <li>{@link ReadSourceDescriptors#withProcessingTime()}
 *   <li>{@link ReadSourceDescriptors#withCreateTime()}
 *   <li>{@link ReadSourceDescriptors#withLogAppendTime()}
 * </ul>
 *
 * <p>For example, to create a {@link ReadSourceDescriptors} with this additional configuration:
 *
 * <pre>{@code
 * pipeline
 *  .apply(Create.of(
 *    KafkaSourceDescriptor.of(
 *      new TopicPartition("topic", 1),
 *      null,
 *      null,
 *      null,
 *      null,
 *      ImmutableList.of("broker_1:9092", "broker_2:9092"))))
 * .apply(KafkaIO.<Long, String>readSourceDescriptors()
 *        .withKeyDeserializer(LongDeserializer.class).
 *        .withValueDeserializer(StringDeserializer.class)
 *        .withProcessingTime()
 *        .commitOffsets());
 * }</pre>
 *
 * <h3>Writing to Kafka</h3>
 *
 * <p>KafkaIO sink supports writing key-value pairs to a Kafka topic. Users can also write just the
 * values or native Kafka producer records using {@link
 * org.apache.kafka.clients.producer.ProducerRecord}. To configure a Kafka sink, you must specify at
 * the minimum Kafka <tt>bootstrapServers</tt>, the topic to write to, and key and value
 * serializers. For example:
 *
 * <pre>{@code
 * PCollection<KV<Long, String>> kvColl = ...;
 * kvColl.apply(KafkaIO.<Long, String>write()
 *      .withBootstrapServers("broker_1:9092,broker_2:9092")
 *      .withTopic("results")
 *
 *      .withKeySerializer(LongSerializer.class)
 *      .withValueSerializer(StringSerializer.class)
 *
 *      // You can further customize KafkaProducer used to write the records by adding more
 *      // settings for ProducerConfig. e.g, to enable compression :
 *      .withProducerConfigUpdates(ImmutableMap.of("compression.type", "gzip"))
 *
 *      // You set publish timestamp for the Kafka records.
 *      .withInputTimestamp() // element timestamp is used while publishing to Kafka
 *      // or you can also set a custom timestamp with a function.
 *      .withPublishTimestampFunction((elem, elemTs) -> ...)
 *
 *      // Optionally, records that fail to serialize can be sent to an error handler
 *      // See {@link ErrorHandler} for details of for details of configuring a bad record error
 *      // handler
 *      .withBadRecordErrorHandler(errorHandler)
 *
 *      // Optionally enable exactly-once sink (on supported runners). See JavaDoc for withEOS().
 *      .withEOS(20, "eos-sink-group-id");
 *   );
 * }</pre>
 *
 * <p>To produce Avro values you can use class {@link
 * io.confluent.kafka.serializers.KafkaAvroSerializer}. To make this class work with {@link
 * KafkaIO#write()} and method withValueSerializer() make sure to erase the generic types by casting
 * to (Class) as shown in the following example:
 *
 * <pre>{@code
 * KafkaIO.<Long, String>write()
 *   ...
 *   .withValueSerializer((Class)KafkaAvroSerializer.class)
 *   .withProducerConfigUpdates( <Map with schema registry configuration details> )
 *   ...
 * }</pre>
 *
 * <p>Often you might want to write just values without any keys to Kafka. Use {@code values()} to
 * write records with default empty(null) key:
 *
 * <pre>{@code
 * PCollection<String> strings = ...;
 * strings.apply(KafkaIO.<Void, String>write()
 *     .withBootstrapServers("broker_1:9092,broker_2:9092")
 *     .withTopic("results")
 *     .withValueSerializer(StringSerializer.class) // just need serializer for value
 *     .values()
 *   );
 * }</pre>
 *
 * <p>Also, if you want to write Kafka {@link ProducerRecord} then you should use {@link
 * KafkaIO#writeRecords()}:
 *
 * <pre>{@code
 * PCollection<ProducerRecord<Long, String>> records = ...;
 * records.apply(KafkaIO.<Long, String>writeRecords()
 *     .withBootstrapServers("broker_1:9092,broker_2:9092")
 *     .withTopic("results")
 *     .withKeySerializer(LongSerializer.class)
 *     .withValueSerializer(StringSerializer.class)
 *   );
 * }</pre>
 *
 * <h3>Advanced Kafka Configuration</h3>
 *
 * KafkaIO allows setting most of the properties in {@link ConsumerConfig} for source or in {@link
 * ProducerConfig} for sink. E.g. if you would like to enable offset <em>auto commit</em> (for
 * external monitoring or other purposes), you can set <tt>"group.id"</tt>,
 * <tt>"enable.auto.commit"</tt>, etc.
 *
 * <h3>Event Timestamps and Watermark</h3>
 *
 * By default, record timestamp (event time) is set to processing time in KafkaIO reader and source
 * watermark is current wall time. If a topic has Kafka server-side ingestion timestamp enabled
 * ('LogAppendTime'), it can enabled with {@link Read#withLogAppendTime()}. A custom timestamp
 * policy can be provided by implementing {@link TimestampPolicyFactory}. See {@link
 * Read#withTimestampPolicyFactory(TimestampPolicyFactory)} for more information.
 *
 * <h3>Supported Kafka Client Versions</h3>
 *
 * KafkaIO relies on <i>kafka-clients</i> for all its interactions with the Kafka cluster.
 * <i>kafka-clients</i> versions 0.10.1 and newer are supported at runtime. The older versions 0.9.x
 * - 0.10.0.0 are also supported, but are deprecated and likely be removed in near future. Please
 * ensure that the version included with the application is compatible with the version of your
 * Kafka cluster. Kafka client usually fails to initialize with a clear error message in case of
 * incompatibility.
 *
 * <h3>Updates to the I/O connector code</h3>
 *
 * For any significant significant updates to this I/O connector, please consider involving
 * corresponding code reviewers mentioned <a
 * href="https://github.com/apache/beam/blob/master/sdks/java/io/kafka/OWNERS">here</a>.
 */
public class KafkaIO {

  /**
   * A specific instance of uninitialized {@link #read()} where key and values are bytes. See
   * #read().
   */
  public static Read<byte[], byte[]> readBytes() {
    return KafkaIO.<byte[], byte[]>read()
        .withKeyDeserializer(ByteArrayDeserializer.class)
        .withValueDeserializer(ByteArrayDeserializer.class);
  }

  /**
   * Creates an uninitialized {@link Read} {@link PTransform}. Before use, basic Kafka configuration
   * should set with {@link Read#withBootstrapServers(String)} and {@link Read#withTopics(List)}.
   * Other optional settings include key and value {@link Deserializer}s, custom timestamp,
   * watermark functions.
   */
  public static <K, V> Read<K, V> read() {
    return new AutoValue_KafkaIO_Read.Builder<K, V>()
        .setTopics(new ArrayList<>())
        .setTopicPartitions(new ArrayList<>())
        .setConsumerFactoryFn(KafkaIOUtils.KAFKA_CONSUMER_FACTORY_FN)
        .setConsumerConfig(KafkaIOUtils.DEFAULT_CONSUMER_PROPERTIES)
        .setMaxNumRecords(Long.MAX_VALUE)
        .setCommitOffsetsInFinalizeEnabled(false)
        .setDynamicRead(false)
        .setTimestampPolicyFactory(TimestampPolicyFactory.withProcessingTime())
        .setConsumerPollingTimeout(2L)
        .setRedistributed(false)
        .setAllowDuplicates(false)
        .setRedistributeNumKeys(0)
        .build();
  }

  /**
   * Creates an uninitialized {@link ReadSourceDescriptors} {@link PTransform}. Different from
   * {@link Read}, setting up {@code topics} and {@code bootstrapServers} is not required during
   * construction time. But the {@code bootstrapServers} still can be configured {@link
   * ReadSourceDescriptors#withBootstrapServers(String)}. Please refer to {@link
   * ReadSourceDescriptors} for more details.
   */
  public static <K, V> ReadSourceDescriptors<K, V> readSourceDescriptors() {
    return ReadSourceDescriptors.<K, V>read();
  }

  /**
   * Creates an uninitialized {@link Write} {@link PTransform}. Before use, Kafka configuration
   * should be set with {@link Write#withBootstrapServers(String)} and {@link Write#withTopic} along
   * with {@link Deserializer}s for (optional) key and values.
   */
  public static <K, V> Write<K, V> write() {
    return new AutoValue_KafkaIO_Write.Builder<K, V>()
        .setWriteRecordsTransform(writeRecords())
        .build();
  }

  /**
   * Creates an uninitialized {@link WriteRecords} {@link PTransform}. Before use, Kafka
   * configuration should be set with {@link WriteRecords#withBootstrapServers(String)} and {@link
   * WriteRecords#withTopic} along with {@link Deserializer}s for (optional) key and values.
   */
  public static <K, V> WriteRecords<K, V> writeRecords() {
    return new AutoValue_KafkaIO_WriteRecords.Builder<K, V>()
        .setProducerConfig(WriteRecords.DEFAULT_PRODUCER_PROPERTIES)
        .setEOS(false)
        .setNumShards(0)
        .setConsumerFactoryFn(KafkaIOUtils.KAFKA_CONSUMER_FACTORY_FN)
        .setBadRecordRouter(BadRecordRouter.THROWING_ROUTER)
        .setBadRecordErrorHandler(new DefaultErrorHandler<>())
        .build();
  }

  ///////////////////////// Read Support \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  /**
   * A {@link PTransform} to read from Kafka topics. See {@link KafkaIO} for more information on
   * usage and configuration.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  public abstract static class Read<K, V>
      extends PTransform<PBegin, PCollection<KafkaRecord<K, V>>> {

    public static final Class<AutoValue_KafkaIO_Read> AUTOVALUE_CLASS =
        AutoValue_KafkaIO_Read.class;

    @Pure
    public abstract Map<String, Object> getConsumerConfig();

    @Pure
    public abstract @Nullable List<String> getTopics();

    @Pure
    public abstract @Nullable List<TopicPartition> getTopicPartitions();

    @Pure
    public abstract @Nullable Pattern getTopicPattern();

    @Pure
    public abstract @Nullable Coder<K> getKeyCoder();

    @Pure
    public abstract @Nullable Coder<V> getValueCoder();

    @Pure
    public abstract SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
        getConsumerFactoryFn();

    @Pure
    public abstract @Nullable SerializableFunction<KafkaRecord<K, V>, Instant> getWatermarkFn();

    @Pure
    public abstract long getMaxNumRecords();

    @Pure
    public abstract @Nullable Duration getMaxReadTime();

    @Pure
    public abstract @Nullable Instant getStartReadTime();

    @Pure
    public abstract @Nullable Instant getStopReadTime();

    @Pure
    public abstract boolean isCommitOffsetsInFinalizeEnabled();

    @Pure
    public abstract boolean isDynamicRead();

    @Pure
    public abstract boolean isRedistributed();

    @Pure
    public abstract boolean isAllowDuplicates();

    @Pure
    public abstract int getRedistributeNumKeys();

    @Pure
    public abstract @Nullable Duration getWatchTopicPartitionDuration();

    @Pure
    public abstract TimestampPolicyFactory<K, V> getTimestampPolicyFactory();

    @Pure
    public abstract @Nullable Map<String, Object> getOffsetConsumerConfig();

    @Pure
    public abstract @Nullable DeserializerProvider<K> getKeyDeserializerProvider();

    @Pure
    public abstract @Nullable DeserializerProvider<V> getValueDeserializerProvider();

    @Pure
    public abstract @Nullable CheckStopReadingFn getCheckStopReadingFn();

    @Pure
    public abstract @Nullable ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    @Pure
    public abstract long getConsumerPollingTimeout();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setConsumerConfig(Map<String, Object> config);

      abstract Builder<K, V> setTopics(List<String> topics);

      abstract Builder<K, V> setTopicPartitions(List<TopicPartition> topicPartitions);

      abstract Builder<K, V> setTopicPattern(Pattern topicPattern);

      abstract Builder<K, V> setKeyCoder(Coder<K> keyCoder);

      abstract Builder<K, V> setValueCoder(Coder<V> valueCoder);

      abstract Builder<K, V> setConsumerFactoryFn(
          SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn);

      abstract Builder<K, V> setWatermarkFn(SerializableFunction<KafkaRecord<K, V>, Instant> fn);

      abstract Builder<K, V> setMaxNumRecords(long maxNumRecords);

      abstract Builder<K, V> setMaxReadTime(Duration maxReadTime);

      abstract Builder<K, V> setStartReadTime(Instant startReadTime);

      abstract Builder<K, V> setStopReadTime(Instant stopReadTime);

      abstract Builder<K, V> setCommitOffsetsInFinalizeEnabled(boolean commitOffsetInFinalize);

      abstract Builder<K, V> setDynamicRead(boolean dynamicRead);

      abstract Builder<K, V> setWatchTopicPartitionDuration(Duration duration);

      abstract Builder<K, V> setRedistributed(boolean withRedistribute);

      abstract Builder<K, V> setAllowDuplicates(boolean allowDuplicates);

      abstract Builder<K, V> setRedistributeNumKeys(int redistributeNumKeys);

      abstract Builder<K, V> setTimestampPolicyFactory(
          TimestampPolicyFactory<K, V> timestampPolicyFactory);

      abstract Builder<K, V> setOffsetConsumerConfig(Map<String, Object> offsetConsumerConfig);

      abstract Builder<K, V> setKeyDeserializerProvider(
          DeserializerProvider<K> deserializerProvider);

      abstract Builder<K, V> setValueDeserializerProvider(
          DeserializerProvider<V> deserializerProvider);

      abstract Builder<K, V> setCheckStopReadingFn(@Nullable CheckStopReadingFn checkStopReadingFn);

      abstract Builder<K, V> setBadRecordErrorHandler(
          @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      Builder<K, V> setCheckStopReadingFn(
          @Nullable SerializableFunction<TopicPartition, Boolean> checkStopReadingFn) {
        return setCheckStopReadingFn(CheckStopReadingFnWrapper.of(checkStopReadingFn));
      }

      abstract Builder<K, V> setConsumerPollingTimeout(long consumerPollingTimeout);

      abstract Read<K, V> build();

      static <K, V> void setupExternalBuilder(
          Builder<K, V> builder, Read.External.Configuration config) {
        ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
        for (String topic : config.topics) {
          listBuilder.add(topic);
        }
        builder.setTopics(listBuilder.build());

        Class<Deserializer<K>> keyDeserializer =
            (Class<Deserializer<K>>) resolveClass(config.keyDeserializer);
        builder.setKeyDeserializerProvider(LocalDeserializerProvider.of(keyDeserializer));
        builder.setKeyCoder(resolveCoder(keyDeserializer));

        Class<Deserializer<V>> valueDeserializer =
            (Class<Deserializer<V>>) resolveClass(config.valueDeserializer);
        builder.setValueDeserializerProvider(LocalDeserializerProvider.of(valueDeserializer));
        builder.setValueCoder(resolveCoder(valueDeserializer));

        Map<String, Object> consumerConfig = new HashMap<>(config.consumerConfig);
        // Key and Value Deserializers always have to be in the config.
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        consumerConfig.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        builder.setConsumerConfig(consumerConfig);

        // Set required defaults
        builder.setTopicPartitions(Collections.emptyList());
        builder.setConsumerFactoryFn(KafkaIOUtils.KAFKA_CONSUMER_FACTORY_FN);
        if (config.maxReadTime != null) {
          builder.setMaxReadTime(Duration.standardSeconds(config.maxReadTime));
        }
        builder.setMaxNumRecords(
            config.maxNumRecords == null ? Long.MAX_VALUE : config.maxNumRecords);

        // Set committing offset configuration.
        builder.setCommitOffsetsInFinalizeEnabled(config.commitOffsetInFinalize);

        // Set timestamp policy with built-in types.
        String timestampPolicy = config.timestampPolicy;
        if (timestampPolicy.equals("ProcessingTime")) {
          builder.setTimestampPolicyFactory(TimestampPolicyFactory.withProcessingTime());
        } else if (timestampPolicy.equals("CreateTime")) {
          builder.setTimestampPolicyFactory(TimestampPolicyFactory.withCreateTime(Duration.ZERO));
        } else if (timestampPolicy.equals("LogAppendTime")) {
          builder.setTimestampPolicyFactory(TimestampPolicyFactory.withLogAppendTime());
        } else {
          throw new IllegalArgumentException(
              "timestampPolicy should be one of (ProcessingTime, CreateTime, LogAppendTime)");
        }

        if (config.startReadTime != null) {
          builder.setStartReadTime(Instant.ofEpochMilli(config.startReadTime));
        }

        if (config.stopReadTime != null) {
          builder.setStopReadTime(Instant.ofEpochMilli(config.stopReadTime));
        }

        // We can expose dynamic read to external build when ReadFromKafkaDoFn is the default
        // implementation.
        builder.setDynamicRead(false);

        if (config.consumerPollingTimeout != null) {
          if (config.consumerPollingTimeout <= 0) {
            throw new IllegalArgumentException("consumerPollingTimeout should be > 0.");
          }
          builder.setConsumerPollingTimeout(config.consumerPollingTimeout);
        } else {
          builder.setConsumerPollingTimeout(2L);
        }

        if (config.redistribute != null) {
          builder.setRedistributed(config.redistribute);
          if (config.redistributeNumKeys != null) {
            builder.setRedistributeNumKeys((int) config.redistributeNumKeys);
          }
          if (config.allowDuplicates != null) {
            builder.setAllowDuplicates(config.allowDuplicates);
          }

        } else {
          builder.setRedistributed(false);
          builder.setRedistributeNumKeys(0);
          builder.setAllowDuplicates(false);
        }
        System.out.println("xxx builder service" + builder.toString());
      }

      private static <T> Coder<T> resolveCoder(Class<Deserializer<T>> deserializer) {
        for (Method method : deserializer.getDeclaredMethods()) {
          if (method.getName().equals("deserialize")) {
            Class<?> returnType = method.getReturnType();
            if (returnType.equals(Object.class)) {
              continue;
            }
            if (returnType.equals(byte[].class)) {
              return (Coder<T>) NullableCoder.of(ByteArrayCoder.of());
            } else if (returnType.equals(Integer.class)) {
              return (Coder<T>) NullableCoder.of(VarIntCoder.of());
            } else if (returnType.equals(Long.class)) {
              return (Coder<T>) NullableCoder.of(VarLongCoder.of());
            } else {
              throw new RuntimeException("Couldn't infer Coder from " + deserializer);
            }
          }
        }
        throw new RuntimeException("Couldn't resolve coder for Deserializer: " + deserializer);
      }
    }

    /**
     * Exposes {@link KafkaIO.TypedWithoutMetadata} as an external transform for cross-language
     * usage.
     */
    @AutoService(ExternalTransformRegistrar.class)
    public static class External implements ExternalTransformRegistrar {

      // Using the transform name in the URN so that the corresponding transform can be easily
      // identified.
      public static final String URN_WITH_METADATA =
          "beam:transform:org.apache.beam:kafka_read_with_metadata:v1";
      public static final String URN_WITHOUT_METADATA =
          "beam:transform:org.apache.beam:kafka_read_without_metadata:v1";

      @Override
      public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
        return ImmutableMap.of(
            URN_WITH_METADATA,
            (Class<? extends ExternalTransformBuilder<?, ?, ?>>)
                (Class<?>) RowsWithMetadata.Builder.class,
            URN_WITHOUT_METADATA,
            (Class<? extends ExternalTransformBuilder<?, ?, ?>>)
                (Class<?>) TypedWithoutMetadata.Builder.class);
      }

      /** Parameters class to expose the Read transform to an external SDK. */
      @SuppressWarnings("initialization")
      public static class Configuration {

        private Map<String, String> consumerConfig;
        private List<String> topics;
        private String keyDeserializer;
        private String valueDeserializer;
        private Long startReadTime;
        private Long stopReadTime;
        private Long maxNumRecords;
        private Long maxReadTime;
        private Boolean commitOffsetInFinalize;
        private Long consumerPollingTimeout;
        private String timestampPolicy;
        private Integer redistributeNumKeys;
        private Boolean redistribute;
        private Boolean allowDuplicates;

        public void setConsumerConfig(Map<String, String> consumerConfig) {
          this.consumerConfig = consumerConfig;
        }

        public void setTopics(List<String> topics) {
          this.topics = topics;
        }

        public void setKeyDeserializer(String keyDeserializer) {
          this.keyDeserializer = keyDeserializer;
        }

        public void setValueDeserializer(String valueDeserializer) {
          this.valueDeserializer = valueDeserializer;
        }

        public void setStartReadTime(Long startReadTime) {
          this.startReadTime = startReadTime;
        }

        public void setStopReadTime(Long stopReadTime) {
          this.stopReadTime = stopReadTime;
        }

        public void setMaxNumRecords(Long maxNumRecords) {
          this.maxNumRecords = maxNumRecords;
        }

        public void setMaxReadTime(Long maxReadTime) {
          this.maxReadTime = maxReadTime;
        }

        public void setCommitOffsetInFinalize(Boolean commitOffsetInFinalize) {
          this.commitOffsetInFinalize = commitOffsetInFinalize;
        }

        public void setTimestampPolicy(String timestampPolicy) {
          this.timestampPolicy = timestampPolicy;
        }

        public void setConsumerPollingTimeout(Long consumerPollingTimeout) {
          this.consumerPollingTimeout = consumerPollingTimeout;
        }

        public void setRedistributeNumKeys(Integer redistributeNumKeys) {
          this.redistributeNumKeys = redistributeNumKeys;
        }

        public void setRedistribute(Boolean redistribute) {
          this.redistribute = redistribute;
        }

        public void setAllowDuplicates(Boolean allowDuplicates) {
          this.allowDuplicates = allowDuplicates;
        }
      }
    }

    /** Sets the bootstrap servers for the Kafka consumer. */
    public Read<K, V> withBootstrapServers(String bootstrapServers) {
      return withConsumerConfigUpdates(
          ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /**
     * Sets the topic to read from.
     *
     * <p>See {@link KafkaUnboundedSource#split(int, PipelineOptions)} for description of how the
     * partitions are distributed among the splits.
     */
    public Read<K, V> withTopic(String topic) {
      return withTopics(ImmutableList.of(topic));
    }

    /**
     * Sets a list of topics to read from. All the partitions from each of the topics are read.
     *
     * <p>See {@link KafkaUnboundedSource#split(int, PipelineOptions)} for description of how the
     * partitions are distributed among the splits.
     */
    public Read<K, V> withTopics(List<String> topics) {
      checkState(
          (getTopicPartitions() == null || getTopicPartitions().isEmpty())
              && getTopicPattern() == null,
          "Only one of topics, topicPartitions or topicPattern can be set");
      return toBuilder().setTopics(ImmutableList.copyOf(topics)).build();
    }

    /**
     * Sets a list of partitions to read from. This allows reading only a subset of partitions for
     * one or more topics when (if ever) needed.
     *
     * <p>See {@link KafkaUnboundedSource#split(int, PipelineOptions)} for description of how the
     * partitions are distributed among the splits.
     */
    public Read<K, V> withTopicPartitions(List<TopicPartition> topicPartitions) {
      checkState(
          (getTopics() == null || getTopics().isEmpty()) && getTopicPattern() == null,
          "Only one of topics, topicPartitions or topicPattern can be set");
      return toBuilder().setTopicPartitions(ImmutableList.copyOf(topicPartitions)).build();
    }

    /**
     * Sets redistribute transform that hints to the runner to try to redistribute the work evenly.
     */
    public Read<K, V> withRedistribute() {
      if (getRedistributeNumKeys() == 0 && isRedistributed()) {
        LOG.warn("This will create a key per record, which is sub-optimal for most use cases.");
      }
      return toBuilder().setRedistributed(true).build();
    }

    public Read<K, V> withAllowDuplicates(Boolean allowDuplicates) {
      if (!isAllowDuplicates()) {
        LOG.warn("Setting this value without setting withRedistribute() will have no effect.");
      }
      return toBuilder().setAllowDuplicates(allowDuplicates).build();
    }

    public Read<K, V> withRedistributeNumKeys(int redistributeNumKeys) {
      checkState(
          isRedistributed(),
          "withRedistributeNumKeys is ignored if withRedistribute() is not enabled on the transform.");
      return toBuilder().setRedistributeNumKeys(redistributeNumKeys).build();
    }

    /**
     * Internally sets a {@link java.util.regex.Pattern} of topics to read from. All the partitions
     * from each of the matching topics are read.
     *
     * <p>See {@link KafkaUnboundedSource#split(int, PipelineOptions)} for description of how the
     * partitions are distributed among the splits.
     */
    public Read<K, V> withTopicPattern(String topicPattern) {
      checkState(
          (getTopics() == null || getTopics().isEmpty())
              && (getTopicPartitions() == null || getTopicPartitions().isEmpty()),
          "Only one of topics, topicPartitions or topicPattern can be set");
      return toBuilder().setTopicPattern(Pattern.compile(topicPattern)).build();
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
      return withKeyDeserializer(LocalDeserializerProvider.of(keyDeserializer));
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
      return withKeyDeserializer(keyDeserializer).toBuilder().setKeyCoder(keyCoder).build();
    }

    public Read<K, V> withKeyDeserializer(DeserializerProvider<K> deserializerProvider) {
      return toBuilder().setKeyDeserializerProvider(deserializerProvider).build();
    }

    public Read<K, V> withKeyDeserializerProviderAndCoder(
        DeserializerProvider<K> deserializerProvider, Coder<K> keyCoder) {
      return toBuilder()
          .setKeyDeserializerProvider(deserializerProvider)
          .setKeyCoder(keyCoder)
          .build();
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
      return withValueDeserializer(LocalDeserializerProvider.of(valueDeserializer));
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
      return withValueDeserializer(valueDeserializer).toBuilder().setValueCoder(valueCoder).build();
    }

    public Read<K, V> withValueDeserializer(DeserializerProvider<V> deserializerProvider) {
      return toBuilder().setValueDeserializerProvider(deserializerProvider).build();
    }

    public Read<K, V> withValueDeserializerProviderAndCoder(
        DeserializerProvider<V> deserializerProvider, Coder<V> valueCoder) {
      return toBuilder()
          .setValueDeserializerProvider(deserializerProvider)
          .setValueCoder(valueCoder)
          .build();
    }

    /**
     * A factory to create Kafka {@link Consumer} from consumer configuration. This is useful for
     * supporting another version of Kafka consumer. Default is {@link KafkaConsumer}.
     */
    public Read<K, V> withConsumerFactoryFn(
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn) {
      return toBuilder().setConsumerFactoryFn(consumerFactoryFn).build();
    }

    /**
     * Update consumer configuration with new properties.
     *
     * @deprecated as of version 2.13. Use {@link #withConsumerConfigUpdates(Map)} instead
     */
    @Deprecated
    public Read<K, V> updateConsumerProperties(Map<String, Object> configUpdates) {
      Map<String, Object> config =
          KafkaIOUtils.updateKafkaProperties(getConsumerConfig(), configUpdates);
      return toBuilder().setConsumerConfig(config).build();
    }

    /**
     * Similar to {@link org.apache.beam.sdk.io.Read.Unbounded#withMaxNumRecords(long)}. Mainly used
     * for tests and demo applications.
     */
    public Read<K, V> withMaxNumRecords(long maxNumRecords) {
      return toBuilder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Use timestamp to set up start offset. It is only supported by Kafka Client 0.10.1.0 onwards
     * and the message format version after 0.10.0.
     *
     * <p>Note that this take priority over start offset configuration {@code
     * ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} and any auto committed offsets.
     *
     * <p>This results in hard failures in either of the following two cases : 1. If one or more
     * partitions do not contain any messages with timestamp larger than or equal to desired
     * timestamp. 2. If the message format version in a partition is before 0.10.0, i.e. the
     * messages do not have timestamps.
     */
    public Read<K, V> withStartReadTime(Instant startReadTime) {
      return toBuilder().setStartReadTime(startReadTime).build();
    }

    /**
     * Use timestamp to set up stop offset. It is only supported by Kafka Client 0.10.1.0 onwards
     * and the message format version after 0.10.0.
     *
     * <p>This results in hard failures in either of the following two cases : 1. If one or more
     * partitions do not contain any messages with timestamp larger than or equal to desired
     * timestamp. 2. If the message format version in a partition is before 0.10.0, i.e. the
     * messages do not have timestamps.
     */
    public Read<K, V> withStopReadTime(Instant stopReadTime) {
      return toBuilder().setStopReadTime(stopReadTime).build();
    }

    /**
     * Similar to {@link org.apache.beam.sdk.io.Read.Unbounded#withMaxReadTime(Duration)}. Mainly
     * used for tests and demo applications.
     */
    public Read<K, V> withMaxReadTime(Duration maxReadTime) {
      return toBuilder().setMaxReadTime(maxReadTime).build();
    }

    /**
     * Sets {@link TimestampPolicy} to {@link TimestampPolicyFactory.LogAppendTimePolicy}. The
     * policy assigns Kafka's log append time (server side ingestion time) to each record. The
     * watermark for each Kafka partition is the timestamp of the last record read. If a partition
     * is idle, the watermark advances to couple of seconds behind wall time. Every record consumed
     * from Kafka is expected to have its timestamp type set to 'LOG_APPEND_TIME'.
     *
     * <p>In Kafka, log append time needs to be enabled for each topic, and all the subsequent
     * records wil have their timestamp set to log append time. If a record does not have its
     * timestamp type set to 'LOG_APPEND_TIME' for any reason, it's timestamp is set to previous
     * record timestamp or latest watermark, whichever is larger.
     *
     * <p>The watermark for the entire source is the oldest of each partition's watermark. If one of
     * the readers falls behind possibly due to uneven distribution of records among Kafka
     * partitions, it ends up holding the watermark for the entire source.
     */
    public Read<K, V> withLogAppendTime() {
      return withTimestampPolicyFactory(TimestampPolicyFactory.withLogAppendTime());
    }

    /**
     * Sets {@link TimestampPolicy} to {@link TimestampPolicyFactory.ProcessingTimePolicy}. This is
     * the default timestamp policy. It assigns processing time to each record. Specifically, this
     * is the timestamp when the record becomes 'current' in the reader. The watermark aways
     * advances to current time. If server side time (log append time) is enabled in Kafka, {@link
     * #withLogAppendTime()} is recommended over this.
     */
    public Read<K, V> withProcessingTime() {
      return withTimestampPolicyFactory(TimestampPolicyFactory.withProcessingTime());
    }

    /**
     * Sets the timestamps policy based on {@link KafkaTimestampType#CREATE_TIME} timestamp of the
     * records. It is an error if a record's timestamp type is not {@link
     * KafkaTimestampType#CREATE_TIME}. The timestamps within a partition are expected to be roughly
     * monotonically increasing with a cap on out of order delays (e.g. 'max delay' of 1 minute).
     * The watermark at any time is '({@code Min(now(), Max(event timestamp so far)) - max delay})'.
     * However, watermark is never set in future and capped to 'now - max delay'. In addition,
     * watermark advanced to 'now - max delay' when a partition is idle.
     *
     * @param maxDelay For any record in the Kafka partition, the timestamp of any subsequent record
     *     is expected to be after {@code current record timestamp - maxDelay}.
     */
    public Read<K, V> withCreateTime(Duration maxDelay) {
      return withTimestampPolicyFactory(TimestampPolicyFactory.withCreateTime(maxDelay));
    }

    /**
     * Provide custom {@link TimestampPolicyFactory} to set event times and watermark for each
     * partition. {@link TimestampPolicyFactory#createTimestampPolicy(TopicPartition, Optional)} is
     * invoked for each partition when the reader starts.
     *
     * @see #withLogAppendTime()
     * @see #withCreateTime(Duration)
     * @see #withProcessingTime()
     */
    public Read<K, V> withTimestampPolicyFactory(
        TimestampPolicyFactory<K, V> timestampPolicyFactory) {
      return toBuilder().setTimestampPolicyFactory(timestampPolicyFactory).build();
    }

    /**
     * A function to assign a timestamp to a record. Default is processing timestamp.
     *
     * @deprecated as of version 2.4. Use {@link
     *     #withTimestampPolicyFactory(TimestampPolicyFactory)} instead.
     */
    @Deprecated
    public Read<K, V> withTimestampFn2(
        SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn) {
      checkArgument(timestampFn != null, "timestampFn can not be null");
      return toBuilder()
          .setTimestampPolicyFactory(TimestampPolicyFactory.withTimestampFn(timestampFn))
          .build();
    }

    /**
     * A function to calculate watermark after a record. Default is last record timestamp.
     *
     * @see #withTimestampFn(SerializableFunction)
     * @deprecated as of version 2.4. Use {@link
     *     #withTimestampPolicyFactory(TimestampPolicyFactory)} instead.
     */
    @Deprecated
    public Read<K, V> withWatermarkFn2(
        SerializableFunction<KafkaRecord<K, V>, Instant> watermarkFn) {
      checkArgument(watermarkFn != null, "watermarkFn can not be null");
      return toBuilder().setWatermarkFn(watermarkFn).build();
    }

    /**
     * A function to assign a timestamp to a record. Default is processing timestamp.
     *
     * @deprecated as of version 2.4. Use {@link
     *     #withTimestampPolicyFactory(TimestampPolicyFactory)} instead.
     */
    @Deprecated
    public Read<K, V> withTimestampFn(SerializableFunction<KV<K, V>, Instant> timestampFn) {
      checkArgument(timestampFn != null, "timestampFn can not be null");
      return withTimestampFn2(unwrapKafkaAndThen(timestampFn));
    }

    /**
     * A function to calculate watermark after a record. Default is last record timestamp.
     *
     * @see #withTimestampFn(SerializableFunction)
     * @deprecated as of version 2.4. Use {@link
     *     #withTimestampPolicyFactory(TimestampPolicyFactory)} instead.
     */
    @Deprecated
    public Read<K, V> withWatermarkFn(SerializableFunction<KV<K, V>, Instant> watermarkFn) {
      checkArgument(watermarkFn != null, "watermarkFn can not be null");
      return withWatermarkFn2(unwrapKafkaAndThen(watermarkFn));
    }

    /**
     * Sets "isolation_level" to "read_committed" in Kafka consumer configuration. This is ensures
     * that the consumer does not read uncommitted messages. Kafka version 0.11 introduced
     * transactional writes. Applications requiring end-to-end exactly-once semantics should only
     * read committed messages. See JavaDoc for {@link KafkaConsumer} for more description.
     */
    public Read<K, V> withReadCommitted() {
      return withConsumerConfigUpdates(ImmutableMap.of("isolation.level", "read_committed"));
    }

    /**
     * Finalized offsets are committed to Kafka. See {@link CheckpointMark#finalizeCheckpoint()}. It
     * helps with minimizing gaps or duplicate processing of records while restarting a pipeline
     * from scratch. But it does not provide hard processing guarantees. There could be a short
     * delay to commit after {@link CheckpointMark#finalizeCheckpoint()} is invoked, as reader might
     * be blocked on reading from Kafka. Note that it is independent of 'AUTO_COMMIT' Kafka consumer
     * configuration. Usually either this or AUTO_COMMIT in Kafka consumer is enabled, but not both.
     */
    public Read<K, V> commitOffsetsInFinalize() {
      return toBuilder().setCommitOffsetsInFinalizeEnabled(true).build();
    }

    /**
     * Configure the KafkaIO to use {@link WatchForKafkaTopicPartitions} to detect and emit any new
     * available {@link TopicPartition} for {@link ReadFromKafkaDoFn} to consume during pipeline
     * execution time. The KafkaIO will regularly check the availability based on the given
     * duration. If the duration is not specified as {@code null}, the default duration is 1 hour.
     */
    public Read<K, V> withDynamicRead(Duration duration) {
      return toBuilder().setDynamicRead(true).setWatchTopicPartitionDuration(duration).build();
    }

    /**
     * Set additional configuration for the backend offset consumer. It may be required for a
     * secured Kafka cluster, especially when you see similar WARN log message 'exception while
     * fetching latest offset for partition {}. will be retried'.
     *
     * <p>In {@link KafkaIO#read()}, there're two consumers running in the backend actually:<br>
     * 1. the main consumer, which reads data from kafka;<br>
     * 2. the secondary offset consumer, which is used to estimate backlog, by fetching latest
     * offset;<br>
     *
     * <p>By default, offset consumer inherits the configuration from main consumer, with an
     * auto-generated {@link ConsumerConfig#GROUP_ID_CONFIG}. This may not work in a secured Kafka
     * which requires more configurations.
     */
    public Read<K, V> withOffsetConsumerConfigOverrides(Map<String, Object> offsetConsumerConfig) {
      return toBuilder().setOffsetConsumerConfig(offsetConsumerConfig).build();
    }

    /**
     * Update configuration for the backend main consumer. Note that the default consumer properties
     * will not be completely overridden. This method only updates the value which has the same key.
     *
     * <p>In {@link KafkaIO#read()}, there're two consumers running in the backend actually:<br>
     * 1. the main consumer, which reads data from kafka;<br>
     * 2. the secondary offset consumer, which is used to estimate backlog, by fetching latest
     * offset;<br>
     *
     * <p>By default, main consumer uses the configuration from {@link
     * KafkaIOUtils#DEFAULT_CONSUMER_PROPERTIES}.
     */
    public Read<K, V> withConsumerConfigUpdates(Map<String, Object> configUpdates) {
      Map<String, Object> config =
          KafkaIOUtils.updateKafkaProperties(getConsumerConfig(), configUpdates);
      return toBuilder().setConsumerConfig(config).build();
    }

    /**
     * A custom {@link CheckStopReadingFn} that determines whether the {@link ReadFromKafkaDoFn}
     * should stop reading from the given {@link TopicPartition}.
     */
    public Read<K, V> withCheckStopReadingFn(CheckStopReadingFn checkStopReadingFn) {
      return toBuilder().setCheckStopReadingFn(checkStopReadingFn).build();
    }

    /**
     * A custom {@link SerializableFunction} that determines whether the {@link ReadFromKafkaDoFn}
     * should stop reading from the given {@link TopicPartition}.
     */
    public Read<K, V> withCheckStopReadingFn(
        SerializableFunction<TopicPartition, Boolean> checkStopReadingFn) {
      return toBuilder()
          .setCheckStopReadingFn(CheckStopReadingFnWrapper.of(checkStopReadingFn))
          .build();
    }

    public Read<K, V> withBadRecordErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      return toBuilder().setBadRecordErrorHandler(badRecordErrorHandler).build();
    }

    /**
     * Sets the timeout time in seconds for Kafka consumer polling request in the {@link
     * ReadFromKafkaDoFn}. A lower timeout optimizes for latency. Increase the timeout if the
     * consumer is not fetching any records. The default is 2 seconds.
     */
    public Read<K, V> withConsumerPollingTimeout(long duration) {
      checkState(duration > 0, "Consumer polling timeout must be greater than 0.");
      return toBuilder().setConsumerPollingTimeout(duration).build();
    }

    /** Returns a {@link PTransform} for PCollection of {@link KV}, dropping Kafka metatdata. */
    public PTransform<PBegin, PCollection<KV<K, V>>> withoutMetadata() {
      return new TypedWithoutMetadata<>(this);
    }

    public PTransform<PBegin, PCollection<Row>> externalWithMetadata() {
      return new RowsWithMetadata<>(this);
    }

    @Override
    public PCollection<KafkaRecord<K, V>> expand(PBegin input) {
      checkArgument(
          getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) != null,
          "withBootstrapServers() is required");
      // With dynamic read, we no longer require providing topic/partition during pipeline
      // construction time. But it requires enabling beam_fn_api.
      if (!isDynamicRead()) {
        checkArgument(
            (getTopics() != null && getTopics().size() > 0)
                || (getTopicPartitions() != null && getTopicPartitions().size() > 0)
                || getTopicPattern() != null,
            "Either withTopic(), withTopics(), withTopicPartitions() or withTopicPattern() is required");
      } else {
        checkArgument(
            ExperimentalOptions.hasExperiment(input.getPipeline().getOptions(), "beam_fn_api"),
            "Kafka Dynamic Read requires enabling experiment beam_fn_api.");
      }
      checkArgument(getKeyDeserializerProvider() != null, "withKeyDeserializer() is required");
      checkArgument(getValueDeserializerProvider() != null, "withValueDeserializer() is required");

      if (!ConsumerSpEL.hasOffsetsForTimes()) {
        LOG.warn(
            "Kafka client version {} is too old. Versions before 0.10.1.0 are deprecated and "
                + "may not be supported in next release of Apache Beam. "
                + "Please upgrade your Kafka client version.",
            AppInfoParser.getVersion());
      }
      if (getStartReadTime() != null) {
        checkArgument(
            ConsumerSpEL.hasOffsetsForTimes(),
            "Consumer.offsetsForTimes is only supported by Kafka Client 0.10.1.0 onwards, "
                + "current version of Kafka Client is "
                + AppInfoParser.getVersion()
                + ". If you are building with maven, set \"kafka.clients.version\" "
                + "maven property to 0.10.1.0 or newer.");
      }
      if (getStopReadTime() != null) {
        checkArgument(
            ConsumerSpEL.hasOffsetsForTimes(),
            "Consumer.offsetsForTimes is only supported by Kafka Client 0.10.1.0 onwards, "
                + "current version of Kafka Client is "
                + AppInfoParser.getVersion()
                + ". If you are building with maven, set \"kafka.clients.version\" "
                + "maven property to 0.10.1.0 or newer.");
      }
      if (isCommitOffsetsInFinalizeEnabled()) {
        checkArgument(
            getConsumerConfig().get(ConsumerConfig.GROUP_ID_CONFIG) != null,
            "commitOffsetsInFinalize() is enabled, but group.id in Kafka consumer config "
                + "is not set. Offset management requires group.id.");
        if (Boolean.TRUE.equals(
            getConsumerConfig().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))) {
          LOG.warn(
              "'{}' in consumer config is enabled even though commitOffsetsInFinalize() "
                  + "is set. You need only one of them.",
              ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        }
      }
      warnAboutUnsafeConfigurations(input);

      // Infer key/value coders if not specified explicitly
      CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();

      Coder<K> keyCoder = getKeyCoder(coderRegistry);
      Coder<V> valueCoder = getValueCoder(coderRegistry);

      final KafkaIOReadImplementationCompatibilityResult compatibility =
          KafkaIOReadImplementationCompatibility.getCompatibility(this);

      // For a number of cases, we prefer using the UnboundedSource Kafka over the new SDF-based
      // Kafka source, for example,
      // * Experiments 'beam_fn_api_use_deprecated_read' and use_deprecated_read will result in
      // legacy UnboundeSource being used.
      // * Experiment 'use_unbounded_sdf_wrapper' will result in legacy UnboundeSource being used
      // but will be wrapped by an SDF.
      // * Some runners or selected features may not be compatible with SDF-based Kafka.
      if (ExperimentalOptions.hasExperiment(
              input.getPipeline().getOptions(), "beam_fn_api_use_deprecated_read")
          || ExperimentalOptions.hasExperiment(
              input.getPipeline().getOptions(), "use_deprecated_read")
          || ExperimentalOptions.hasExperiment(
              input.getPipeline().getOptions(), "use_unbounded_sdf_wrapper")
          || compatibility.supportsOnly(KafkaIOReadImplementation.LEGACY)
          || (compatibility.supports(KafkaIOReadImplementation.LEGACY)
              && runnerPrefersLegacyRead(input.getPipeline().getOptions()))) {
        return input.apply(new ReadFromKafkaViaUnbounded<>(this, keyCoder, valueCoder));
      }
      return input.apply(new ReadFromKafkaViaSDF<>(this, keyCoder, valueCoder));
    }

    private void warnAboutUnsafeConfigurations(PBegin input) {
      Long checkpointingInterval =
          input
              .getPipeline()
              .getOptions()
              .as(FakeFlinkPipelineOptions.class)
              .getCheckpointingInterval();
      String autoOffsetReset = (String) getConsumerConfig().get(AUTO_OFFSET_RESET_CONFIG);
      if (checkpointingInterval != null
          && checkpointingInterval != -1
          && Boolean.TRUE.equals(getConsumerConfig().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
          && !isCommitOffsetsInFinalizeEnabled()
          && (autoOffsetReset == null || "latest".equals(autoOffsetReset))) {
        LOG.warn(
            "When using the Flink runner with checkpointingInterval enabled,"
                + " Kafka enable.auto.commit enabled,"
                + " and Kafka auto.offset.reset set to latest or unset,"
                + " there is a chance for every checkpoint to time out,"
                + " which will cause data loss."
                + " We recommend setting commitOffsetInFinalize to true in ReadFromKafka,"
                + " enable.auto.commit to false, and auto.offset.reset to none");
      }
    }

    // This class is designed to mimic the Flink pipeline options, so we can check for the
    // checkpointingInterval property, but without needing to depend on the Flink runner
    // Do not use this
    public interface FakeFlinkPipelineOptions extends PipelineOptions {
      @Default.Long(-1)
      Long getCheckpointingInterval();

      void setCheckpointingInterval(Long interval);
    }

    private boolean runnerPrefersLegacyRead(PipelineOptions options) {
      // Only Dataflow runner requires sdf read at this moment. For other non-portable runners, if
      // it doesn't specify use_sdf_read, it will use legacy read regarding to performance concern.
      // TODO(https://github.com/apache/beam/issues/20530): Remove this special check when we
      // address performance issue.
      if (ExperimentalOptions.hasExperiment(options, "use_sdf_read")) {
        return false;
      }
      if (options.getRunner().getName().startsWith("org.apache.beam.runners.dataflow.")) {
        return false;
      } else if (ExperimentalOptions.hasExperiment(options, "beam_fn_api")) {
        return false;
      }
      return true;
    }

    /**
     * A {@link PTransformOverride} for runners to swap {@link ReadFromKafkaViaSDF} to legacy Kafka
     * read if runners doesn't have a good support on executing unbounded Splittable DoFn.
     */
    @Internal
    public static final PTransformOverride KAFKA_READ_OVERRIDE =
        PTransformOverride.of(
            PTransformMatchers.classEqualTo(ReadFromKafkaViaSDF.class),
            new KafkaReadOverrideFactory<>());

    private static class KafkaReadOverrideFactory<K, V>
        implements PTransformOverrideFactory<
            PBegin, PCollection<KafkaRecord<K, V>>, ReadFromKafkaViaSDF<K, V>> {

      @Override
      public PTransformReplacement<PBegin, PCollection<KafkaRecord<K, V>>> getReplacementTransform(
          AppliedPTransform<PBegin, PCollection<KafkaRecord<K, V>>, ReadFromKafkaViaSDF<K, V>>
              transform) {
        try {
          return PTransformReplacement.of(
              transform.getPipeline().begin(),
              new ReadFromKafkaViaUnbounded<>(
                  transform.getTransform().kafkaRead,
                  transform.getTransform().keyCoder,
                  transform.getTransform().valueCoder));
        } catch (KafkaIOReadImplementationCompatibilityException e) {
          throw new IllegalStateException(
              "The current runner does not support SDF-based Kafka read properly "
                  + "and the replacement runner lacks the support for the following properties: "
                  + e.getConflictingProperties()
                  + ". For example if you are using Dataflow then consider using Dataflow Runner v2.");
        }
      }

      @Override
      public Map<PCollection<?>, ReplacementOutput> mapOutputs(
          Map<TupleTag<?>, PCollection<?>> outputs, PCollection<KafkaRecord<K, V>> newOutput) {
        return ReplacementOutputs.singleton(outputs, newOutput);
      }
    }

    private abstract static class AbstractReadFromKafka<K, V>
        extends PTransform<PBegin, PCollection<KafkaRecord<K, V>>> {
      Read<K, V> kafkaRead;
      Coder<K> keyCoder;
      Coder<V> valueCoder;

      AbstractReadFromKafka(
          Read<K, V> kafkaRead,
          Coder<K> keyCoder,
          Coder<V> valueCoder,
          KafkaIOReadImplementation implementation) {
        KafkaIOReadImplementationCompatibility.getCompatibility(kafkaRead)
            .checkSupport(implementation);
        this.kafkaRead = kafkaRead;
        this.keyCoder = keyCoder;
        this.valueCoder = valueCoder;
      }
    }

    static class ReadFromKafkaViaUnbounded<K, V> extends AbstractReadFromKafka<K, V> {
      ReadFromKafkaViaUnbounded(Read<K, V> kafkaRead, Coder<K> keyCoder, Coder<V> valueCoder) {
        super(kafkaRead, keyCoder, valueCoder, KafkaIOReadImplementation.LEGACY);
      }

      @Override
      public PCollection<KafkaRecord<K, V>> expand(PBegin input) {
        if (kafkaRead.getBadRecordErrorHandler() != null) {
          LOG.warn(
              "The Legacy implementation of Kafka Read does not support writing malformed"
                  + " messages to an error handler. Use the SDF implementation instead.");
        }
        // Handles unbounded source to bounded conversion if maxNumRecords or maxReadTime is set.
        Unbounded<KafkaRecord<K, V>> unbounded =
            org.apache.beam.sdk.io.Read.from(
                kafkaRead
                    .toBuilder()
                    .setKeyCoder(keyCoder)
                    .setValueCoder(valueCoder)
                    .build()
                    .makeSource());

        PTransform<PBegin, PCollection<KafkaRecord<K, V>>> transform = unbounded;

        if (kafkaRead.getMaxNumRecords() < Long.MAX_VALUE || kafkaRead.getMaxReadTime() != null) {
          // bounded read
          transform =
              unbounded
                  .withMaxReadTime(kafkaRead.getMaxReadTime())
                  .withMaxNumRecords(kafkaRead.getMaxNumRecords());
        }

        if (kafkaRead.isRedistributed()) {
          // fail here instead.
          checkArgument(
              kafkaRead.isCommitOffsetsInFinalizeEnabled(),
              "commitOffsetsInFinalize() can't be enabled with isRedistributed");
          PCollection<KafkaRecord<K, V>> output = input.getPipeline().apply(transform);
          if (kafkaRead.getRedistributeNumKeys() == 0) {
            return output.apply(
                "Insert Redistribute",
                Redistribute.<KafkaRecord<K, V>>arbitrarily()
                    .withAllowDuplicates(kafkaRead.isAllowDuplicates()));
          } else {
            return output.apply(
                "Insert Redistribute with Shards",
                Redistribute.<KafkaRecord<K, V>>arbitrarily()
                    .withAllowDuplicates(kafkaRead.isAllowDuplicates())
                    .withNumBuckets((int) kafkaRead.getRedistributeNumKeys()));
          }
        }
        return input.getPipeline().apply(transform);
      }
    }

    static class ReadFromKafkaViaSDF<K, V> extends AbstractReadFromKafka<K, V> {
      ReadFromKafkaViaSDF(Read<K, V> kafkaRead, Coder<K> keyCoder, Coder<V> valueCoder) {
        super(kafkaRead, keyCoder, valueCoder, KafkaIOReadImplementation.SDF);
      }

      @Override
      public PCollection<KafkaRecord<K, V>> expand(PBegin input) {
        ReadSourceDescriptors<K, V> readTransform =
            ReadSourceDescriptors.<K, V>read()
                .withConsumerConfigOverrides(kafkaRead.getConsumerConfig())
                .withOffsetConsumerConfigOverrides(kafkaRead.getOffsetConsumerConfig())
                .withConsumerFactoryFn(kafkaRead.getConsumerFactoryFn())
                .withKeyDeserializerProvider(kafkaRead.getKeyDeserializerProvider())
                .withValueDeserializerProvider(kafkaRead.getValueDeserializerProvider())
                .withManualWatermarkEstimator()
                .withTimestampPolicyFactory(kafkaRead.getTimestampPolicyFactory())
                .withCheckStopReadingFn(kafkaRead.getCheckStopReadingFn())
                .withConsumerPollingTimeout(kafkaRead.getConsumerPollingTimeout());
        if (kafkaRead.isCommitOffsetsInFinalizeEnabled()) {
          readTransform = readTransform.commitOffsets();
        }
        if (kafkaRead.getStopReadTime() != null) {
          readTransform = readTransform.withBounded();
        }
        if (kafkaRead.getBadRecordErrorHandler() != null) {
          readTransform =
              readTransform.withBadRecordErrorHandler(kafkaRead.getBadRecordErrorHandler());
        }
        if (kafkaRead.isRedistributed()) {
          readTransform = readTransform.withRedistribute();
        }
        if (kafkaRead.isAllowDuplicates()) {
          readTransform = readTransform.withAllowDuplicates();
        }
        if (kafkaRead.getRedistributeNumKeys() > 0) {
          readTransform = readTransform.withRedistributeNumKeys(kafkaRead.getRedistributeNumKeys());
        }
        PCollection<KafkaSourceDescriptor> output;
        if (kafkaRead.isDynamicRead()) {
          Set<String> topics = new HashSet<>();
          if (kafkaRead.getTopics() != null && kafkaRead.getTopics().size() > 0) {
            topics.addAll(kafkaRead.getTopics());
          }
          if (kafkaRead.getTopicPartitions() != null && kafkaRead.getTopicPartitions().size() > 0) {
            for (TopicPartition topicPartition : kafkaRead.getTopicPartitions()) {
              topics.add(topicPartition.topic());
            }
          }
          output =
              input.apply(
                  new WatchForKafkaTopicPartitions(
                      kafkaRead.getWatchTopicPartitionDuration(),
                      kafkaRead.getConsumerFactoryFn(),
                      kafkaRead.getConsumerConfig(),
                      kafkaRead.getCheckStopReadingFn(),
                      topics,
                      kafkaRead.getTopicPattern(),
                      kafkaRead.getStartReadTime(),
                      kafkaRead.getStopReadTime()));
        } else {
          output =
              input
                  .getPipeline()
                  .apply(Impulse.create())
                  .apply(ParDo.of(new GenerateKafkaSourceDescriptor(kafkaRead)));
        }
        if (kafkaRead.isRedistributed()) {
          PCollection<KafkaRecord<K, V>> pcol =
              output.apply(readTransform).setCoder(KafkaRecordCoder.of(keyCoder, valueCoder));
          if (kafkaRead.getRedistributeNumKeys() == 0) {
            return pcol.apply(
                "Insert Redistribute",
                Redistribute.<KafkaRecord<K, V>>arbitrarily()
                    .withAllowDuplicates(kafkaRead.isAllowDuplicates()));
          } else {
            return pcol.apply(
                "Insert Redistribute with Shards",
                Redistribute.<KafkaRecord<K, V>>arbitrarily()
                    .withAllowDuplicates(true)
                    .withNumBuckets((int) kafkaRead.getRedistributeNumKeys()));
          }
        }
        return output.apply(readTransform).setCoder(KafkaRecordCoder.of(keyCoder, valueCoder));
      }
    }

    /**
     * A DoFn which generates {@link KafkaSourceDescriptor} based on the configuration of {@link
     * Read}.
     */
    @VisibleForTesting
    static class GenerateKafkaSourceDescriptor extends DoFn<byte[], KafkaSourceDescriptor> {
      GenerateKafkaSourceDescriptor(Read<?, ?> read) {
        this.consumerConfig = read.getConsumerConfig();
        this.consumerFactoryFn = read.getConsumerFactoryFn();
        this.topics = read.getTopics();
        this.topicPartitions = read.getTopicPartitions();
        this.topicPattern = read.getTopicPattern();
        this.startReadTime = read.getStartReadTime();
        this.stopReadTime = read.getStopReadTime();
      }

      private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
          consumerFactoryFn;

      private final @Nullable List<TopicPartition> topicPartitions;

      private final @Nullable Instant startReadTime;

      private final @Nullable Instant stopReadTime;

      @VisibleForTesting final Map<String, Object> consumerConfig;

      @VisibleForTesting final @Nullable List<String> topics;

      private final @Nullable Pattern topicPattern;

      @ProcessElement
      public void processElement(OutputReceiver<KafkaSourceDescriptor> receiver) {
        List<TopicPartition> partitions =
            new ArrayList<>(Preconditions.checkStateNotNull(topicPartitions));
        if (partitions.isEmpty()) {
          try (Consumer<?, ?> consumer = consumerFactoryFn.apply(consumerConfig)) {
            List<String> topics = Preconditions.checkStateNotNull(this.topics);
            if (topics.isEmpty()) {
              Pattern pattern = Preconditions.checkStateNotNull(topicPattern);
              for (Map.Entry<String, List<PartitionInfo>> entry :
                  consumer.listTopics().entrySet()) {
                if (pattern.matcher(entry.getKey()).matches()) {
                  for (PartitionInfo p : entry.getValue()) {
                    partitions.add(new TopicPartition(p.topic(), p.partition()));
                  }
                }
              }
            } else {
              for (String topic : topics) {
                for (PartitionInfo p : consumer.partitionsFor(topic)) {
                  partitions.add(new TopicPartition(p.topic(), p.partition()));
                }
              }
            }
          }
        }
        for (TopicPartition topicPartition : partitions) {
          receiver.output(
              KafkaSourceDescriptor.of(
                  topicPartition, null, startReadTime, null, stopReadTime, null));
        }
      }
    }

    private Coder<K> getKeyCoder(CoderRegistry coderRegistry) {
      return (getKeyCoder() != null)
          ? getKeyCoder()
          : Preconditions.checkStateNotNull(getKeyDeserializerProvider()).getCoder(coderRegistry);
    }

    private Coder<V> getValueCoder(CoderRegistry coderRegistry) {
      return (getValueCoder() != null)
          ? getValueCoder()
          : Preconditions.checkStateNotNull(getValueDeserializerProvider()).getCoder(coderRegistry);
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
    private static <KeyT, ValueT, OutT>
        SerializableFunction<KafkaRecord<KeyT, ValueT>, OutT> unwrapKafkaAndThen(
            final SerializableFunction<KV<KeyT, ValueT>, OutT> fn) {
      return record -> fn.apply(record.getKV());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      List<String> topics = Preconditions.checkStateNotNull(getTopics());
      List<TopicPartition> topicPartitions = Preconditions.checkStateNotNull(getTopicPartitions());
      Pattern topicPattern = getTopicPattern();
      if (topics.size() > 0) {
        builder.add(DisplayData.item("topics", Joiner.on(",").join(topics)).withLabel("Topic/s"));
      } else if (topicPartitions.size() > 0) {
        builder.add(
            DisplayData.item("topicPartitions", Joiner.on(",").join(topicPartitions))
                .withLabel("Topic Partition/s"));
      } else if (topicPattern != null) {
        builder.add(
            DisplayData.item("topicPattern", topicPattern.pattern()).withLabel("Topic Pattern"));
      }
      Set<String> disallowedConsumerPropertiesKeys =
          KafkaIOUtils.DISALLOWED_CONSUMER_PROPERTIES.keySet();
      for (Map.Entry<String, Object> conf : getConsumerConfig().entrySet()) {
        String key = conf.getKey();
        if (!disallowedConsumerPropertiesKeys.contains(key)) {
          Object value =
              DisplayData.inferType(conf.getValue()) != null
                  ? conf.getValue()
                  : String.valueOf(conf.getValue());
          builder.add(DisplayData.item(key, ValueProvider.StaticValueProvider.of(value)));
        }
      }
    }
  }

  /**
   * A {@link PTransform} to read from Kafka topics. Similar to {@link KafkaIO.Read}, but removes
   * Kafka metatdata and returns a {@link PCollection} of {@link KV}. See {@link KafkaIO} for more
   * information on usage and configuration of reader.
   */
  public static class TypedWithoutMetadata<K, V> extends PTransform<PBegin, PCollection<KV<K, V>>> {
    private final Read<K, V> read;

    TypedWithoutMetadata(Read<K, V> read) {
      super("KafkaIO.Read");
      this.read = read;
    }

    static class Builder<K, V>
        implements ExternalTransformBuilder<
            Read.External.Configuration, PBegin, PCollection<KV<K, V>>> {

      @Override
      public PTransform<PBegin, PCollection<KV<K, V>>> buildExternal(
          Read.External.Configuration config) {
        Read.Builder<K, V> readBuilder = new AutoValue_KafkaIO_Read.Builder<>();
        Read.Builder.setupExternalBuilder(readBuilder, config);

        return readBuilder.build().withoutMetadata();
      }
    }

    @Override
    public PCollection<KV<K, V>> expand(PBegin begin) {
      return begin
          .apply(read)
          .apply(
              "Remove Kafka Metadata",
              ParDo.of(
                  new DoFn<KafkaRecord<K, V>, KV<K, V>>() {
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

  @DefaultSchema(JavaFieldSchema.class)
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  /**
   * Represents a Kafka header. We define a new class so that we can add schema annotations for
   * generating Rows.
   */
  static class KafkaHeader {

    String key;
    byte @Nullable [] value;

    @SchemaCreate
    public KafkaHeader(String key, byte @Nullable [] value) {
      this.key = key;
      this.value = value;
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  /**
   * Represents a Kafka record with metadata whey key and values are byte arrays. This class should
   * only be used to represent a Kafka record for external transforms.
   * TODO(https://github.com/apache/beam/issues/18919): use regular KafkaRecord class when Beam
   * Schema inference supports generics.
   */
  static class ByteArrayKafkaRecord {

    String topic;
    int partition;
    long offset;
    long timestamp;
    byte @Nullable [] key;
    byte @Nullable [] value;
    @Nullable List<KafkaHeader> headers;
    int timestampTypeId;
    String timestampTypeName;

    @SchemaCreate
    public ByteArrayKafkaRecord(
        String topic,
        int partition,
        long offset,
        long timestamp,
        byte @Nullable [] key,
        byte @Nullable [] value,
        @Nullable List<KafkaHeader> headers,
        int timestampTypeId,
        String timestampTypeName) {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
      this.timestamp = timestamp;
      this.key = key;
      this.value = value;
      this.headers = headers;
      this.timestampTypeId = timestampTypeId;
      this.timestampTypeName = timestampTypeName;
    }
  }

  /**
   * A {@link PTransform} to read from Kafka topics. Similar to {@link KafkaIO.Read}, but generates
   * a {@link PCollection} of {@link Row}. This class is primarily used as a cross-language
   * transform since {@link KafkaRecord} is not a type that can be easily encoded using Beam's
   * standard coders. See {@link KafkaIO} for more information on usage and configuration of reader.
   */
  static class RowsWithMetadata<K, V> extends PTransform<PBegin, PCollection<Row>> {
    private final Read<K, V> read;

    RowsWithMetadata(Read<K, V> read) {
      super("KafkaIO.RowsWithMetadata");
      this.read = read;
    }

    static class Builder<K, V>
        implements ExternalTransformBuilder<Read.External.Configuration, PBegin, PCollection<Row>> {

      @Override
      public PTransform<PBegin, PCollection<Row>> buildExternal(
          Read.External.Configuration config) {
        Read.Builder<K, V> readBuilder = new AutoValue_KafkaIO_Read.Builder<>();
        Read.Builder.setupExternalBuilder(readBuilder, config);

        Class<Deserializer<K>> keyDeserializer =
            (Class<Deserializer<K>>) resolveClass(config.keyDeserializer);
        Coder<K> keyCoder = Read.Builder.resolveCoder(keyDeserializer);
        if (!(keyCoder instanceof NullableCoder
            && keyCoder.getCoderArguments().get(0) instanceof ByteArrayCoder)) {
          throw new RuntimeException(
              "ExternalWithMetadata transform only supports keys of type nullable(byte[])");
        }
        Class<Deserializer<V>> valueDeserializer =
            (Class<Deserializer<V>>) resolveClass(config.valueDeserializer);
        Coder<V> valueCoder = Read.Builder.resolveCoder(valueDeserializer);
        if (!(valueCoder instanceof NullableCoder
            && valueCoder.getCoderArguments().get(0) instanceof ByteArrayCoder)) {
          throw new RuntimeException(
              "ExternalWithMetadata transform only supports values of type nullable(byte[])");
        }

        return readBuilder.build().externalWithMetadata();
      }
    }

    public static <K, V> ByteArrayKafkaRecord toExternalKafkaRecord(KafkaRecord<K, V> kafkaRecord) {
      List<KafkaHeader> headers =
          (kafkaRecord.getHeaders() == null)
              ? null
              : Arrays.stream(kafkaRecord.getHeaders().toArray())
                  .map(h -> new KafkaHeader(h.key(), h.value()))
                  .collect(Collectors.toList());
      ByteArrayKafkaRecord byteArrayKafkaRecord =
          new ByteArrayKafkaRecord(
              kafkaRecord.getTopic(),
              kafkaRecord.getPartition(),
              kafkaRecord.getOffset(),
              kafkaRecord.getTimestamp(),
              (byte[]) kafkaRecord.getKV().getKey(),
              (byte[]) kafkaRecord.getKV().getValue(),
              headers,
              kafkaRecord.getTimestampType().id,
              kafkaRecord.getTimestampType().name);

      return byteArrayKafkaRecord;
    }

    @Override
    public PCollection<Row> expand(PBegin begin) {
      return begin
          .apply(read)
          .apply(
              "Convert to ExternalKafkaRecord",
              ParDo.of(
                  new DoFn<KafkaRecord<K, V>, ByteArrayKafkaRecord>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx) {
                      KafkaRecord<K, V> kafkRecord = ctx.element();
                      ctx.output(toExternalKafkaRecord(kafkRecord));
                    }
                  }))
          .apply(Convert.toRows());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      read.populateDisplayData(builder);
    }
  }

  /**
   * A {@link PTransform} to read from {@link KafkaSourceDescriptor}. See {@link KafkaIO} for more
   * information on usage and configuration. See {@link ReadFromKafkaDoFn} for more implementation
   * details.
   *
   * <p>During expansion, if {@link ReadSourceDescriptors#isCommitOffsetEnabled()} is {@code true},
   * the transform will expand to:
   *
   * <pre>{@code
   * PCollection<KafkaSourceDescriptor> --> ParDo(ReadFromKafkaDoFn<KafkaSourceDescriptor, KV<KafkaSourceDescriptor, KafkaRecord>>) --> Map(output KafkaRecord)
   *                                                                                                          |
   *                                                                                                          --> KafkaCommitOffset
   * }</pre>
   *
   * . Note that this expansion is not supported when running with x-lang on Dataflow.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  public abstract static class ReadSourceDescriptors<K, V>
      extends PTransform<PCollection<KafkaSourceDescriptor>, PCollection<KafkaRecord<K, V>>> {

    private final TupleTag<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> records = new TupleTag<>();

    private static final Logger LOG = LoggerFactory.getLogger(ReadSourceDescriptors.class);

    @Pure
    abstract Map<String, Object> getConsumerConfig();

    @Pure
    abstract @Nullable Map<String, Object> getOffsetConsumerConfig();

    @Pure
    abstract @Nullable DeserializerProvider<K> getKeyDeserializerProvider();

    @Pure
    abstract @Nullable DeserializerProvider<V> getValueDeserializerProvider();

    @Pure
    abstract @Nullable Coder<K> getKeyCoder();

    @Pure
    abstract @Nullable Coder<V> getValueCoder();

    @Pure
    abstract SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
        getConsumerFactoryFn();

    @Pure
    abstract @Nullable CheckStopReadingFn getCheckStopReadingFn();

    @Pure
    abstract @Nullable SerializableFunction<KafkaRecord<K, V>, Instant>
        getExtractOutputTimestampFn();

    @Pure
    abstract @Nullable SerializableFunction<Instant, WatermarkEstimator<Instant>>
        getCreateWatermarkEstimatorFn();

    @Pure
    abstract boolean isCommitOffsetEnabled();

    @Pure
    abstract boolean isRedistribute();

    @Pure
    abstract boolean isAllowDuplicates();

    @Pure
    abstract int getRedistributeNumKeys();

    @Pure
    abstract @Nullable TimestampPolicyFactory<K, V> getTimestampPolicyFactory();

    @Pure
    abstract BadRecordRouter getBadRecordRouter();

    @Pure
    abstract ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    @Pure
    abstract long getConsumerPollingTimeout();

    abstract boolean isBounded();

    abstract ReadSourceDescriptors.Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract ReadSourceDescriptors.Builder<K, V> setConsumerConfig(Map<String, Object> config);

      abstract ReadSourceDescriptors.Builder<K, V> setOffsetConsumerConfig(
          @Nullable Map<String, Object> offsetConsumerConfig);

      abstract ReadSourceDescriptors.Builder<K, V> setConsumerFactoryFn(
          SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn);

      abstract ReadSourceDescriptors.Builder<K, V> setCheckStopReadingFn(
          @Nullable CheckStopReadingFn checkStopReadingFn);

      ReadSourceDescriptors.Builder<K, V> setCheckStopReadingFn(
          @Nullable SerializableFunction<TopicPartition, Boolean> checkStopReadingFn) {
        return setCheckStopReadingFn(CheckStopReadingFnWrapper.of(checkStopReadingFn));
      }

      abstract ReadSourceDescriptors.Builder<K, V> setKeyDeserializerProvider(
          @Nullable DeserializerProvider<K> deserializerProvider);

      abstract ReadSourceDescriptors.Builder<K, V> setValueDeserializerProvider(
          @Nullable DeserializerProvider<V> deserializerProvider);

      abstract ReadSourceDescriptors.Builder<K, V> setKeyCoder(Coder<K> keyCoder);

      abstract ReadSourceDescriptors.Builder<K, V> setValueCoder(Coder<V> valueCoder);

      abstract ReadSourceDescriptors.Builder<K, V> setExtractOutputTimestampFn(
          SerializableFunction<KafkaRecord<K, V>, Instant> fn);

      abstract ReadSourceDescriptors.Builder<K, V> setCreateWatermarkEstimatorFn(
          SerializableFunction<Instant, WatermarkEstimator<Instant>> fn);

      abstract ReadSourceDescriptors.Builder<K, V> setCommitOffsetEnabled(
          boolean commitOffsetEnabled);

      abstract ReadSourceDescriptors.Builder<K, V> setTimestampPolicyFactory(
          TimestampPolicyFactory<K, V> policy);

      abstract ReadSourceDescriptors.Builder<K, V> setBadRecordRouter(
          BadRecordRouter badRecordRouter);

      abstract ReadSourceDescriptors.Builder<K, V> setBadRecordErrorHandler(
          ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      abstract ReadSourceDescriptors.Builder<K, V> setConsumerPollingTimeout(long duration);

      abstract ReadSourceDescriptors.Builder<K, V> setBounded(boolean bounded);

      abstract ReadSourceDescriptors.Builder<K, V> setRedistribute(boolean withRedistribute);

      abstract ReadSourceDescriptors.Builder<K, V> setAllowDuplicates(boolean allowDuplicates);

      abstract ReadSourceDescriptors.Builder<K, V> setRedistributeNumKeys(int redistributeNumKeys);

      abstract ReadSourceDescriptors<K, V> build();
    }

    public static <K, V> ReadSourceDescriptors<K, V> read() {
      return new AutoValue_KafkaIO_ReadSourceDescriptors.Builder<K, V>()
          .setConsumerFactoryFn(KafkaIOUtils.KAFKA_CONSUMER_FACTORY_FN)
          .setConsumerConfig(KafkaIOUtils.DEFAULT_CONSUMER_PROPERTIES)
          .setCommitOffsetEnabled(false)
          .setBounded(false)
          .setBadRecordRouter(BadRecordRouter.THROWING_ROUTER)
          .setBadRecordErrorHandler(new ErrorHandler.DefaultErrorHandler<>())
          .setConsumerPollingTimeout(2L)
          .setRedistribute(false)
          .setAllowDuplicates(false)
          .setRedistributeNumKeys(0)
          .build()
          .withProcessingTime()
          .withMonotonicallyIncreasingWatermarkEstimator();
    }

    /**
     * Sets the bootstrap servers to use for the Kafka consumer if unspecified via
     * KafkaSourceDescriptor#getBootStrapServers()}.
     */
    public ReadSourceDescriptors<K, V> withBootstrapServers(String bootstrapServers) {
      return withConsumerConfigUpdates(
          ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    public ReadSourceDescriptors<K, V> withKeyDeserializerProvider(
        @Nullable DeserializerProvider<K> deserializerProvider) {
      return toBuilder().setKeyDeserializerProvider(deserializerProvider).build();
    }

    public ReadSourceDescriptors<K, V> withValueDeserializerProvider(
        @Nullable DeserializerProvider<V> deserializerProvider) {
      return toBuilder().setValueDeserializerProvider(deserializerProvider).build();
    }

    /**
     * Sets a Kafka {@link Deserializer} to interpret key bytes read from Kafka.
     *
     * <p>In addition, Beam also needs a {@link Coder} to serialize and deserialize key objects at
     * runtime. KafkaIO tries to infer a coder for the key based on the {@link Deserializer} class,
     * however in case that fails, you can use {@link #withKeyDeserializerAndCoder(Class, Coder)} to
     * provide the key coder explicitly.
     */
    public ReadSourceDescriptors<K, V> withKeyDeserializer(
        Class<? extends Deserializer<K>> keyDeserializer) {
      return withKeyDeserializerProvider(LocalDeserializerProvider.of(keyDeserializer));
    }

    /**
     * Sets a Kafka {@link Deserializer} to interpret value bytes read from Kafka.
     *
     * <p>In addition, Beam also needs a {@link Coder} to serialize and deserialize value objects at
     * runtime. KafkaIO tries to infer a coder for the value based on the {@link Deserializer}
     * class, however in case that fails, you can use {@link #withValueDeserializerAndCoder(Class,
     * Coder)} to provide the value coder explicitly.
     */
    public ReadSourceDescriptors<K, V> withValueDeserializer(
        Class<? extends Deserializer<V>> valueDeserializer) {
      return withValueDeserializerProvider(LocalDeserializerProvider.of(valueDeserializer));
    }

    /**
     * Sets a Kafka {@link Deserializer} for interpreting key bytes read from Kafka along with a
     * {@link Coder} for helping the Beam runner materialize key objects at runtime if necessary.
     *
     * <p>Use this method to override the coder inference performed within {@link
     * #withKeyDeserializer(Class)}.
     */
    public ReadSourceDescriptors<K, V> withKeyDeserializerAndCoder(
        Class<? extends Deserializer<K>> keyDeserializer, Coder<K> keyCoder) {
      return withKeyDeserializer(keyDeserializer).toBuilder().setKeyCoder(keyCoder).build();
    }

    /**
     * Sets a Kafka {@link Deserializer} for interpreting value bytes read from Kafka along with a
     * {@link Coder} for helping the Beam runner materialize value objects at runtime if necessary.
     *
     * <p>Use this method to override the coder inference performed within {@link
     * #withValueDeserializer(Class)}.
     */
    public ReadSourceDescriptors<K, V> withValueDeserializerAndCoder(
        Class<? extends Deserializer<V>> valueDeserializer, Coder<V> valueCoder) {
      return withValueDeserializer(valueDeserializer).toBuilder().setValueCoder(valueCoder).build();
    }

    /**
     * A factory to create Kafka {@link Consumer} from consumer configuration. This is useful for
     * supporting another version of Kafka consumer. Default is {@link KafkaConsumer}.
     */
    public ReadSourceDescriptors<K, V> withConsumerFactoryFn(
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn) {
      return toBuilder().setConsumerFactoryFn(consumerFactoryFn).build();
    }

    /**
     * A custom {@link CheckStopReadingFn} that determines whether the {@link ReadFromKafkaDoFn}
     * should stop reading from the given {@link TopicPartition}.
     */
    public ReadSourceDescriptors<K, V> withCheckStopReadingFn(
        @Nullable CheckStopReadingFn checkStopReadingFn) {
      return toBuilder().setCheckStopReadingFn(checkStopReadingFn).build();
    }

    /**
     * A custom {@link SerializableFunction} that determines whether the {@link ReadFromKafkaDoFn}
     * should stop reading from the given {@link TopicPartition}.
     */
    public ReadSourceDescriptors<K, V> withCheckStopReadingFn(
        @Nullable SerializableFunction<TopicPartition, Boolean> checkStopReadingFn) {
      return toBuilder()
          .setCheckStopReadingFn(CheckStopReadingFnWrapper.of(checkStopReadingFn))
          .build();
    }

    /**
     * Updates configuration for the main consumer. This method merges updates from the provided map
     * with any prior updates using {@link KafkaIOUtils#DEFAULT_CONSUMER_PROPERTIES} as the starting
     * configuration.
     *
     * <p>In {@link ReadFromKafkaDoFn}, there're two consumers running in the backend:
     *
     * <ol>
     *   <li>the main consumer which reads data from kafka.
     *   <li>the secondary offset consumer which is used to estimate the backlog by fetching the
     *       latest offset.
     * </ol>
     *
     * <p>See {@link #withConsumerConfigOverrides} for overriding the configuration instead of
     * updating it.
     *
     * <p>See {@link #withOffsetConsumerConfigOverrides} for configuring the secondary offset
     * consumer.
     */
    public ReadSourceDescriptors<K, V> withConsumerConfigUpdates(
        Map<String, Object> configUpdates) {
      Map<String, Object> config =
          KafkaIOUtils.updateKafkaProperties(getConsumerConfig(), configUpdates);
      return toBuilder().setConsumerConfig(config).build();
    }

    /**
     * A function to calculate output timestamp for a given {@link KafkaRecord}. The default value
     * is {@link #withProcessingTime()}.
     */
    public ReadSourceDescriptors<K, V> withExtractOutputTimestampFn(
        SerializableFunction<KafkaRecord<K, V>, Instant> fn) {
      return toBuilder().setExtractOutputTimestampFn(fn).build();
    }

    /**
     * A function to create a {@link WatermarkEstimator}. The default value is {@link
     * MonotonicallyIncreasing}.
     */
    public ReadSourceDescriptors<K, V> withCreatWatermarkEstimatorFn(
        SerializableFunction<Instant, WatermarkEstimator<Instant>> fn) {
      return toBuilder().setCreateWatermarkEstimatorFn(fn).build();
    }

    /** Use the log append time as the output timestamp. */
    public ReadSourceDescriptors<K, V> withLogAppendTime() {
      return withExtractOutputTimestampFn(
          ReadSourceDescriptors.ExtractOutputTimestampFns.useLogAppendTime());
    }

    /** Use the processing time as the output timestamp. */
    public ReadSourceDescriptors<K, V> withProcessingTime() {
      return withExtractOutputTimestampFn(
          ReadSourceDescriptors.ExtractOutputTimestampFns.useProcessingTime());
    }

    /** Enable Redistribute. */
    public ReadSourceDescriptors<K, V> withRedistribute() {
      return toBuilder().setRedistribute(true).build();
    }

    public ReadSourceDescriptors<K, V> withAllowDuplicates() {
      return toBuilder().setAllowDuplicates(true).build();
    }

    public ReadSourceDescriptors<K, V> withRedistributeNumKeys(int redistributeNumKeys) {
      return toBuilder().setRedistributeNumKeys(redistributeNumKeys).build();
    }

    /** Use the creation time of {@link KafkaRecord} as the output timestamp. */
    public ReadSourceDescriptors<K, V> withCreateTime() {
      return withExtractOutputTimestampFn(
          ReadSourceDescriptors.ExtractOutputTimestampFns.useCreateTime());
    }

    /** Use the {@link WallTime} as the watermark estimator. */
    public ReadSourceDescriptors<K, V> withWallTimeWatermarkEstimator() {
      return withCreatWatermarkEstimatorFn(
          state -> {
            return new WallTime(state);
          });
    }

    /** Use the {@link MonotonicallyIncreasing} as the watermark estimator. */
    public ReadSourceDescriptors<K, V> withMonotonicallyIncreasingWatermarkEstimator() {
      return withCreatWatermarkEstimatorFn(
          state -> {
            return new MonotonicallyIncreasing(state);
          });
    }

    /** Use the {@link Manual} as the watermark estimator. */
    public ReadSourceDescriptors<K, V> withManualWatermarkEstimator() {
      return withCreatWatermarkEstimatorFn(
          state -> {
            return new Manual(state);
          });
    }

    /**
     * Sets "isolation_level" to "read_committed" in Kafka consumer configuration. This ensures that
     * the consumer does not read uncommitted messages. Kafka version 0.11 introduced transactional
     * writes. Applications requiring end-to-end exactly-once semantics should only read committed
     * messages. See JavaDoc for {@link KafkaConsumer} for more description.
     */
    public ReadSourceDescriptors<K, V> withReadCommitted() {
      return withConsumerConfigUpdates(ImmutableMap.of("isolation.level", "read_committed"));
    }

    /**
     * Enable committing record offset. If {@link ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG} is set
     * together with {@link #commitOffsets()}, {@link #commitOffsets()} will be ignored.
     */
    public ReadSourceDescriptors<K, V> commitOffsets() {
      return toBuilder().setCommitOffsetEnabled(true).build();
    }

    /**
     * Set additional configuration for the offset consumer. It may be required for a secured Kafka
     * cluster, especially when you see similar WARN log message {@code exception while fetching
     * latest offset for partition {}. will be retried}.
     *
     * <p>In {@link ReadFromKafkaDoFn}, there are two consumers running in the backend:
     *
     * <ol>
     *   <li>the main consumer which reads data from kafka.
     *   <li>the secondary offset consumer which is used to estimate the backlog by fetching the
     *       latest offset.
     * </ol>
     *
     * <p>By default, offset consumer inherits the configuration from main consumer, with an
     * auto-generated {@link ConsumerConfig#GROUP_ID_CONFIG}. This may not work in a secured Kafka
     * which requires additional configuration.
     *
     * <p>See {@link #withConsumerConfigUpdates} for configuring the main consumer.
     */
    public ReadSourceDescriptors<K, V> withOffsetConsumerConfigOverrides(
        @Nullable Map<String, Object> offsetConsumerConfig) {
      return toBuilder().setOffsetConsumerConfig(offsetConsumerConfig).build();
    }

    /**
     * Replaces the configuration for the main consumer.
     *
     * <p>In {@link ReadFromKafkaDoFn}, there are two consumers running in the backend:
     *
     * <ol>
     *   <li>the main consumer which reads data from kafka.
     *   <li>the secondary offset consumer which is used to estimate the backlog by fetching the
     *       latest offset.
     * </ol>
     *
     * <p>By default, main consumer uses the configuration from {@link
     * KafkaIOUtils#DEFAULT_CONSUMER_PROPERTIES}.
     *
     * <p>See {@link #withConsumerConfigUpdates} for updating the configuration instead of
     * overriding it.
     */
    public ReadSourceDescriptors<K, V> withConsumerConfigOverrides(
        Map<String, Object> consumerConfig) {
      return toBuilder().setConsumerConfig(consumerConfig).build();
    }

    public ReadSourceDescriptors<K, V> withBadRecordErrorHandler(
        ErrorHandler<BadRecord, ?> errorHandler) {
      return toBuilder()
          .setBadRecordRouter(BadRecordRouter.RECORDING_ROUTER)
          .setBadRecordErrorHandler(errorHandler)
          .build();
    }

    /**
     * Sets the timeout time in seconds for Kafka consumer polling request in the {@link
     * ReadFromKafkaDoFn}. A lower timeout optimizes for latency. Increase the timeout if the
     * consumer is not fetching any records. The default is 2 seconds.
     */
    public ReadSourceDescriptors<K, V> withConsumerPollingTimeout(long duration) {
      return toBuilder().setConsumerPollingTimeout(duration).build();
    }

    ReadAllFromRow<K, V> forExternalBuild() {
      return new ReadAllFromRow<>(this);
    }

    /**
     * A transform that is used in cross-language case. The input {@link Row} should be encoded with
     * an equivalent schema as {@link KafkaSourceDescriptor}.
     */
    private static class ReadAllFromRow<K, V>
        extends PTransform<PCollection<Row>, PCollection<KV<K, V>>> {

      private final ReadSourceDescriptors<K, V> readViaSDF;

      ReadAllFromRow(ReadSourceDescriptors<K, V> read) {
        readViaSDF = read;
      }

      @Override
      public PCollection<KV<K, V>> expand(PCollection<Row> input) {
        return input
            .apply(Convert.fromRows(KafkaSourceDescriptor.class))
            .apply(readViaSDF)
            .apply(
                ParDo.of(
                    new DoFn<KafkaRecord<K, V>, KV<K, V>>() {
                      @ProcessElement
                      public void processElement(
                          @Element KafkaRecord<K, V> element,
                          OutputReceiver<KV<K, V>> outputReceiver) {
                        outputReceiver.output(element.getKV());
                      }
                    }))
            .setCoder(
                KvCoder.<K, V>of(
                    Preconditions.checkStateNotNull(readViaSDF.getKeyCoder()),
                    Preconditions.checkStateNotNull(readViaSDF.getValueCoder())));
      }
    }

    /**
     * Set the {@link TimestampPolicyFactory}. If the {@link TimestampPolicyFactory} is given, the
     * output timestamp will be computed by the {@link
     * TimestampPolicyFactory#createTimestampPolicy(TopicPartition, Optional)} and {@link Manual} is
     * used as the watermark estimator.
     */
    ReadSourceDescriptors<K, V> withTimestampPolicyFactory(
        TimestampPolicyFactory<K, V> timestampPolicyFactory) {
      return toBuilder()
          .setTimestampPolicyFactory(timestampPolicyFactory)
          .build()
          .withManualWatermarkEstimator();
    }

    /** Enable treating the Kafka sources as bounded as opposed to the unbounded default. */
    ReadSourceDescriptors<K, V> withBounded() {
      return toBuilder().setBounded(true).build();
    }

    @Override
    public PCollection<KafkaRecord<K, V>> expand(PCollection<KafkaSourceDescriptor> input) {
      checkArgument(getKeyDeserializerProvider() != null, "withKeyDeserializer() is required");
      checkArgument(getValueDeserializerProvider() != null, "withValueDeserializer() is required");

      if (!ConsumerSpEL.hasOffsetsForTimes()) {
        LOG.warn(
            "Kafka client version {} is too old. Versions before 0.10.1.0 are deprecated and "
                + "may not be supported in next release of Apache Beam. "
                + "Please upgrade your Kafka client version.",
            AppInfoParser.getVersion());
      }

      if (isCommitOffsetEnabled()) {
        if (configuredKafkaCommit()) {
          LOG.info(
              "auto_commit is set together with commitOffsetEnabled but you only need one of them. "
                  + "The commitOffsetEnabled is going to be ignored");
        }
      }

      if (isRedistribute()) {
        if (getRedistributeNumKeys() == 0) {
          LOG.warn("This will create a key per record, which is sub-optimal for most use cases.");
        }
      }

      if (getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
        LOG.warn(
            "The bootstrapServers is not set. It must be populated through the KafkaSourceDescriptor during runtime otherwise the pipeline will fail.");
      }

      CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
      Coder<K> keyCoder = getKeyCoder(coderRegistry);
      Coder<V> valueCoder = getValueCoder(coderRegistry);
      Coder<KafkaRecord<K, V>> recordCoder = KafkaRecordCoder.of(keyCoder, valueCoder);

      try {
        PCollectionTuple pCollectionTuple =
            input.apply(
                ParDo.of(ReadFromKafkaDoFn.<K, V>create(this, records))
                    .withOutputTags(records, TupleTagList.of(BadRecordRouter.BAD_RECORD_TAG)));
        getBadRecordErrorHandler()
            .addErrorCollection(
                pCollectionTuple
                    .get(BadRecordRouter.BAD_RECORD_TAG)
                    .setCoder(BadRecord.getCoder(input.getPipeline())));
        PCollection<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> outputWithDescriptor =
            pCollectionTuple
                .get(records)
                .setCoder(
                    KvCoder.of(
                        input
                            .getPipeline()
                            .getSchemaRegistry()
                            .getSchemaCoder(KafkaSourceDescriptor.class),
                        recordCoder));

        boolean applyCommitOffsets =
            isCommitOffsetEnabled() && !configuredKafkaCommit() && !isRedistribute();
        if (!applyCommitOffsets) {
          return outputWithDescriptor
              .apply(MapElements.into(new TypeDescriptor<KafkaRecord<K, V>>() {}).via(KV::getValue))
              .setCoder(recordCoder);
        }

        // Add transform for committing offsets to Kafka with consistency with beam pipeline data
        // processing.
        String requestedVersionString =
            input
                .getPipeline()
                .getOptions()
                .as(StreamingOptions.class)
                .getUpdateCompatibilityVersion();
        if (requestedVersionString != null) {
          List<String> requestedVersion = Arrays.asList(requestedVersionString.split("\\."));
          List<String> targetVersion = Arrays.asList("2", "60", "0");

          if (Comparators.lexicographical(Comparator.<String>naturalOrder())
                  .compare(requestedVersion, targetVersion)
              < 0) {
            return expand259Commits(
                outputWithDescriptor, recordCoder, input.getPipeline().getSchemaRegistry());
          }
        }
        outputWithDescriptor.apply(new KafkaCommitOffset<>(this, false)).setCoder(VoidCoder.of());
        return outputWithDescriptor
            .apply(MapElements.into(new TypeDescriptor<KafkaRecord<K, V>>() {}).via(KV::getValue))
            .setCoder(recordCoder);
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e.getMessage());
      }
    }

    private PCollection<KafkaRecord<K, V>> expand259Commits(
        PCollection<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> outputWithDescriptor,
        Coder<KafkaRecord<K, V>> recordCoder,
        SchemaRegistry schemaRegistry)
        throws NoSuchSchemaException {
      // Reshuffles the data and then branches off applying commit offsets.
      outputWithDescriptor =
          outputWithDescriptor
              .apply(Reshuffle.viaRandomKey())
              .setCoder(
                  KvCoder.of(
                      schemaRegistry.getSchemaCoder(KafkaSourceDescriptor.class), recordCoder));
      outputWithDescriptor.apply(new KafkaCommitOffset<>(this, true)).setCoder(VoidCoder.of());
      return outputWithDescriptor
          .apply(MapElements.into(new TypeDescriptor<KafkaRecord<K, V>>() {}).via(KV::getValue))
          .setCoder(recordCoder);
    }

    private Coder<K> getKeyCoder(CoderRegistry coderRegistry) {
      return (getKeyCoder() != null)
          ? getKeyCoder()
          : Preconditions.checkStateNotNull(getKeyDeserializerProvider()).getCoder(coderRegistry);
    }

    private Coder<V> getValueCoder(CoderRegistry coderRegistry) {
      return (getValueCoder() != null)
          ? getValueCoder()
          : Preconditions.checkStateNotNull(getValueDeserializerProvider()).getCoder(coderRegistry);
    }

    private boolean configuredKafkaCommit() {
      return Boolean.TRUE.equals(getConsumerConfig().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
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
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  private static final Logger LOG = LoggerFactory.getLogger(KafkaIO.class);

  /** Static class, prevent instantiation. */
  private KafkaIO() {}

  //////////////////////// Sink Support \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  /**
   * A {@link PTransform} to write to a Kafka topic with ProducerRecord's. See {@link KafkaIO} for
   * more information on usage and configuration.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  public abstract static class WriteRecords<K, V>
      extends PTransform<PCollection<ProducerRecord<K, V>>, PDone> {
    // TODO (Version 3.0): Create the only one generic {@code Write<T>} transform which will be
    // parameterized depending on type of input collection (KV, ProducerRecords, etc). In such case,
    // we shouldn't have to duplicate the same API for similar transforms like {@link Write} and
    // {@link WriteRecords}. See example at {@link PubsubIO.Write}.

    @Pure
    public abstract @Nullable String getTopic();

    @Pure
    public abstract Map<String, Object> getProducerConfig();

    @Pure
    public abstract @Nullable SerializableFunction<Map<String, Object>, Producer<K, V>>
        getProducerFactoryFn();

    @Pure
    public abstract @Nullable Class<? extends Serializer<K>> getKeySerializer();

    @Pure
    public abstract @Nullable Class<? extends Serializer<V>> getValueSerializer();

    @Pure
    public abstract @Nullable KafkaPublishTimestampFunction<ProducerRecord<K, V>>
        getPublishTimestampFunction();

    // Configuration for EOS sink
    @Pure
    public abstract boolean isEOS();

    @Pure
    public abstract @Nullable String getSinkGroupId();

    @Pure
    public abstract int getNumShards();

    @Pure
    public abstract @Nullable SerializableFunction<Map<String, Object>, ? extends Consumer<?, ?>>
        getConsumerFactoryFn();

    @Pure
    public abstract BadRecordRouter getBadRecordRouter();

    @Pure
    public abstract ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setTopic(String topic);

      abstract Builder<K, V> setProducerConfig(Map<String, Object> producerConfig);

      abstract Builder<K, V> setProducerFactoryFn(
          @Nullable SerializableFunction<Map<String, Object>, Producer<K, V>> fn);

      abstract Builder<K, V> setKeySerializer(@Nullable Class<? extends Serializer<K>> serializer);

      abstract Builder<K, V> setValueSerializer(Class<? extends Serializer<V>> serializer);

      abstract Builder<K, V> setPublishTimestampFunction(
          KafkaPublishTimestampFunction<ProducerRecord<K, V>> timestampFunction);

      abstract Builder<K, V> setEOS(boolean eosEnabled);

      abstract Builder<K, V> setSinkGroupId(String sinkGroupId);

      abstract Builder<K, V> setNumShards(int numShards);

      abstract Builder<K, V> setConsumerFactoryFn(
          SerializableFunction<Map<String, Object>, ? extends Consumer<?, ?>> fn);

      abstract Builder<K, V> setBadRecordRouter(BadRecordRouter router);

      abstract Builder<K, V> setBadRecordErrorHandler(
          ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      abstract WriteRecords<K, V> build();
    }

    /**
     * Returns a new {@link Write} transform with Kafka producer pointing to {@code
     * bootstrapServers}.
     */
    public WriteRecords<K, V> withBootstrapServers(String bootstrapServers) {
      return withProducerConfigUpdates(
          ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /**
     * Sets the default Kafka topic to write to. Use {@code ProducerRecords} to set topic name per
     * published record.
     */
    public WriteRecords<K, V> withTopic(String topic) {
      return toBuilder().setTopic(topic).build();
    }

    /**
     * Sets a {@link Serializer} for serializing key (if any) to bytes.
     *
     * <p>A key is optional while writing to Kafka. Note when a key is set, its hash is used to
     * determine partition in Kafka (see {@link ProducerRecord} for more details).
     */
    public WriteRecords<K, V> withKeySerializer(Class<? extends Serializer<K>> keySerializer) {
      return toBuilder().setKeySerializer(keySerializer).build();
    }

    /** Sets a {@link Serializer} for serializing value to bytes. */
    public WriteRecords<K, V> withValueSerializer(Class<? extends Serializer<V>> valueSerializer) {
      return toBuilder().setValueSerializer(valueSerializer).build();
    }

    /**
     * Adds the given producer properties, overriding old values of properties with the same key.
     *
     * @deprecated as of version 2.13. Use {@link #withProducerConfigUpdates(Map)} instead.
     */
    @Deprecated
    public WriteRecords<K, V> updateProducerProperties(Map<String, Object> configUpdates) {
      Map<String, Object> config =
          KafkaIOUtils.updateKafkaProperties(getProducerConfig(), configUpdates);
      return toBuilder().setProducerConfig(config).build();
    }

    /**
     * Update configuration for the producer. Note that the default producer properties will not be
     * completely overridden. This method only updates the value which has the same key.
     *
     * <p>By default, the producer uses the configuration from {@link #DEFAULT_PRODUCER_PROPERTIES}.
     */
    public WriteRecords<K, V> withProducerConfigUpdates(Map<String, Object> configUpdates) {
      Map<String, Object> config =
          KafkaIOUtils.updateKafkaProperties(getProducerConfig(), configUpdates);
      return toBuilder().setProducerConfig(config).build();
    }

    /**
     * Sets a custom function to create Kafka producer. Primarily used for tests. Default is {@link
     * KafkaProducer}
     */
    public WriteRecords<K, V> withProducerFactoryFn(
        SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn) {
      return toBuilder().setProducerFactoryFn(producerFactoryFn).build();
    }

    /**
     * The timestamp for each record being published is set to timestamp of the element in the
     * pipeline. This is equivalent to {@code withPublishTimestampFunction((e, ts) -> ts)}. <br>
     * NOTE: Kafka's retention policies are based on message timestamps. If the pipeline is
     * processing messages from the past, they might be deleted immediately by Kafka after being
     * published if the timestamps are older than Kafka cluster's {@code log.retention.hours}.
     */
    public WriteRecords<K, V> withInputTimestamp() {
      return withPublishTimestampFunction(KafkaPublishTimestampFunction.withElementTimestamp());
    }

    /**
     * A function to provide timestamp for records being published. <br>
     * NOTE: Kafka's retention policies are based on message timestamps. If the pipeline is
     * processing messages from the past, they might be deleted immediately by Kafka after being
     * published if the timestamps are older than Kafka cluster's {@code log.retention.hours}.
     *
     * @deprecated use {@code ProducerRecords} to set publish timestamp.
     */
    @Deprecated
    public WriteRecords<K, V> withPublishTimestampFunction(
        KafkaPublishTimestampFunction<ProducerRecord<K, V>> timestampFunction) {
      return toBuilder().setPublishTimestampFunction(timestampFunction).build();
    }

    /**
     * Provides exactly-once semantics while writing to Kafka, which enables applications with
     * end-to-end exactly-once guarantees on top of exactly-once semantics <i>within</i> Beam
     * pipelines. It ensures that records written to sink are committed on Kafka exactly once, even
     * in the case of retries during pipeline execution even when some processing is retried.
     * Retries typically occur when workers restart (as in failure recovery), or when the work is
     * redistributed (as in an autoscaling event).
     *
     * <p>Beam runners typically provide exactly-once semantics for results of a pipeline, but not
     * for side effects from user code in transform. If a transform such as Kafka sink writes to an
     * external system, those writes might occur more than once. When EOS is enabled here, the sink
     * transform ties checkpointing semantics in compatible Beam runners and transactions in Kafka
     * (version 0.11+) to ensure a record is written only once. As the implementation relies on
     * runners checkpoint semantics, not all the runners are compatible. The sink throws an
     * exception during initialization if the runner is not explicitly allowed. The Dataflow, Flink,
     * and Spark runners are compatible.
     *
     * <p>Note on performance: Exactly-once sink involves two shuffles of the records. In addition
     * to cost of shuffling the records among workers, the records go through 2
     * serialization-deserialization cycles. Depending on volume and cost of serialization, the CPU
     * cost might be noticeable. The CPU cost can be reduced by writing byte arrays (i.e.
     * serializing them to byte before writing to Kafka sink).
     *
     * @param numShards Sets sink parallelism. The state metadata stored on Kafka is stored across
     *     this many virtual partitions using {@code sinkGroupId}. A good rule of thumb is to set
     *     this to be around number of partitions in Kafka topic.
     * @param sinkGroupId The <i>group id</i> used to store small amount of state as metadata on
     *     Kafka. It is similar to <i>consumer group id</i> used with a {@link KafkaConsumer}. Each
     *     job should use a unique group id so that restarts/updates of job preserve the state to
     *     ensure exactly-once semantics. The state is committed atomically with sink transactions
     *     on Kafka. See {@link KafkaProducer#sendOffsetsToTransaction(Map, String)} for more
     *     information. The sink performs multiple sanity checks during initialization to catch
     *     common mistakes so that it does not end up using state that does not <i>seem</i> to be
     *     written by the same job.
     */
    public WriteRecords<K, V> withEOS(int numShards, String sinkGroupId) {
      KafkaExactlyOnceSink.ensureEOSSupport();
      checkArgument(numShards >= 1, "numShards should be >= 1");
      checkArgument(sinkGroupId != null, "sinkGroupId is required for exactly-once sink");
      return toBuilder().setEOS(true).setNumShards(numShards).setSinkGroupId(sinkGroupId).build();
    }

    /**
     * When exactly-once semantics are enabled (see {@link #withEOS(int, String)}), the sink needs
     * to fetch previously stored state with Kafka topic. Fetching the metadata requires a consumer.
     * Similar to {@link Read#withConsumerFactoryFn(SerializableFunction)}, a factory function can
     * be supplied if required in a specific case. The default is {@link KafkaConsumer}.
     */
    public WriteRecords<K, V> withConsumerFactoryFn(
        SerializableFunction<Map<String, Object>, ? extends Consumer<?, ?>> consumerFactoryFn) {
      return toBuilder().setConsumerFactoryFn(consumerFactoryFn).build();
    }

    public WriteRecords<K, V> withBadRecordErrorHandler(
        ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      return toBuilder()
          .setBadRecordRouter(BadRecordRouter.RECORDING_ROUTER)
          .setBadRecordErrorHandler(badRecordErrorHandler)
          .build();
    }

    @Override
    public PDone expand(PCollection<ProducerRecord<K, V>> input) {
      checkArgument(
          getProducerConfig().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) != null,
          "withBootstrapServers() is required");

      checkArgument(getKeySerializer() != null, "withKeySerializer() is required");
      checkArgument(getValueSerializer() != null, "withValueSerializer() is required");

      if (isEOS()) {
        checkArgument(getTopic() != null, "withTopic() is required when isEOS() is true");
        checkArgument(
            getBadRecordErrorHandler() instanceof DefaultErrorHandler,
            "BadRecordErrorHandling isn't supported with Kafka Exactly Once writing");
        KafkaExactlyOnceSink.ensureEOSSupport();

        // TODO: Verify that the group_id does not have existing state stored on Kafka unless
        //       this is an upgrade. This avoids issues with simple mistake of reusing group_id
        //       across multiple runs or across multiple jobs. This is checked when the sink
        //       transform initializes while processing the output. It might be better to
        //       check here to catch common mistake.

        input.apply(new KafkaExactlyOnceSink<>(this));
      } else {
        // Even though the errors are the only output from writing to Kafka, we maintain a
        // PCollectionTuple
        // with a void tag as the 'primary' output for easy forward compatibility
        PCollectionTuple pCollectionTuple =
            input.apply(
                ParDo.of(new KafkaWriter<>(this))
                    .withOutputTags(
                        new TupleTag<Void>(), TupleTagList.of(BadRecordRouter.BAD_RECORD_TAG)));
        getBadRecordErrorHandler()
            .addErrorCollection(
                pCollectionTuple
                    .get(BadRecordRouter.BAD_RECORD_TAG)
                    .setCoder(BadRecord.getCoder(input.getPipeline())));
      }
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(@Nullable PipelineOptions options) {
      Preconditions.checkStateNotNull(options);
      if (isEOS()) {
        String runner = options.getRunner().getName();
        if ("org.apache.beam.runners.direct.DirectRunner".equals(runner)
            || runner.startsWith("org.apache.beam.runners.dataflow.")
            || runner.startsWith("org.apache.beam.runners.spark.")
            || runner.startsWith("org.apache.beam.runners.flink.")) {
          return;
        }
        throw new UnsupportedOperationException(
            runner
                + " is not a runner known to be compatible with Kafka exactly-once sink. This"
                + " implementation of exactly-once sink relies on specific checkpoint guarantees."
                + " Only the runners with known to have compatible checkpoint semantics are"
                + " allowed.");
      }
    }

    // set config defaults
    private static final Map<String, Object> DEFAULT_PRODUCER_PROPERTIES =
        ImmutableMap.of(ProducerConfig.RETRIES_CONFIG, 3);

    /** A set of properties that are not required or don't make sense for our producer. */
    private static final Map<String, String> IGNORED_PRODUCER_PROPERTIES =
        ImmutableMap.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "Use withKeySerializer instead",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "Use withValueSerializer instead");

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("topic", getTopic()).withLabel("Topic"));
      Set<String> ignoredProducerPropertiesKeys = IGNORED_PRODUCER_PROPERTIES.keySet();
      for (Map.Entry<String, Object> conf : getProducerConfig().entrySet()) {
        String key = conf.getKey();
        if (!ignoredProducerPropertiesKeys.contains(key)) {
          Object value =
              DisplayData.inferType(conf.getValue()) != null
                  ? conf.getValue()
                  : String.valueOf(conf.getValue());
          builder.add(DisplayData.item(key, ValueProvider.StaticValueProvider.of(value)));
        }
      }
    }
  }

  /**
   * A {@link PTransform} to write to a Kafka topic with KVs . See {@link KafkaIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  public abstract static class Write<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {
    // TODO (Version 3.0): Create the only one generic {@code Write<T>} transform which will be
    // parameterized depending on type of input collection (KV, ProducerRecords, etc). In such case,
    // we shouldn't have to duplicate the same API for similar transforms like {@link Write} and
    // {@link WriteRecords}. See example at {@link PubsubIO.Write}.

    public static final Class<AutoValue_KafkaIO_Write> AUTOVALUE_CLASS =
        AutoValue_KafkaIO_Write.class;

    abstract @Nullable String getTopic();

    public abstract WriteRecords<K, V> getWriteRecordsTransform();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V>
        implements ExternalTransformBuilder<
            Write.External.Configuration, PCollection<KV<K, V>>, PDone> {
      abstract Builder<K, V> setTopic(String topic);

      abstract Builder<K, V> setWriteRecordsTransform(WriteRecords<K, V> transform);

      abstract Write<K, V> build();

      @Override
      public PTransform<PCollection<KV<K, V>>, PDone> buildExternal(
          Write.External.Configuration configuration) {
        setTopic(configuration.topic);

        Map<String, Object> producerConfig = new HashMap<>(configuration.producerConfig);
        Class<Serializer<K>> keySerializer =
            (Class<Serializer<K>>) resolveClass(configuration.keySerializer);
        Class<Serializer<V>> valSerializer =
            (Class<Serializer<V>>) resolveClass(configuration.valueSerializer);

        WriteRecords<K, V> writeRecords =
            KafkaIO.<K, V>writeRecords()
                .withProducerConfigUpdates(producerConfig)
                .withKeySerializer(keySerializer)
                .withValueSerializer(valSerializer)
                .withTopic(configuration.topic);
        setWriteRecordsTransform(writeRecords);

        return build();
      }
    }

    /** Exposes {@link KafkaIO.Write} as an external transform for cross-language usage. */
    @AutoService(ExternalTransformRegistrar.class)
    public static class External implements ExternalTransformRegistrar {

      public static final String URN = "beam:transform:org.apache.beam:kafka_write:v1";

      @Override
      public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
        return ImmutableMap.of(
            URN,
            (Class<KafkaIO.Write.Builder<?, ?>>) (Class<?>) AutoValue_KafkaIO_Write.Builder.class);
      }

      /** Parameters class to expose the Write transform to an external SDK. */
      @SuppressWarnings("initialization")
      public static class Configuration {

        private Map<String, String> producerConfig;
        private String topic;
        private String keySerializer;
        private String valueSerializer;

        public void setProducerConfig(Map<String, String> producerConfig) {
          this.producerConfig = producerConfig;
        }

        public void setTopic(String topic) {
          this.topic = topic;
        }

        public void setKeySerializer(String keySerializer) {
          this.keySerializer = keySerializer;
        }

        public void setValueSerializer(String valueSerializer) {
          this.valueSerializer = valueSerializer;
        }
      }
    }

    /** Used mostly to reduce using of boilerplate of wrapping {@link WriteRecords} methods. */
    private Write<K, V> withWriteRecordsTransform(WriteRecords<K, V> transform) {
      return toBuilder().setWriteRecordsTransform(transform).build();
    }

    /**
     * Wrapper method over {@link WriteRecords#withBootstrapServers(String)}, used to keep the
     * compatibility with old API based on KV type of element.
     */
    public Write<K, V> withBootstrapServers(String bootstrapServers) {
      return withWriteRecordsTransform(
          getWriteRecordsTransform().withBootstrapServers(bootstrapServers));
    }

    /**
     * Wrapper method over {@link WriteRecords#withTopic(String)}, used to keep the compatibility
     * with old API based on KV type of element.
     */
    public Write<K, V> withTopic(String topic) {
      return toBuilder()
          .setTopic(topic)
          .setWriteRecordsTransform(getWriteRecordsTransform().withTopic(topic))
          .build();
    }

    /**
     * Wrapper method over {@link WriteRecords#withKeySerializer(Class)}, used to keep the
     * compatibility with old API based on KV type of element.
     */
    public Write<K, V> withKeySerializer(Class<? extends Serializer<K>> keySerializer) {
      return withWriteRecordsTransform(getWriteRecordsTransform().withKeySerializer(keySerializer));
    }

    /**
     * Wrapper method over {@link WriteRecords#withValueSerializer(Class)}, used to keep the
     * compatibility with old API based on KV type of element.
     */
    public Write<K, V> withValueSerializer(Class<? extends Serializer<V>> valueSerializer) {
      return withWriteRecordsTransform(
          getWriteRecordsTransform().withValueSerializer(valueSerializer));
    }

    /**
     * Wrapper method over {@link WriteRecords#withProducerFactoryFn(SerializableFunction)}, used to
     * keep the compatibility with old API based on KV type of element.
     */
    public Write<K, V> withProducerFactoryFn(
        SerializableFunction<Map<String, Object>, Producer<K, V>> producerFactoryFn) {
      return withWriteRecordsTransform(
          getWriteRecordsTransform().withProducerFactoryFn(producerFactoryFn));
    }

    /**
     * Wrapper method over {@link WriteRecords#withInputTimestamp()}, used to keep the compatibility
     * with old API based on KV type of element.
     */
    public Write<K, V> withInputTimestamp() {
      return withWriteRecordsTransform(getWriteRecordsTransform().withInputTimestamp());
    }

    /**
     * Wrapper method over {@link
     * WriteRecords#withPublishTimestampFunction(KafkaPublishTimestampFunction)}, used to keep the
     * compatibility with old API based on KV type of element.
     *
     * @deprecated use {@link WriteRecords} and {@code ProducerRecords} to set publish timestamp.
     */
    @Deprecated
    public Write<K, V> withPublishTimestampFunction(
        KafkaPublishTimestampFunction<KV<K, V>> timestampFunction) {
      return withWriteRecordsTransform(
          getWriteRecordsTransform()
              .withPublishTimestampFunction(new PublishTimestampFunctionKV<>(timestampFunction)));
    }

    /**
     * Wrapper method over {@link WriteRecords#withEOS(int, String)}, used to keep the compatibility
     * with old API based on KV type of element.
     */
    public Write<K, V> withEOS(int numShards, String sinkGroupId) {
      return withWriteRecordsTransform(getWriteRecordsTransform().withEOS(numShards, sinkGroupId));
    }

    /**
     * Wrapper method over {@link WriteRecords#withConsumerFactoryFn(SerializableFunction)}, used to
     * keep the compatibility with old API based on KV type of element.
     */
    public Write<K, V> withConsumerFactoryFn(
        SerializableFunction<Map<String, Object>, ? extends Consumer<?, ?>> consumerFactoryFn) {
      return withWriteRecordsTransform(
          getWriteRecordsTransform().withConsumerFactoryFn(consumerFactoryFn));
    }

    /**
     * Adds the given producer properties, overriding old values of properties with the same key.
     *
     * @deprecated as of version 2.13. Use {@link #withProducerConfigUpdates(Map)} instead.
     */
    @Deprecated
    public Write<K, V> updateProducerProperties(Map<String, Object> configUpdates) {
      return withWriteRecordsTransform(
          getWriteRecordsTransform().updateProducerProperties(configUpdates));
    }

    /**
     * Update configuration for the producer. Note that the default producer properties will not be
     * completely overridden. This method only updates the value which has the same key.
     *
     * <p>By default, the producer uses the configuration from {@link
     * WriteRecords#DEFAULT_PRODUCER_PROPERTIES}.
     */
    public Write<K, V> withProducerConfigUpdates(Map<String, Object> configUpdates) {
      return withWriteRecordsTransform(
          getWriteRecordsTransform().withProducerConfigUpdates(configUpdates));
    }

    /**
     * Configure a {@link BadRecordErrorHandler} for sending records to if they fail to serialize
     * when being sent to Kafka.
     */
    public Write<K, V> withBadRecordErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      return withWriteRecordsTransform(
          getWriteRecordsTransform().withBadRecordErrorHandler(badRecordErrorHandler));
    }

    @Override
    public PDone expand(PCollection<KV<K, V>> input) {
      final String topic = Preconditions.checkStateNotNull(getTopic(), "withTopic() is required");

      KvCoder<K, V> kvCoder = (KvCoder<K, V>) input.getCoder();
      return input
          .apply(
              "Kafka ProducerRecord",
              MapElements.via(
                  new SimpleFunction<KV<K, V>, ProducerRecord<K, V>>() {
                    @Override
                    public ProducerRecord<K, V> apply(KV<K, V> element) {
                      return new ProducerRecord<>(topic, element.getKey(), element.getValue());
                    }
                  }))
          .setCoder(ProducerRecordCoder.of(kvCoder.getKeyCoder(), kvCoder.getValueCoder()))
          .apply(getWriteRecordsTransform());
    }

    @Override
    public void validate(@Nullable PipelineOptions options) {
      getWriteRecordsTransform().validate(options);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      getWriteRecordsTransform().populateDisplayData(builder);
    }

    /**
     * Writes just the values to Kafka. This is useful for writing collections of values rather
     * thank {@link KV}s.
     */
    public PTransform<PCollection<V>, PDone> values() {
      // KafkaValueWrite requires a write transform that can handle null keys.
      // Since it will only pass null, we can re-use the current transform safely
      // with this nullable key serializer
      Class<? extends Serializer<K>> nullKeySerializer =
          (Class<? extends Serializer<K>>) (Class<?>) StringSerializer.class;
      Write<@Nullable K, V> nullKeyWriteTransform =
          (Write<@Nullable K, V>) this.withKeySerializer(nullKeySerializer);
      return new KafkaValueWrite<>(nullKeyWriteTransform);
    }

    /**
     * Wrapper class which allows to use {@code KafkaPublishTimestampFunction<KV<K, V>} with {@link
     * WriteRecords#withPublishTimestampFunction(KafkaPublishTimestampFunction)}.
     */
    private static class PublishTimestampFunctionKV<K, V>
        implements KafkaPublishTimestampFunction<ProducerRecord<K, V>> {

      private KafkaPublishTimestampFunction<KV<K, V>> fn;

      public PublishTimestampFunctionKV(KafkaPublishTimestampFunction<KV<K, V>> fn) {
        this.fn = fn;
      }

      @Override
      public Instant getTimestamp(ProducerRecord<K, V> e, Instant ts) {
        return fn.getTimestamp(KV.of(e.key(), e.value()), ts);
      }
    }
  }

  /**
   * Same as {@code Write<K, V>} without a Key. Null is used for key as it is the convention is
   * Kafka when there is no key specified. Majority of Kafka writers don't specify a key.
   */
  private static class KafkaValueWrite<V> extends PTransform<PCollection<V>, PDone> {
    private final Write<@Nullable ?, V> kvWriteTransform;

    private KafkaValueWrite(Write<@Nullable ?, V> kvWriteTransform) {
      this.kvWriteTransform = kvWriteTransform;
    }

    @Override
    public PDone expand(PCollection<V> input) {
      return (PDone)
          input
              .apply(
                  "Kafka values with default key",
                  MapElements.via(
                      new SimpleFunction<V, KV<@Nullable Object, V>>() {
                        @Override
                        public KV<@Nullable Object, V> apply(V element) {
                          return KV.of(null, element);
                        }
                      }))
              .setCoder(KvCoder.of(new NullOnlyCoder<>(), input.getCoder()))
              .apply((Write<@Nullable Object, V>) kvWriteTransform);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      kvWriteTransform.populateDisplayData(builder);
    }
  }

  private static class NullOnlyCoder<@Nullable T> extends AtomicCoder<T> {
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

  private static Class<?> resolveClass(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not find class: " + className);
    }
  }
}
