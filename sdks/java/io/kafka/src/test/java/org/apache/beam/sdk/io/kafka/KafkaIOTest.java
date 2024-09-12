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

import static org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProviderTest.mockDeserializerProvider;
import static org.apache.beam.sdk.metrics.MetricResultsMatchers.attemptedMetricsResult;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.io.kafka.KafkaIO.Read.FakeFlinkPipelineOptions;
import org.apache.beam.sdk.io.kafka.KafkaMocks.PositionErrorConsumerFactory;
import org.apache.beam.sdk.io.kafka.KafkaMocks.SendErrorProducerFactory;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.metrics.SourceMetrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.BadRecordErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.ErrorSinkTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableWithSize;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests of {@link KafkaIO}. Run with 'mvn test -Dkafka.clients.version=0.10.1.1', to test with a
 * specific Kafka version.
 */
@RunWith(JUnit4.class)
public class KafkaIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaIOTest.class);

  /*
   * The tests below borrow code and structure from CountingSourceTest. In addition verifies
   * the reader interleaves the records from multiple partitions.
   *
   * Other tests to consider :
   *   - test KafkaRecordCoder
   */

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule
  public ExpectedLogs unboundedReaderExpectedLogs = ExpectedLogs.none(KafkaUnboundedReader.class);

  @Rule public ExpectedLogs kafkaIOExpectedLogs = ExpectedLogs.none(KafkaIO.class);

  private static final Instant LOG_APPEND_START_TIME = new Instant(600 * 1000);
  private static final String TIMESTAMP_START_MILLIS_CONFIG = "test.timestamp.start.millis";
  private static final String TIMESTAMP_TYPE_CONFIG = "test.timestamp.type";
  static List<String> mkKafkaTopics = ImmutableList.of("topic_a", "topic_b");
  static String mkKafkaServers = "myServer1:9092,myServer2:9092";

  // Update mock consumer with records distributed among the given topics, each with given number
  // of partitions. Records are assigned in round-robin order among the partitions.
  private static MockConsumer<byte[], byte[]> mkMockConsumer(
      List<String> topics,
      int partitionsPerTopic,
      int numElements,
      OffsetResetStrategy offsetResetStrategy,
      Map<String, Object> config,
      SerializableFunction<Integer, byte[]> keyFunction,
      SerializableFunction<Integer, byte[]> valueFunction) {

    final List<TopicPartition> partitions = new ArrayList<>();
    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
    Map<String, List<PartitionInfo>> partitionMap = new HashMap<>();

    for (String topic : topics) {
      List<PartitionInfo> partIds = new ArrayList<>(partitionsPerTopic);
      for (int i = 0; i < partitionsPerTopic; i++) {
        TopicPartition tp = new TopicPartition(topic, i);
        partitions.add(tp);
        partIds.add(new PartitionInfo(topic, i, null, null, null));
        records.put(tp, new ArrayList<>());
      }
      partitionMap.put(topic, partIds);
    }

    int numPartitions = partitions.size();
    final long[] offsets = new long[numPartitions];

    long timestampStartMillis =
        (Long)
            config.getOrDefault(TIMESTAMP_START_MILLIS_CONFIG, LOG_APPEND_START_TIME.getMillis());
    TimestampType timestampType =
        TimestampType.forName(
            (String)
                config.getOrDefault(
                    TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.toString()));

    for (int i = 0; i < numElements; i++) {
      int pIdx = i % numPartitions;
      TopicPartition tp = partitions.get(pIdx);

      byte[] key = keyFunction.apply(i);
      byte[] value = valueFunction.apply(i);

      records
          .get(tp)
          .add(
              new ConsumerRecord<>(
                  tp.topic(),
                  tp.partition(),
                  offsets[pIdx]++,
                  timestampStartMillis + Duration.standardSeconds(i).getMillis(),
                  timestampType,
                  0,
                  key.length,
                  value.length,
                  key,
                  value));
    }

    // This is updated when reader assigns partitions.
    final AtomicReference<List<TopicPartition>> assignedPartitions =
        new AtomicReference<>(Collections.<TopicPartition>emptyList());

    final MockConsumer<byte[], byte[]> consumer =
        new MockConsumer<byte[], byte[]>(offsetResetStrategy) {
          @Override
          public synchronized void assign(final Collection<TopicPartition> assigned) {
            super.assign(assigned);
            assignedPartitions.set(ImmutableList.copyOf(assigned));
            for (TopicPartition tp : assigned) {
              updateBeginningOffsets(ImmutableMap.of(tp, 0L));
              updateEndOffsets(ImmutableMap.of(tp, (long) records.get(tp).size()));
            }
          }
          // Override offsetsForTimes() in order to look up the offsets by timestamp.
          @Override
          public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
              Map<TopicPartition, Long> timestampsToSearch) {
            return timestampsToSearch.entrySet().stream()
                .map(
                    e -> {
                      // In test scope, timestamp == offset.
                      long maxOffset = offsets[partitions.indexOf(e.getKey())];
                      long offset = e.getValue();
                      OffsetAndTimestamp value =
                          (offset >= maxOffset) ? null : new OffsetAndTimestamp(offset, offset);
                      return new SimpleEntry<>(e.getKey(), value);
                    })
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
          }
        };

    for (String topic : topics) {
      consumer.updatePartitions(topic, partitionMap.get(topic));
    }

    // MockConsumer does not maintain any relationship between partition seek position and the
    // records added. e.g. if we add 10 records to a partition and then seek to end of the
    // partition, MockConsumer is still going to return the 10 records in next poll. It is
    // our responsibility to make sure currently enqueued records sync with partition offsets.
    // The following task will be called inside each invocation to MockConsumer.poll().
    // We enqueue only the records with the offset >= partition's current position.
    Runnable recordEnqueueTask =
        new Runnable() {
          @Override
          public void run() {
            // add all the records with offset >= current partition position.
            int recordsAdded = 0;
            for (TopicPartition tp : assignedPartitions.get()) {
              long curPos = consumer.position(tp);
              for (ConsumerRecord<byte[], byte[]> r : records.get(tp)) {
                if (r.offset() >= curPos) {
                  consumer.addRecord(r);
                  recordsAdded++;
                }
              }
            }
            if (recordsAdded == 0) {
              if (config.get("inject.error.at.eof") != null) {
                consumer.setException(new KafkaException("Injected error in consumer.poll()"));
              }
              // MockConsumer.poll(timeout) does not actually wait even when there aren't any
              // records.
              // Add a small wait here in order to avoid busy looping in the reader.
              Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
              // TODO: BEAM-4086: testUnboundedSourceWithoutBoundedWrapper() occasionally hangs
              //     without this wait. Need to look into it.
            }
            consumer.schedulePollTask(this);
          }
        };

    consumer.schedulePollTask(recordEnqueueTask);
    return consumer;
  }

  private static class ConsumerFactoryFn
      implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {
    private final List<String> topics;
    private final int partitionsPerTopic;
    private final int numElements;
    private final OffsetResetStrategy offsetResetStrategy;
    private SerializableFunction<Integer, byte[]> keyFunction;
    private SerializableFunction<Integer, byte[]> valueFunction;

    ConsumerFactoryFn(
        List<String> topics,
        int partitionsPerTopic,
        int numElements,
        OffsetResetStrategy offsetResetStrategy) {
      this.topics = topics;
      this.partitionsPerTopic = partitionsPerTopic;
      this.numElements = numElements;
      this.offsetResetStrategy = offsetResetStrategy;
      keyFunction = i -> ByteBuffer.wrap(new byte[4]).putInt(i).array();
      valueFunction = i -> ByteBuffer.wrap(new byte[8]).putLong(i).array();
    }

    ConsumerFactoryFn(
        List<String> topics,
        int partitionsPerTopic,
        int numElements,
        OffsetResetStrategy offsetResetStrategy,
        SerializableFunction<Integer, byte[]> keyFunction,
        SerializableFunction<Integer, byte[]> valueFunction) {
      this.topics = topics;
      this.partitionsPerTopic = partitionsPerTopic;
      this.numElements = numElements;
      this.offsetResetStrategy = offsetResetStrategy;
      this.keyFunction = keyFunction;
      this.valueFunction = valueFunction;
    }

    @Override
    public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
      return mkMockConsumer(
          topics,
          partitionsPerTopic,
          numElements,
          offsetResetStrategy,
          config,
          keyFunction,
          valueFunction);
    }
  }

  static KafkaIO.Read<Integer, Long> mkKafkaReadTransform(
      int numElements, @Nullable SerializableFunction<KV<Integer, Long>, Instant> timestampFn) {
    return mkKafkaReadTransform(
        numElements,
        numElements,
        timestampFn,
        false, /*redistribute*/
        false, /*allowDuplicates*/
        0);
  }

  /**
   * Creates a consumer with two topics, with 10 partitions each. numElements are (round-robin)
   * assigned all the 20 partitions.
   */
  static KafkaIO.Read<Integer, Long> mkKafkaReadTransform(
      int numElements,
      @Nullable Integer maxNumRecords,
      @Nullable SerializableFunction<KV<Integer, Long>, Instant> timestampFn,
      @Nullable Boolean redistribute,
      @Nullable Boolean withAllowDuplicates,
      @Nullable Integer numKeys) {

    KafkaIO.Read<Integer, Long> reader =
        KafkaIO.<Integer, Long>read()
            .withBootstrapServers(mkKafkaServers)
            .withTopics(mkKafkaTopics)
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    mkKafkaTopics, 10, numElements, OffsetResetStrategy.EARLIEST)) // 20 partitions
            .withKeyDeserializer(IntegerDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);
    if (maxNumRecords != null) {
      reader = reader.withMaxNumRecords(maxNumRecords);
    }

    if (withAllowDuplicates == null) {
      withAllowDuplicates = false;
    }

    if (timestampFn != null) {
      reader = reader.withTimestampFn(timestampFn);
    }

    if (redistribute) {
      if (numKeys != null) {
        reader =
            reader
                .withRedistribute()
                .withAllowDuplicates(withAllowDuplicates)
                .withRedistributeNumKeys(numKeys);
      }
      reader = reader.withRedistribute();
    }
    return reader;
  }

  private static class AssertMultipleOf implements SerializableFunction<Iterable<Long>, Void> {
    private final int num;

    AssertMultipleOf(int num) {
      this.num = num;
    }

    @Override
    public Void apply(Iterable<Long> values) {
      for (Long v : values) {
        assertEquals(0, v % num);
      }
      return null;
    }
  }

  public static void addCountingAsserts(PCollection<Long> input, long numElements) {
    // Count == numElements
    // Unique count == numElements
    // Min == 0
    // Max == numElements-1
    addCountingAsserts(input, numElements, numElements, 0L, numElements - 1);
  }

  public static void addCountingAsserts(
      PCollection<Long> input, long count, long uniqueCount, long min, long max) {

    PAssert.thatSingleton(input.apply("Count", Count.globally())).isEqualTo(count);

    PAssert.thatSingleton(input.apply(Distinct.create()).apply("UniqueCount", Count.globally()))
        .isEqualTo(uniqueCount);

    PAssert.thatSingleton(input.apply("Min", Min.globally())).isEqualTo(min);

    PAssert.thatSingleton(input.apply("Max", Max.globally())).isEqualTo(max);
  }

  @Test
  public void testReadAvroGenericRecordsWithConfluentSchemaRegistry() {
    int numElements = 100;
    String topic = "my_topic";
    String schemaRegistryUrl = "mock://my-scope-name";
    String keySchemaSubject = topic + "-key";
    String valueSchemaSubject = topic + "-value";

    List<KV<GenericRecord, GenericRecord>> inputs = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      inputs.add(
          KV.of(
              new AvroGeneratedUser("KeyName" + i, i, "color" + i),
              new AvroGeneratedUser("ValueName" + i, i, "color" + i)));
    }

    KafkaIO.Read<GenericRecord, GenericRecord> reader =
        KafkaIO.<GenericRecord, GenericRecord>read()
            .withBootstrapServers("localhost:9092")
            .withTopic(topic)
            .withKeyDeserializer(
                mockDeserializerProvider(schemaRegistryUrl, keySchemaSubject, null))
            .withValueDeserializer(
                mockDeserializerProvider(schemaRegistryUrl, valueSchemaSubject, null))
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    ImmutableList.of(topic),
                    1,
                    numElements,
                    OffsetResetStrategy.EARLIEST,
                    new KeyAvroSerializableFunction(topic, schemaRegistryUrl),
                    new ValueAvroSerializableFunction(topic, schemaRegistryUrl)))
            .withMaxNumRecords(numElements);

    PCollection<KV<GenericRecord, GenericRecord>> input = p.apply(reader.withoutMetadata());

    PAssert.that(input).containsInAnyOrder(inputs);
    p.run();
  }

  @Test
  public void testReadAvroSpecificRecordsWithConfluentSchemaRegistry() {
    int numElements = 100;
    String topic = "my_topic";
    String schemaRegistryUrl = "mock://my-scope-name";
    String valueSchemaSubject = topic + "-value";

    List<KV<Integer, AvroGeneratedUser>> inputs = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      inputs.add(KV.of(i, new AvroGeneratedUser("ValueName" + i, i, "color" + i)));
    }

    KafkaIO.Read<Integer, AvroGeneratedUser> reader =
        KafkaIO.<Integer, AvroGeneratedUser>read()
            .withBootstrapServers("localhost:9092")
            .withTopic(topic)
            .withKeyDeserializer(IntegerDeserializer.class)
            .withValueDeserializer(
                mockDeserializerProvider(schemaRegistryUrl, valueSchemaSubject, null))
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    ImmutableList.of(topic),
                    1,
                    numElements,
                    OffsetResetStrategy.EARLIEST,
                    i -> ByteBuffer.wrap(new byte[4]).putInt(i).array(),
                    new ValueAvroSerializableFunction(topic, schemaRegistryUrl)))
            .withMaxNumRecords(numElements);

    PCollection<KV<Integer, AvroGeneratedUser>> input = p.apply(reader.withoutMetadata());

    PAssert.that(input).containsInAnyOrder(inputs);
    p.run();
  }

  public static class IntegerDeserializerWithHeadersAssertor extends IntegerDeserializer
      implements Deserializer<Integer> {

    @Override
    public Integer deserialize(String topic, byte[] data) {
      assertEquals(false, ConsumerSpEL.deserializerSupportsHeaders());
      return super.deserialize(topic, data);
    }

    @Override
    public Integer deserialize(String topic, Headers headers, byte[] data) {
      // Overriding the default should trigger header evaluation and this to be called.
      assertEquals(true, ConsumerSpEL.deserializerSupportsHeaders());
      return super.deserialize(topic, data);
    }
  }

  public static class LongDeserializerWithHeadersAssertor extends LongDeserializer
      implements Deserializer<Long> {

    @Override
    public Long deserialize(String topic, byte[] data) {
      assertEquals(false, ConsumerSpEL.deserializerSupportsHeaders());
      return super.deserialize(topic, data);
    }

    @Override
    public Long deserialize(String topic, Headers headers, byte[] data) {
      // Overriding the default should trigger header evaluation and this to be called.
      assertEquals(true, ConsumerSpEL.deserializerSupportsHeaders());
      return super.deserialize(topic, data);
    }
  }

  @Test
  public void testDeserializationWithHeaders() throws Exception {
    // To assert that we continue to prefer the Deserializer API with headers in Kafka API 2.1.0
    // onwards
    int numElements = 1000;
    String topic = "my_topic";

    KafkaIO.Read<Integer, Long> reader =
        KafkaIO.<Integer, Long>read()
            .withBootstrapServers("none")
            .withTopic("my_topic")
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    ImmutableList.of(topic), 10, numElements, OffsetResetStrategy.EARLIEST))
            .withMaxNumRecords(numElements)
            .withKeyDeserializerAndCoder(
                KafkaIOTest.IntegerDeserializerWithHeadersAssertor.class,
                BigEndianIntegerCoder.of())
            .withValueDeserializerAndCoder(
                KafkaIOTest.LongDeserializerWithHeadersAssertor.class, BigEndianLongCoder.of());

    PCollection<Long> input = p.apply(reader.withoutMetadata()).apply(Values.create());

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  public void testUnboundedSource() {
    int numElements = 1000;

    PCollection<Long> input =
        p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  public void testRiskyConfigurationWarnsProperly() {
    int numElements = 1000;

    p.getOptions().as(FakeFlinkPipelineOptions.class).setCheckpointingInterval(1L);

    PCollection<Long> input =
        p.apply(
                mkKafkaReadTransform(numElements, new ValueAsTimestampFn())
                    .withConsumerConfigUpdates(
                        ImmutableMap.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true))
                    .withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, numElements);

    kafkaIOExpectedLogs.verifyWarn(
        "When using the Flink runner with checkpointingInterval enabled");
    p.run();
  }

  @Test
  public void testCommitOffsetsInFinalizeAndRedistributeWarningsWithAllowDuplicates() {
    int numElements = 1000;

    PCollection<Long> input =
        p.apply(
                mkKafkaReadTransform(
                        numElements,
                        numElements,
                        new ValueAsTimestampFn(),
                        true, /*redistribute*/
                        true, /*allowDuplicates*/
                        0)
                    .commitOffsetsInFinalize()
                    .withConsumerConfigUpdates(
                        ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "group_id"))
                    .withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, numElements);
    p.run();

    kafkaIOExpectedLogs.verifyWarn(
        "Offsets committed due to usage of commitOffsetsInFinalize() may not capture all work processed due to use of withRedistribute()");
  }

  @Test
  public void testCommitOffsetsInFinalizeAndRedistributeNoWarningsWithAllowDuplicates() {
    int numElements = 1000;

    PCollection<Long> input =
        p.apply(
                mkKafkaReadTransform(
                        numElements,
                        numElements,
                        new ValueAsTimestampFn(),
                        true, /*redistribute*/
                        false, /*allowDuplicates*/
                        0)
                    .commitOffsetsInFinalize()
                    .withConsumerConfigUpdates(
                        ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "group_id"))
                    .withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  public void testNumKeysIgnoredWithRedistributeNotEnabled() {
    thrown.expect(Exception.class);
    thrown.expectMessage(
        "withRedistributeNumKeys is ignored if withRedistribute() is not enabled on the transform");

    int numElements = 1000;

    PCollection<Long> input =
        p.apply(
                mkKafkaReadTransform(
                        numElements,
                        numElements,
                        new ValueAsTimestampFn(),
                        false, /*redistribute*/
                        false, /*allowDuplicates*/
                        0)
                    .withRedistributeNumKeys(100)
                    .commitOffsetsInFinalize()
                    .withConsumerConfigUpdates(
                        ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "group_id"))
                    .withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, numElements);

    p.run();
  }

  @Test
  public void testDisableRedistributeKafkaOffsetLegacy() {
    thrown.expect(Exception.class);
    thrown.expectMessage(
        "Can not enable isRedistribute() while committing offsets prior to 2.60.0");
    p.getOptions().as(StreamingOptions.class).setUpdateCompatibilityVersion("2.59.0");

    p.apply(
            Create.of(
                KafkaSourceDescriptor.of(
                    new TopicPartition("topic", 1),
                    null,
                    null,
                    null,
                    null,
                    ImmutableList.of("8.8.8.8:9092"))))
        .apply(
            KafkaIO.<Long, Long>readSourceDescriptors()
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(LongDeserializer.class)
                .withRedistribute()
                .withProcessingTime()
                .commitOffsets());
    p.run();
  }

  @Test
  public void testUnreachableKafkaBrokers() {
    // Expect an exception when the Kafka brokers are not reachable on the workers.
    // We specify partitions explicitly so that splitting does not involve server interaction.
    // Set request timeout to 10ms so that test does not take long.

    thrown.expect(Exception.class);
    thrown.expectMessage("Reader-0: Timeout while initializing partition 'test-0'");

    int numElements = 1000;
    PCollection<Long> input =
        p.apply(
                KafkaIO.<Integer, Long>read()
                    .withBootstrapServers("8.8.8.8:9092") // Google public DNS ip.
                    .withTopicPartitions(ImmutableList.of(new TopicPartition("test", 0)))
                    .withKeyDeserializer(IntegerDeserializer.class)
                    .withValueDeserializer(LongDeserializer.class)
                    .withConsumerConfigUpdates(
                        ImmutableMap.of(
                            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
                            5,
                            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                            8,
                            ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
                            8,
                            "default.api.timeout.ms",
                            10))
                    .withMaxNumRecords(10)
                    .withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  public void testResolveDefaultApiTimeout() {

    final String defaultApiTimeoutConfig = "default.api.timeout.ms";

    assertEquals(
        Duration.millis(20),
        KafkaUnboundedReader.resolveDefaultApiTimeout(
            KafkaIO.<Integer, Long>read()
                .withConsumerConfigUpdates(
                    ImmutableMap.of(
                        defaultApiTimeoutConfig,
                        20,
                        ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                        30))));

    assertEquals(
        Duration.millis(2 * 30),
        KafkaUnboundedReader.resolveDefaultApiTimeout(
            KafkaIO.<Integer, Long>read()
                .withConsumerConfigUpdates(
                    ImmutableMap.of(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30))));

    assertEquals(
        Duration.millis(60 * 1000),
        KafkaUnboundedReader.resolveDefaultApiTimeout(KafkaIO.<Integer, Long>read()));
  }

  @Test
  public void testUnboundedSourceWithSingleTopic() {
    // same as testUnboundedSource, but with single topic

    int numElements = 1000;
    String topic = "my_topic";
    String bootStrapServer = "none";

    KafkaIO.Read<Integer, Long> reader =
        KafkaIO.<Integer, Long>read()
            .withBootstrapServers(bootStrapServer)
            .withTopic("my_topic")
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    ImmutableList.of(topic), 10, numElements, OffsetResetStrategy.EARLIEST))
            .withMaxNumRecords(numElements)
            .withKeyDeserializer(IntegerDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);

    PCollection<Long> input = p.apply(reader.withoutMetadata()).apply(Values.create());

    addCountingAsserts(input, numElements);
    PipelineResult result = p.run();
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SOURCE),
        hasItem(String.format("kafka:%s.%s", bootStrapServer, topic)));
  }

  @Test
  public void testUnboundedSourceWithExplicitPartitions() {
    int numElements = 1000;

    String topic = "test";
    List<String> topics = ImmutableList.of(topic);
    String bootStrapServer = "none";

    KafkaIO.Read<byte[], Long> reader =
        KafkaIO.<byte[], Long>read()
            .withBootstrapServers(bootStrapServer)
            .withTopicPartitions(ImmutableList.of(new TopicPartition(topic, 5)))
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    topics, 10, numElements, OffsetResetStrategy.EARLIEST)) // 10 partitions
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .withMaxNumRecords(numElements / 10);

    PCollection<Long> input = p.apply(reader.withoutMetadata()).apply(Values.create());

    // assert that every element is a multiple of 5.
    PAssert.that(input).satisfies(new AssertMultipleOf(5));

    PAssert.thatSingleton(input.apply(Count.globally())).isEqualTo(numElements / 10L);

    PipelineResult result = p.run();
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SOURCE),
        hasItem(String.format("kafka:%s.%s", bootStrapServer, topic)));
  }

  @Test
  public void testUnboundedSourceWithPattern() {
    int numElements = 1000;

    List<String> topics =
        ImmutableList.of(
            "best", "gest", "hest", "jest", "lest", "nest", "pest", "rest", "test", "vest", "west",
            "zest");
    String bootStrapServer = "none";

    KafkaIO.Read<byte[], Long> reader =
        KafkaIO.<byte[], Long>read()
            .withBootstrapServers("none")
            .withTopicPattern("[a-z]est")
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(topics, 10, numElements, OffsetResetStrategy.EARLIEST))
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .withMaxNumRecords(numElements);

    PCollection<Long> input = p.apply(reader.withoutMetadata()).apply(Values.create());

    addCountingAsserts(input, numElements);
    PipelineResult result = p.run();
    String[] expect =
        topics.stream()
            .map(topic -> String.format("kafka:%s.%s", bootStrapServer, topic))
            .toArray(String[]::new);
    assertThat(Lineage.query(result.metrics(), Lineage.Type.SOURCE), containsInAnyOrder(expect));
  }

  @Test
  public void testUnboundedSourceWithPartiallyMatchedPattern() {
    int numElements = 1000;
    long numMatchedElements = numElements / 2; // Expected elements if split across 2 topics

    List<String> topics = ImmutableList.of("test", "Test");
    String bootStrapServer = "none";

    KafkaIO.Read<byte[], Long> reader =
        KafkaIO.<byte[], Long>read()
            .withBootstrapServers(bootStrapServer)
            .withTopicPattern("[a-z]est")
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(topics, 1, numElements, OffsetResetStrategy.EARLIEST))
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .withMaxNumRecords(numMatchedElements);

    PCollection<Long> input = p.apply(reader.withoutMetadata()).apply(Values.create());

    // With 1 partition per topic element to partition allocation alternates between test and Test,
    // producing even elements for test and odd elements for Test.
    // The pattern only matches test, so we expect even elements.
    PAssert.that(input).satisfies(new AssertMultipleOf(2));

    PAssert.thatSingleton(input.apply("Count", Count.globally())).isEqualTo(numMatchedElements);

    PipelineResult result = p.run();
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SOURCE),
        hasItem(String.format("kafka:%s.test", bootStrapServer)));
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SOURCE),
        not(hasItem(String.format("kafka:%s.Test", bootStrapServer))));
  }

  @Test
  public void testUnboundedSourceWithUnmatchedPattern() {
    // Expect an exception when provided pattern doesn't match any Kafka topics.
    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(instanceOf(IllegalStateException.class));
    thrown.expectMessage(
        "Could not find any partitions. Please check Kafka configuration and topic names");

    int numElements = 1000;

    List<String> topics = ImmutableList.of("chest", "crest", "egest", "guest", "quest", "wrest");

    KafkaIO.Read<byte[], Long> reader =
        KafkaIO.<byte[], Long>read()
            .withBootstrapServers("none")
            .withTopicPattern("[a-z]est")
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(topics, 10, numElements, OffsetResetStrategy.EARLIEST))
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .withMaxNumRecords(numElements);

    p.apply(reader.withoutMetadata()).apply(Values.create());

    p.run();
  }

  @Test
  public void testUnboundedSourceWithWrongTopic() {
    // Expect an exception when provided Kafka topic doesn't exist.
    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(instanceOf(IllegalStateException.class));
    thrown.expectMessage(
        "Could not find any partitions info. Please check Kafka configuration and make sure that "
            + "provided topics exist.");

    int numElements = 1000;
    KafkaIO.Read<Integer, Long> reader =
        KafkaIO.<Integer, Long>read()
            .withBootstrapServers("none")
            .withTopic("wrong_topic") // read from topic that doesn't exist
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    ImmutableList.of("my_topic"), 10, numElements, OffsetResetStrategy.EARLIEST))
            .withMaxNumRecords(numElements)
            .withKeyDeserializer(IntegerDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);

    p.apply(reader.withoutMetadata()).apply(Values.create());
    p.run();
  }

  private static class ElementValueDiff extends DoFn<Long, Long> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element() - c.timestamp().getMillis());
    }
  }

  @Test
  public void testUnboundedSourceTimestamps() {

    int numElements = 1000;

    PCollection<Long> input =
        p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, numElements);

    PCollection<Long> diffs =
        input
            .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
            .apply("DistinctTimestamps", Distinct.create());
    // This assert also confirms that diffs only has one unique value.
    PAssert.thatSingleton(diffs).isEqualTo(0L);

    p.run();
  }

  @Test
  public void testUnboundedSourceLogAppendTimestamps() {
    // LogAppendTime (server side timestamp) for records is set based on record index
    // in MockConsumer above. Ensure that those exact timestamps are set by the source.
    int numElements = 1000;

    PCollection<Long> input =
        p.apply(mkKafkaReadTransform(numElements, null).withLogAppendTime().withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, numElements);

    PCollection<Long> diffs =
        input
            .apply(
                MapElements.into(TypeDescriptors.longs())
                    .via(t -> LOG_APPEND_START_TIME.plus(Duration.standardSeconds(t)).getMillis()))
            .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
            .apply("DistinctTimestamps", Distinct.create());

    // This assert also confirms that diff only has one unique value.
    PAssert.thatSingleton(diffs).isEqualTo(0L);

    p.run();
  }

  @Test
  public void testUnboundedSourceCustomTimestamps() {
    // The custom timestamps is set to customTimestampStartMillis + value.
    // Tests basic functionality of custom timestamps.

    final int numElements = 1000;
    final long customTimestampStartMillis = 80000L;

    PCollection<Long> input =
        p.apply(
                mkKafkaReadTransform(numElements, null)
                    .withTimestampPolicyFactory(
                        (tp, prevWatermark) ->
                            new CustomTimestampPolicyWithLimitedDelay<Integer, Long>(
                                record ->
                                    new Instant(
                                        TimeUnit.SECONDS.toMillis(record.getKV().getValue())
                                            + customTimestampStartMillis),
                                Duration.ZERO,
                                prevWatermark))
                    .withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, numElements);

    PCollection<Long> diffs =
        input
            .apply(
                MapElements.into(TypeDescriptors.longs())
                    .via(t -> TimeUnit.SECONDS.toMillis(t) + customTimestampStartMillis))
            .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
            .apply("DistinctTimestamps", Distinct.create());

    // This assert also confirms that diff only has one unique value.
    PAssert.thatSingleton(diffs).isEqualTo(0L);

    p.run();
  }

  @Test
  public void testUnboundedSourceCreateTimestamps() {
    // Same as testUnboundedSourceCustomTimestamps with create timestamp.

    final int numElements = 1000;
    final long createTimestampStartMillis = 50000L;

    PCollection<Long> input =
        p.apply(
                mkKafkaReadTransform(numElements, null)
                    .withCreateTime(Duration.ZERO)
                    .withConsumerConfigUpdates(
                        ImmutableMap.of(
                            TIMESTAMP_TYPE_CONFIG,
                            "CreateTime",
                            TIMESTAMP_START_MILLIS_CONFIG,
                            createTimestampStartMillis))
                    .withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, numElements);

    PCollection<Long> diffs =
        input
            .apply(
                MapElements.into(TypeDescriptors.longs())
                    .via(t -> TimeUnit.SECONDS.toMillis(t) + createTimestampStartMillis))
            .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
            .apply("DistinctTimestamps", Distinct.create());

    // This assert also confirms that diff only has one unique value.
    PAssert.thatSingleton(diffs).isEqualTo(0L);

    p.run();
  }

  // Returns TIMESTAMP_MAX_VALUE for watermark when all the records are read from a partition.
  static class TimestampPolicyWithEndOfSource<K, V> implements TimestampPolicyFactory<K, V> {
    private final long maxOffset;

    TimestampPolicyWithEndOfSource(long maxOffset) {
      this.maxOffset = maxOffset;
    }

    @Override
    public TimestampPolicy<K, V> createTimestampPolicy(
        TopicPartition tp, Optional<Instant> previousWatermark) {
      return new TimestampPolicy<K, V>() {
        long lastOffset = 0;
        Instant lastTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;

        @Override
        public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<K, V> record) {
          lastOffset = record.getOffset();
          lastTimestamp = new Instant(record.getTimestamp());
          return lastTimestamp;
        }

        @Override
        public Instant getWatermark(PartitionContext ctx) {
          if (lastOffset < maxOffset) {
            return lastTimestamp;
          } else { // EOF
            return BoundedWindow.TIMESTAMP_MAX_VALUE;
          }
        }
      };
    }
  }

  @Test
  public void testUnboundedSourceWithExceptionInKafkaFetch() {
    // Similar testUnboundedSource, but with an injected exception inside Kafk Consumer poll.

    // The reader should throw an IOException:
    thrown.expectCause(isA(IOException.class));
    thrown.expectCause(hasMessage(containsString("Exception while reading from Kafka")));
    // The original exception is from MockConsumer.poll():
    thrown.expectCause(hasCause(isA(KafkaException.class)));
    thrown.expectCause(hasCause(hasMessage(containsString("Injected error in consumer.poll()"))));

    int numElements = 1000;
    String topic = "my_topic";

    KafkaIO.Read<Integer, Long> reader =
        KafkaIO.<Integer, Long>read()
            .withBootstrapServers("none")
            .withTopic("my_topic")
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    ImmutableList.of(topic), 10, numElements, OffsetResetStrategy.EARLIEST))
            .withMaxNumRecords(2 * numElements) // Try to read more messages than available.
            .withConsumerConfigUpdates(ImmutableMap.of("inject.error.at.eof", true))
            .withKeyDeserializer(IntegerDeserializer.class)
            .withValueDeserializer(LongDeserializer.class);

    PCollection<Long> input = p.apply(reader.withoutMetadata()).apply(Values.create());

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  @Ignore // TODO : BEAM-4086 : enable once flakiness is fixed.
  public void testUnboundedSourceWithoutBoundedWrapper() {
    // This is same as testUnboundedSource() without the BoundedSource wrapper.
    // Most of the tests in this file set 'maxNumRecords' on the source, which wraps
    // the unbounded source in a bounded source. As a result, the test pipeline run as
    // bounded/batch pipelines under direct-runner.
    // This tests runs without such a wrapper over unbounded wrapper, and depends on watermark
    // progressing to infinity to end the test (see TimestampPolicyWithEndOfSource above).

    final int numElements = 1000;
    final int numPartitions = 10;
    String topic = "testUnboundedSourceWithoutBoundedWrapper";

    KafkaIO.Read<byte[], Long> reader =
        KafkaIO.<byte[], Long>read()
            .withBootstrapServers(topic)
            .withTopic(topic)
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    ImmutableList.of(topic),
                    numPartitions,
                    numElements,
                    OffsetResetStrategy.EARLIEST))
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .withTimestampPolicyFactory(
                new TimestampPolicyWithEndOfSource<>(numElements / numPartitions - 1));

    p.apply("readFromKafka", reader.withoutMetadata())
        .apply(Values.create())
        .apply(Window.into(FixedWindows.of(Duration.standardDays(100))));

    PipelineResult result = p.run();

    MetricName elementsRead = SourceMetrics.elementsRead().getName();

    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.inNamespace(elementsRead.getNamespace()))
                    .build());

    assertThat(
        metrics.getCounters(),
        hasItem(
            attemptedMetricsResult(
                elementsRead.getNamespace(),
                elementsRead.getName(),
                "readFromKafka",
                (long) numElements)));
  }

  private static class RemoveKafkaMetadata<K, V> extends DoFn<KafkaRecord<K, V>, KV<K, V>> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element().getKV());
    }
  }

  @Test
  public void testUnboundedSourceSplits() throws Exception {

    int numElements = 1000;
    int numSplits = 10;

    // Coders must be specified explicitly here due to the way the transform
    // is used in the test.
    UnboundedSource<KafkaRecord<Integer, Long>, ?> initial =
        mkKafkaReadTransform(numElements, null)
            .withKeyDeserializerAndCoder(IntegerDeserializer.class, BigEndianIntegerCoder.of())
            .withValueDeserializerAndCoder(LongDeserializer.class, BigEndianLongCoder.of())
            .makeSource();

    List<? extends UnboundedSource<KafkaRecord<Integer, Long>, ?>> splits =
        initial.split(numSplits, p.getOptions());
    assertEquals("Expected exact splitting", numSplits, splits.size());

    long elementsPerSplit = numElements / numSplits;
    assertEquals("Expected even splits", numElements, elementsPerSplit * numSplits);
    PCollectionList<Long> pcollections = PCollectionList.empty(p);
    for (int i = 0; i < splits.size(); ++i) {
      pcollections =
          pcollections.and(
              p.apply("split" + i, Read.from(splits.get(i)).withMaxNumRecords(elementsPerSplit))
                  .apply("Remove Metadata " + i, ParDo.of(new RemoveKafkaMetadata<>()))
                  .apply("collection " + i, Values.create()));
    }
    PCollection<Long> input = pcollections.apply(Flatten.pCollections());

    addCountingAsserts(input, numElements);
    p.run();
  }

  /** A timestamp function that uses the given value as the timestamp. */
  static class ValueAsTimestampFn implements SerializableFunction<KV<Integer, Long>, Instant> {
    @Override
    public Instant apply(KV<Integer, Long> input) {
      return new Instant(input.getValue());
    }
  }

  // Kafka records are read in a separate thread inside the reader. As a result advance() might not
  // read any records even from the mock consumer, especially for the first record.
  // This is a helper method to loop until we read a record.
  private static void advanceOnce(UnboundedReader<?> reader, boolean isStarted) throws IOException {
    if (!isStarted && reader.start()) {
      return;
    }
    while (!reader.advance()) {
      // very rarely will there be more than one attempts.
      // In case of a bug we might end up looping forever, and test will fail with a timeout.

      // Avoid hard cpu spinning in case of a test failure.
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testUnboundedSourceCheckpointMark() throws Exception {
    int numElements = 85; // 85 to make sure some partitions have more records than other.

    // create a single split:
    UnboundedSource<KafkaRecord<Integer, Long>, KafkaCheckpointMark> source =
        mkKafkaReadTransform(numElements, new ValueAsTimestampFn())
            .makeSource()
            .split(1, PipelineOptionsFactory.create())
            .get(0);

    UnboundedReader<KafkaRecord<Integer, Long>> reader = source.createReader(null, null);
    final int numToSkip = 20; // one from each partition.

    // advance numToSkip elements
    for (int i = 0; i < numToSkip; ++i) {
      advanceOnce(reader, i > 0);
    }

    // Confirm that we get the expected element in sequence before checkpointing.
    assertEquals(numToSkip - 1, (long) reader.getCurrent().getKV().getValue());
    assertEquals(numToSkip - 1, reader.getCurrentTimestamp().getMillis());

    // Checkpoint and restart, and confirm that the source continues correctly.
    KafkaCheckpointMark mark =
        CoderUtils.clone(
            source.getCheckpointMarkCoder(), (KafkaCheckpointMark) reader.getCheckpointMark());
    reader = source.createReader(null, mark);

    // Confirm that we get the next elements in sequence.
    // This also confirms that Reader interleaves records from each partitions by the reader.

    for (int i = numToSkip; i < numElements; i++) {
      advanceOnce(reader, i > numToSkip);
      assertEquals(i, (long) reader.getCurrent().getKV().getValue());
      assertEquals(i, reader.getCurrentTimestamp().getMillis());
    }
  }

  @Test
  public void testUnboundedSourceCheckpointMarkWithEmptyPartitions() throws Exception {
    // Similar to testUnboundedSourceCheckpointMark(), but verifies that source resumes
    // properly from empty partitions, without missing messages added since checkpoint.

    // Initialize consumer with fewer elements than number of partitions so that some are empty.
    int initialNumElements = 5;
    UnboundedSource<KafkaRecord<Integer, Long>, KafkaCheckpointMark> source =
        mkKafkaReadTransform(initialNumElements, new ValueAsTimestampFn())
            .makeSource()
            .split(1, PipelineOptionsFactory.create())
            .get(0);

    UnboundedReader<KafkaRecord<Integer, Long>> reader = source.createReader(null, null);

    for (int l = 0; l < initialNumElements; ++l) {
      advanceOnce(reader, l > 0);
    }

    // Checkpoint and restart, and confirm that the source continues correctly.
    KafkaCheckpointMark mark =
        CoderUtils.clone(
            source.getCheckpointMarkCoder(), (KafkaCheckpointMark) reader.getCheckpointMark());

    // Create another source with MockConsumer with OffsetResetStrategy.LATEST. This insures that
    // the reader need to explicitly need to seek to first offset for partitions that were empty.

    int numElements = 100; // all the 20 partitions will have elements
    List<String> topics = ImmutableList.of("topic_a", "topic_b");

    source =
        KafkaIO.<Integer, Long>read()
            .withBootstrapServers("none")
            .withTopics(topics)
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(topics, 10, numElements, OffsetResetStrategy.LATEST))
            .withKeyDeserializer(IntegerDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .withMaxNumRecords(numElements)
            .withTimestampFn(new ValueAsTimestampFn())
            .makeSource()
            .split(1, PipelineOptionsFactory.create())
            .get(0);

    reader = source.createReader(null, mark);

    // Verify in any order. As the partitions are unevenly read, the returned records are not in a
    // simple order. Note that testUnboundedSourceCheckpointMark() verifies round-robin oder.

    List<Long> expected = new ArrayList<>();
    List<Long> actual = new ArrayList<>();
    for (long i = initialNumElements; i < numElements; i++) {
      advanceOnce(reader, i > initialNumElements);
      expected.add(i);
      actual.add(reader.getCurrent().getKV().getValue());
    }
    assertThat(actual, IsIterableContainingInAnyOrder.containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testUnboundedSourceMetrics() {
    int numElements = 1000;

    String readStep = "readFromKafka";

    p.apply(
        readStep,
        mkKafkaReadTransform(numElements, new ValueAsTimestampFn())
            .withConsumerConfigUpdates(
                ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, "test.group"))
            .commitOffsetsInFinalize()
            .withoutMetadata());

    PipelineResult result = p.run();

    String splitId = "0";

    MetricName elementsRead = SourceMetrics.elementsRead().getName();
    MetricName elementsReadBySplit = SourceMetrics.elementsReadBySplit(splitId).getName();
    MetricName bytesRead = SourceMetrics.bytesRead().getName();
    MetricName bytesReadBySplit = SourceMetrics.bytesReadBySplit(splitId).getName();
    MetricName backlogElementsOfSplit = SourceMetrics.backlogElementsOfSplit(splitId).getName();
    MetricName backlogBytesOfSplit = SourceMetrics.backlogBytesOfSplit(splitId).getName();

    MetricQueryResults metrics = result.metrics().allMetrics();

    Iterable<MetricResult<Long>> counters = metrics.getCounters();

    assertThat(
        counters,
        hasItem(
            attemptedMetricsResult(
                elementsRead.getNamespace(), elementsRead.getName(), readStep, 1000L)));

    assertThat(
        counters,
        hasItem(
            attemptedMetricsResult(
                elementsReadBySplit.getNamespace(),
                elementsReadBySplit.getName(),
                readStep,
                1000L)));

    assertThat(
        counters,
        hasItem(
            attemptedMetricsResult(
                bytesRead.getNamespace(), bytesRead.getName(), readStep, 12000L)));

    assertThat(
        counters,
        hasItem(
            attemptedMetricsResult(
                bytesReadBySplit.getNamespace(), bytesReadBySplit.getName(), readStep, 12000L)));

    MetricQueryResults backlogElementsMetrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            backlogElementsOfSplit.getNamespace(),
                            backlogElementsOfSplit.getName()))
                    .build());

    // since gauge values may be inconsistent in some environments assert only on their existence.
    assertThat(backlogElementsMetrics.getGauges(), IsIterableWithSize.iterableWithSize(1));

    MetricQueryResults backlogBytesMetrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            backlogBytesOfSplit.getNamespace(), backlogBytesOfSplit.getName()))
                    .build());

    // since gauge values may be inconsistent in some environments assert only on their existence.
    assertThat(backlogBytesMetrics.getGauges(), IsIterableWithSize.iterableWithSize(1));

    // Check checkpointMarkCommitsEnqueued metric.
    MetricQueryResults commitsEnqueuedMetrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            KafkaUnboundedReader.METRIC_NAMESPACE,
                            KafkaUnboundedReader.CHECKPOINT_MARK_COMMITS_ENQUEUED_METRIC))
                    .build());

    assertThat(commitsEnqueuedMetrics.getCounters(), IsIterableWithSize.iterableWithSize(1));
    assertThat(
        commitsEnqueuedMetrics.getCounters().iterator().next().getAttempted(), greaterThan(0L));
  }

  @Test
  public void testUnboundedReaderLogsCommitFailure() throws Exception {

    List<String> topics = ImmutableList.of("topic_a");

    PositionErrorConsumerFactory positionErrorConsumerFactory = new PositionErrorConsumerFactory();

    UnboundedSource<KafkaRecord<Integer, Long>, KafkaCheckpointMark> source =
        KafkaIO.<Integer, Long>read()
            .withBootstrapServers("myServer1:9092,myServer2:9092")
            .withTopics(topics)
            .withConsumerFactoryFn(positionErrorConsumerFactory)
            .withKeyDeserializer(IntegerDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .makeSource();

    UnboundedReader<KafkaRecord<Integer, Long>> reader = source.createReader(null, null);

    reader.start();

    unboundedReaderExpectedLogs.verifyWarn("exception while fetching latest offset for partition");

    reader.close();
  }

  @Test
  public void testSink() throws Exception {
    // Simply read from kafka source and write to kafka sink. Then verify the records
    // are correctly published to mock kafka producer.

    int numElements = 1000;

    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {

      ProducerSendCompletionThread completionThread =
          new ProducerSendCompletionThread(producerWrapper.mockProducer).start();

      String topic = "test";
      String bootStrapServer = "none";

      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(
              KafkaIO.<Integer, Long>write()
                  .withBootstrapServers(bootStrapServer)
                  .withTopic(topic)
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(LongSerializer.class)
                  .withInputTimestamp()
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey)));

      PipelineResult result = p.run();

      completionThread.shutdown();

      verifyProducerRecords(producerWrapper.mockProducer, topic, numElements, false, true);
      assertThat(
          Lineage.query(result.metrics(), Lineage.Type.SINK),
          hasItem(String.format("kafka:%s.%s", bootStrapServer, topic)));
    }
  }

  public static class FailingLongSerializer implements Serializer<Long> {
    // enables instantiation by registrys
    public FailingLongSerializer() {}

    @Override
    public byte[] serialize(String topic, Long data) {
      throw new SerializationException("ExpectedSerializationException");
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      // intentionally left blank for compatibility with older kafka versions
    }
  }

  @Test
  public void testSinkWithSerializationErrors() throws Exception {
    // Attempt to write 10 elements to Kafka, but they will all fail to serialize, and be sent to
    // the DLQ

    int numElements = 10;

    try (MockProducerWrapper producerWrapper =
        new MockProducerWrapper(new FailingLongSerializer())) {

      ProducerSendCompletionThread completionThread =
          new ProducerSendCompletionThread(producerWrapper.mockProducer).start();

      String topic = "test";

      BadRecordErrorHandler<PCollection<Long>> eh =
          p.registerBadRecordErrorHandler(new ErrorSinkTransform());

      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(
              KafkaIO.<Integer, Long>write()
                  .withBootstrapServers("none")
                  .withTopic(topic)
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(FailingLongSerializer.class)
                  .withInputTimestamp()
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey))
                  .withBadRecordErrorHandler(eh));

      eh.close();

      PAssert.thatSingleton(Objects.requireNonNull(eh.getOutput())).isEqualTo(10L);

      p.run();

      completionThread.shutdown();

      verifyProducerRecords(producerWrapper.mockProducer, topic, 0, false, true);
    }
  }

  @Test
  public void testValuesSink() throws Exception {
    // similar to testSink(), but use values()' interface.

    int numElements = 1000;

    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {

      ProducerSendCompletionThread completionThread =
          new ProducerSendCompletionThread(producerWrapper.mockProducer).start();

      String topic = "test";

      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(Values.create()) // there are no keys
          .apply(
              KafkaIO.<Integer, Long>write()
                  .withBootstrapServers("none")
                  .withTopic(topic)
                  .withValueSerializer(LongSerializer.class)
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey))
                  .values());

      p.run();

      completionThread.shutdown();

      verifyProducerRecords(producerWrapper.mockProducer, topic, numElements, true, false);
    }
  }

  @Test
  public void testRecordsSink() throws Exception {
    // Simply read from kafka source and write to kafka sink using ProducerRecord transform. Then
    // verify the records are correctly published to mock kafka producer.

    int numElements = 1000;

    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {

      ProducerSendCompletionThread completionThread =
          new ProducerSendCompletionThread(producerWrapper.mockProducer).start();

      String topic = "test";

      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(ParDo.of(new KV2ProducerRecord(topic)))
          .setCoder(ProducerRecordCoder.of(VarIntCoder.of(), VarLongCoder.of()))
          .apply(
              KafkaIO.<Integer, Long>writeRecords()
                  .withBootstrapServers("none")
                  .withTopic(topic)
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(LongSerializer.class)
                  .withInputTimestamp()
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey)));

      p.run();

      completionThread.shutdown();

      verifyProducerRecords(producerWrapper.mockProducer, topic, numElements, false, true);
    }
  }

  @Test
  public void testSinkToMultipleTopics() throws Exception {
    // Set different output topic names
    int numElements = 1000;

    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {

      ProducerSendCompletionThread completionThread =
          new ProducerSendCompletionThread(producerWrapper.mockProducer).start();

      String defaultTopic = "test";

      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(ParDo.of(new KV2ProducerRecord(defaultTopic, false)))
          .setCoder(ProducerRecordCoder.of(VarIntCoder.of(), VarLongCoder.of()))
          .apply(
              KafkaIO.<Integer, Long>writeRecords()
                  .withBootstrapServers("none")
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(LongSerializer.class)
                  .withInputTimestamp()
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey)));

      p.run();

      completionThread.shutdown();

      // Verify that appropriate messages are written to different Kafka topics
      List<ProducerRecord<Integer, Long>> sent = producerWrapper.mockProducer.history();

      for (int i = 0; i < numElements; i++) {
        ProducerRecord<Integer, Long> record = sent.get(i);
        if (i % 2 == 0) {
          assertEquals("test_2", record.topic());
        } else {
          assertEquals("test_1", record.topic());
        }
        assertEquals(i, record.key().intValue());
        assertEquals(i, record.value().longValue());
        assertEquals(i, record.timestamp().intValue());
        assertEquals(0, record.headers().toArray().length);
      }
    }
  }

  @Test
  public void testKafkaWriteHeaders() throws Exception {
    // Set different output topic names
    int numElements = 1;
    SimpleEntry<String, String> header = new SimpleEntry<>("header_key", "header_value");
    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {

      ProducerSendCompletionThread completionThread =
          new ProducerSendCompletionThread(producerWrapper.mockProducer).start();

      String defaultTopic = "test";
      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(
              ParDo.of(
                  new KV2ProducerRecord(defaultTopic, true, System.currentTimeMillis(), header)))
          .setCoder(ProducerRecordCoder.of(VarIntCoder.of(), VarLongCoder.of()))
          .apply(
              KafkaIO.<Integer, Long>writeRecords()
                  .withBootstrapServers("none")
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(LongSerializer.class)
                  .withInputTimestamp()
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey)));

      p.run();

      completionThread.shutdown();

      // Verify that appropriate header is written with producer record
      List<ProducerRecord<Integer, Long>> sent = producerWrapper.mockProducer.history();

      for (int i = 0; i < numElements; i++) {
        ProducerRecord<Integer, Long> record = sent.get(i);
        Headers headers = record.headers();
        assertNotNull(headers);
        Header[] headersArray = headers.toArray();
        assertEquals(1, headersArray.length);
        assertEquals(header.getKey(), headersArray[0].key());
        assertEquals(
            header.getValue(), new String(headersArray[0].value(), StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  public void testSinkProducerRecordsWithCustomTS() throws Exception {
    int numElements = 1000;

    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {

      ProducerSendCompletionThread completionThread =
          new ProducerSendCompletionThread(producerWrapper.mockProducer).start();

      final String defaultTopic = "test";
      final Long ts = System.currentTimeMillis();

      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(ParDo.of(new KV2ProducerRecord(defaultTopic, ts)))
          .setCoder(ProducerRecordCoder.of(VarIntCoder.of(), VarLongCoder.of()))
          .apply(
              KafkaIO.<Integer, Long>writeRecords()
                  .withBootstrapServers("none")
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(LongSerializer.class)
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey)));

      p.run();

      completionThread.shutdown();

      // Verify that messages are written with user-defined timestamp
      List<ProducerRecord<Integer, Long>> sent = producerWrapper.mockProducer.history();

      for (int i = 0; i < numElements; i++) {
        ProducerRecord<Integer, Long> record = sent.get(i);
        assertEquals(defaultTopic, record.topic());
        assertEquals(i, record.key().intValue());
        assertEquals(i, record.value().longValue());
        assertEquals(ts, record.timestamp());
      }
    }
  }

  @Test
  public void testSinkProducerRecordsWithCustomPartition() throws Exception {
    int numElements = 1000;

    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {

      ProducerSendCompletionThread completionThread =
          new ProducerSendCompletionThread(producerWrapper.mockProducer).start();

      final String defaultTopic = "test";
      final Integer partition = 1;

      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(ParDo.of(new KV2ProducerRecord(defaultTopic, partition)))
          .setCoder(ProducerRecordCoder.of(VarIntCoder.of(), VarLongCoder.of()))
          .apply(
              KafkaIO.<Integer, Long>writeRecords()
                  .withBootstrapServers("none")
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(LongSerializer.class)
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey)));

      p.run();

      completionThread.shutdown();

      // Verify that messages are written with user-defined timestamp
      List<ProducerRecord<Integer, Long>> sent = producerWrapper.mockProducer.history();

      for (int i = 0; i < numElements; i++) {
        ProducerRecord<Integer, Long> record = sent.get(i);
        assertEquals(defaultTopic, record.topic());
        assertEquals(partition, record.partition());
        assertEquals(i, record.key().intValue());
        assertEquals(i, record.value().longValue());
      }
    }
  }

  private static class KV2ProducerRecord
      extends DoFn<KV<Integer, Long>, ProducerRecord<Integer, Long>> {
    final String topic;
    final Integer partition;
    final boolean isSingleTopic;
    final Long ts;
    final SimpleEntry<String, String> header;

    KV2ProducerRecord(String topic) {
      this(topic, true);
    }

    KV2ProducerRecord(String topic, Integer partition) {
      this(topic, true, null, null, partition);
    }

    KV2ProducerRecord(String topic, Long ts) {
      this(topic, true, ts);
    }

    KV2ProducerRecord(String topic, boolean isSingleTopic) {
      this(topic, isSingleTopic, null);
    }

    KV2ProducerRecord(String topic, boolean isSingleTopic, Long ts) {
      this(topic, isSingleTopic, ts, null, null);
    }

    KV2ProducerRecord(
        String topic, boolean isSingleTopic, Long ts, SimpleEntry<String, String> header) {
      this(topic, isSingleTopic, ts, header, null);
    }

    KV2ProducerRecord(
        String topic,
        boolean isSingleTopic,
        Long ts,
        SimpleEntry<String, String> header,
        Integer partition) {
      this.topic = topic;
      this.partition = partition;
      this.isSingleTopic = isSingleTopic;
      this.ts = ts;
      this.header = header;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      KV<Integer, Long> kv = ctx.element();
      List<Header> headers = null;
      if (header != null) {
        headers =
            Arrays.asList(
                new RecordHeader(
                    header.getKey(), header.getValue().getBytes(StandardCharsets.UTF_8)));
      }
      if (isSingleTopic) {
        ctx.output(new ProducerRecord<>(topic, partition, ts, kv.getKey(), kv.getValue(), headers));
      } else {
        if (kv.getKey() % 2 == 0) {
          ctx.output(
              new ProducerRecord<>(
                  topic + "_2", partition, ts, kv.getKey(), kv.getValue(), headers));
        } else {
          ctx.output(
              new ProducerRecord<>(
                  topic + "_1", partition, ts, kv.getKey(), kv.getValue(), headers));
        }
      }
    }
  }

  @Test
  public void testExactlyOnceSink() {
    // testSink() with EOS enabled.
    // This does not actually inject retries in a stage to test exactly-once-semantics.
    // It mainly exercises the code in normal flow without retries.
    // Ideally we should test EOS Sink by triggering replays of a messages between stages.
    // It is not feasible to test such retries with direct runner. When DoFnTester supports
    // state, we can test ExactlyOnceWriter DoFn directly to ensure it handles retries correctly.

    if (!ProducerSpEL.supportsTransactions()) {
      LOG.warn(
          "testExactlyOnceSink() is disabled as Kafka client version does not support transactions.");
      return;
    }

    int numElements = 1000;

    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {

      ProducerSendCompletionThread completionThread =
          new ProducerSendCompletionThread(producerWrapper.mockProducer).start();

      String topic = "test-eos";
      String bootStrapServer = "none";

      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(
              KafkaIO.<Integer, Long>write()
                  .withBootstrapServers(bootStrapServer)
                  .withTopic(topic)
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(LongSerializer.class)
                  .withEOS(1, "test-eos")
                  .withConsumerFactoryFn(
                      new ConsumerFactoryFn(
                          Lists.newArrayList(topic), 10, 10, OffsetResetStrategy.EARLIEST))
                  .withPublishTimestampFunction((e, ts) -> ts)
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey)));

      PipelineResult result = p.run();

      completionThread.shutdown();

      verifyProducerRecords(producerWrapper.mockProducer, topic, numElements, false, true);
      assertThat(
          Lineage.query(result.metrics(), Lineage.Type.SINK),
          hasItem(String.format("kafka:%s.%s", bootStrapServer, topic)));
    }
  }

  @Test
  public void testExactlyOnceSinkWithSendException() throws Throwable {

    if (!ProducerSpEL.supportsTransactions()) {
      LOG.warn(
          "testExactlyOnceSink() is disabled as Kafka client version does not support transactions.");
      return;
    }

    thrown.expect(KafkaException.class);
    thrown.expectMessage("fakeException");

    String topic = "test";

    p.apply(Create.of(ImmutableList.of(KV.of(1, 1L), KV.of(2, 2L))))
        .apply(
            KafkaIO.<Integer, Long>write()
                .withBootstrapServers("none")
                .withTopic(topic)
                .withKeySerializer(IntegerSerializer.class)
                .withValueSerializer(LongSerializer.class)
                .withEOS(1, "testException")
                .withConsumerFactoryFn(
                    new ConsumerFactoryFn(
                        Lists.newArrayList(topic), 10, 10, OffsetResetStrategy.EARLIEST))
                .withProducerFactoryFn(new SendErrorProducerFactory()));

    try {
      p.run();
    } catch (PipelineExecutionException e) {
      // throwing inner exception helps assert that first exception is thrown from the Sink
      throw e.getCause();
    }
  }

  @Test
  public void testSinkWithSendErrors() throws Throwable {
    // similar to testSink(), except that up to 10 of the send calls to producer will fail
    // asynchronously.

    // TODO: Ideally we want the pipeline to run to completion by retrying bundles that fail.
    // We limit the number of errors injected to 10 below. This would reflect a real streaming
    // pipeline. But I am sure how to achieve that. For now expect an exception:

    thrown.expect(InjectedErrorException.class);
    thrown.expectMessage("Injected Error #1");

    int numElements = 1000;

    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {

      ProducerSendCompletionThread completionThreadWithErrors =
          new ProducerSendCompletionThread(producerWrapper.mockProducer, 10, 100).start();

      String topic = "test";

      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(
              KafkaIO.<Integer, Long>write()
                  .withBootstrapServers("none")
                  .withTopic(topic)
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(LongSerializer.class)
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey)));

      try {
        p.run();
      } catch (PipelineExecutionException e) {
        // throwing inner exception helps assert that first exception is thrown from the Sink
        throw e.getCause().getCause();
      } finally {
        completionThreadWithErrors.shutdown();
      }
    }
  }

  @Test
  public void testUnboundedSourceStartReadTime() {

    assumeTrue(ConsumerSpEL.hasOffsetsForTimes());

    int numElements = 1000;
    // In this MockConsumer, we let the elements of the time and offset equal and there are 20
    // partitions. So set this startTime can read half elements.
    int startTime = numElements / 20 / 2;
    int maxNumRecords = numElements / 2;

    PCollection<Long> input =
        p.apply(
                mkKafkaReadTransform(
                        numElements,
                        maxNumRecords,
                        new ValueAsTimestampFn(),
                        false, /*redistribute*/
                        false, /*allowDuplicates*/
                        0)
                    .withStartReadTime(new Instant(startTime))
                    .withoutMetadata())
            .apply(Values.create());

    addCountingAsserts(input, maxNumRecords, maxNumRecords, maxNumRecords, numElements - 1);
    p.run();
  }

  @Rule public ExpectedException noMessagesException = ExpectedException.none();

  @Test
  public void testUnboundedSourceStartReadTimeException() {

    assumeTrue(ConsumerSpEL.hasOffsetsForTimes());

    noMessagesException.expect(RuntimeException.class);

    int numElements = 1000;
    // In this MockConsumer, we let the elements of the time and offset equal and there are 20
    // partitions. So set this startTime can not read any element.
    int startTime = numElements / 20;

    p.apply(
            mkKafkaReadTransform(
                    numElements,
                    numElements,
                    new ValueAsTimestampFn(),
                    false, /*redistribute*/
                    false, /*allowDuplicates*/
                    0)
                .withStartReadTime(new Instant(startTime))
                .withoutMetadata())
        .apply(Values.create());

    p.run();
  }

  @Test
  public void testUnboundedSourceRawSizeMetric() {
    final String readStep = "readFromKafka";
    final int numElements = 1000;
    final int numPartitionsPerTopic = 10;
    final int recordSize = 12; // The size of key and value is defined in ConsumerFactoryFn.

    List<String> topics = ImmutableList.of("test");

    KafkaIO.Read<byte[], Long> reader =
        KafkaIO.<byte[], Long>read()
            .withBootstrapServers("none")
            .withTopicPartitions(
                ImmutableList.of(new TopicPartition("test", 5), new TopicPartition("test", 8)))
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    topics, numPartitionsPerTopic, numElements, OffsetResetStrategy.EARLIEST))
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(LongDeserializer.class)
            .withMaxNumRecords(numElements / numPartitionsPerTopic * 2); // 2 is the # of partitions

    p.apply(readStep, reader.withoutMetadata()).apply(Values.create());

    PipelineResult result = p.run();

    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.inNamespace(KafkaUnboundedReader.METRIC_NAMESPACE))
                    .build());

    assertThat(
        metrics.getDistributions(),
        hasItem(
            attemptedMetricsResult(
                KafkaUnboundedReader.METRIC_NAMESPACE,
                KafkaUnboundedReader.RAW_SIZE_METRIC_PREFIX + "test-5",
                readStep,
                DistributionResult.create(
                    recordSize * numElements / numPartitionsPerTopic,
                    numElements / numPartitionsPerTopic,
                    recordSize,
                    recordSize))));

    assertThat(
        metrics.getDistributions(),
        hasItem(
            attemptedMetricsResult(
                KafkaUnboundedReader.METRIC_NAMESPACE,
                KafkaUnboundedReader.RAW_SIZE_METRIC_PREFIX + "test-8",
                readStep,
                DistributionResult.create(
                    recordSize * numElements / numPartitionsPerTopic,
                    numElements / numPartitionsPerTopic,
                    recordSize,
                    recordSize))));
  }

  @Test
  public void testSourceDisplayData() {
    KafkaIO.Read<Integer, Long> read = mkKafkaReadTransform(10, null);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("topics", "topic_a,topic_b"));
    assertThat(displayData, hasDisplayItem("enable.auto.commit", false));
    assertThat(displayData, hasDisplayItem("bootstrap.servers", "myServer1:9092,myServer2:9092"));
    assertThat(displayData, hasDisplayItem("auto.offset.reset", "latest"));
    assertThat(displayData, hasDisplayItem("receive.buffer.bytes", 524288));
  }

  @Test
  public void testSourceWithExplicitPartitionsDisplayData() {
    KafkaIO.Read<byte[], byte[]> read =
        KafkaIO.readBytes()
            .withBootstrapServers("myServer1:9092,myServer2:9092")
            .withTopicPartitions(
                ImmutableList.of(new TopicPartition("test", 5), new TopicPartition("test", 6)))
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    Lists.newArrayList("test"),
                    10,
                    10,
                    OffsetResetStrategy.EARLIEST)); // 10 partitions

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("topicPartitions", "test-5,test-6"));
    assertThat(displayData, hasDisplayItem("enable.auto.commit", false));
    assertThat(displayData, hasDisplayItem("bootstrap.servers", "myServer1:9092,myServer2:9092"));
    assertThat(displayData, hasDisplayItem("auto.offset.reset", "latest"));
    assertThat(displayData, hasDisplayItem("receive.buffer.bytes", 524288));
  }

  @Test
  public void testSourceWithPatternDisplayData() {
    KafkaIO.Read<byte[], byte[]> read =
        KafkaIO.readBytes()
            .withBootstrapServers("myServer1:9092,myServer2:9092")
            .withTopicPattern("[a-z]est")
            .withConsumerFactoryFn(
                new ConsumerFactoryFn(
                    Lists.newArrayList("test"), 10, 10, OffsetResetStrategy.EARLIEST));

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("topicPattern", "[a-z]est"));
    assertThat(displayData, hasDisplayItem("enable.auto.commit", false));
    assertThat(displayData, hasDisplayItem("bootstrap.servers", "myServer1:9092,myServer2:9092"));
    assertThat(displayData, hasDisplayItem("auto.offset.reset", "latest"));
    assertThat(displayData, hasDisplayItem("receive.buffer.bytes", 524288));
  }

  @Test
  public void testSinkDisplayData() {
    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {
      KafkaIO.Write<Integer, Long> write =
          KafkaIO.<Integer, Long>write()
              .withBootstrapServers("myServerA:9092,myServerB:9092")
              .withTopic("myTopic")
              .withValueSerializer(LongSerializer.class)
              .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey))
              .withProducerConfigUpdates(ImmutableMap.of("retry.backoff.ms", 100));

      DisplayData displayData = DisplayData.from(write);

      assertThat(displayData, hasDisplayItem("topic", "myTopic"));
      assertThat(displayData, hasDisplayItem("bootstrap.servers", "myServerA:9092,myServerB:9092"));
      assertThat(displayData, hasDisplayItem("retries", 3));
      assertThat(displayData, hasDisplayItem("retry.backoff.ms", 100));
    }
  }

  @Test
  public void testSinkMetrics() throws Exception {
    // Simply read from kafka source and write to kafka sink. Then verify the metrics are reported.

    int numElements = 1000;

    try (MockProducerWrapper producerWrapper = new MockProducerWrapper(new LongSerializer())) {

      ProducerSendCompletionThread completionThread =
          new ProducerSendCompletionThread(producerWrapper.mockProducer).start();

      String topic = "test";

      p.apply(mkKafkaReadTransform(numElements, new ValueAsTimestampFn()).withoutMetadata())
          .apply(
              "writeToKafka",
              KafkaIO.<Integer, Long>write()
                  .withBootstrapServers("none")
                  .withTopic(topic)
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(LongSerializer.class)
                  .withProducerFactoryFn(new ProducerFactoryFn(producerWrapper.producerKey)));

      PipelineResult result = p.run();

      MetricName elementsWritten = SinkMetrics.elementsWritten().getName();

      MetricQueryResults metrics =
          result
              .metrics()
              .queryMetrics(
                  MetricsFilter.builder()
                      .addNameFilter(MetricNameFilter.inNamespace(elementsWritten.getNamespace()))
                      .build());

      assertThat(
          metrics.getCounters(),
          hasItem(
              attemptedMetricsResult(
                  elementsWritten.getNamespace(),
                  elementsWritten.getName(),
                  "writeToKafka",
                  1000L)));

      completionThread.shutdown();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testWithInvalidConsumerPollingTimeout() {
    KafkaIO.<Integer, Long>read().withConsumerPollingTimeout(-5L);
  }

  @Test
  public void testWithValidConsumerPollingTimeout() {
    KafkaIO.Read<Integer, Long> reader =
        KafkaIO.<Integer, Long>read().withConsumerPollingTimeout(15L);
    assertEquals(15, reader.getConsumerPollingTimeout());
  }

  private static void verifyProducerRecords(
      MockProducer<Integer, Long> mockProducer,
      String topic,
      int numElements,
      boolean keyIsAbsent,
      boolean verifyTimestamp) {

    // verify that appropriate messages are written to kafka
    List<ProducerRecord<Integer, Long>> sent = mockProducer.history();

    // sort by values
    sent.sort(Comparator.comparingLong(ProducerRecord::value));

    for (int i = 0; i < numElements; i++) {
      ProducerRecord<Integer, Long> record = sent.get(i);
      assertEquals(topic, record.topic());
      if (keyIsAbsent) {
        assertNull(record.key());
      } else {
        assertEquals(i, record.key().intValue());
      }
      assertEquals(i, record.value().longValue());
      if (verifyTimestamp) {
        assertEquals(i, record.timestamp().intValue());
      }
    }
  }

  /**
   * This wrapper over MockProducer. It also places the mock producer in global MOCK_PRODUCER_MAP.
   * The map is needed so that the producer returned by ProducerFactoryFn during pipeline can be
   * used in verification after the test. We also override {@code flush()} method in MockProducer so
   * that test can control behavior of {@code send()} method (e.g. to inject errors).
   */
  private static class MockProducerWrapper implements AutoCloseable {

    final String producerKey;
    final MockProducer<Integer, Long> mockProducer;

    // MockProducer has "closed" method starting version 0.11.
    private static Method closedMethod;

    static {
      try {
        closedMethod = MockProducer.class.getMethod("closed");
      } catch (NoSuchMethodException e) {
        closedMethod = null;
      }
    }

    MockProducerWrapper(Serializer<Long> valueSerializer) {
      producerKey = String.valueOf(ThreadLocalRandom.current().nextLong());
      mockProducer =
          new MockProducer<Integer, Long>(
              Cluster.empty()
                  .withPartitions(
                      ImmutableMap.of(
                          new TopicPartition("test", 0),
                          new PartitionInfo("test", 0, null, null, null),
                          new TopicPartition("test", 1),
                          new PartitionInfo("test", 1, null, null, null))),
              false, // disable synchronous completion of send. see ProducerSendCompletionThread
              // below.
              new DefaultPartitioner(),
              new IntegerSerializer(),
              valueSerializer) {

            // override flush() so that it does not complete all the waiting sends, giving a chance
            // to
            // ProducerCompletionThread to inject errors.

            @Override
            public synchronized void flush() {
              while (completeNext()) {
                // there are some uncompleted records. let the completion thread handle them.
                try {
                  Thread.sleep(10);
                } catch (InterruptedException e) {
                  // ok to retry.
                }
              }
            }
          };

      // Add the producer to the global map so that producer factory function can access it.
      assertNull(MOCK_PRODUCER_MAP.putIfAbsent(producerKey, mockProducer));
    }

    @Override
    public void close() {
      MOCK_PRODUCER_MAP.remove(producerKey);
      try {
        if (closedMethod == null || !((Boolean) closedMethod.invoke(mockProducer))) {
          mockProducer.close();
        }
      } catch (Exception e) { // Not expected.
        throw new RuntimeException(e);
      }
    }
  }

  private static final ConcurrentMap<String, MockProducer<Integer, Long>> MOCK_PRODUCER_MAP =
      new ConcurrentHashMap<>();

  private static class ProducerFactoryFn
      implements SerializableFunction<Map<String, Object>, Producer<Integer, Long>> {
    final String producerKey;

    ProducerFactoryFn(String producerKey) {
      this.producerKey = producerKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Producer<Integer, Long> apply(Map<String, Object> config) {

      // Make sure the config is correctly set up for serializers.
      Utils.newInstance(
              (Class<? extends Serializer<?>>)
                  ((Class<?>) config.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
                      .asSubclass(Serializer.class))
          .configure(config, true);

      Utils.newInstance(
              (Class<? extends Serializer<?>>)
                  ((Class<?>) config.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
                      .asSubclass(Serializer.class))
          .configure(config, false);

      // Returning same producer in each instance in a pipeline seems to work fine currently.
      // If DirectRunner creates multiple DoFn instances for sinks, we might need to handle
      // it appropriately. I.e. allow multiple producers for each producerKey and concatenate
      // all the messages written to each producer for verification after the pipeline finishes.

      return MOCK_PRODUCER_MAP.get(producerKey);
    }
  }

  private static class InjectedErrorException extends RuntimeException {
    InjectedErrorException(String message) {
      super(message);
    }
  }

  /**
   * We start MockProducer with auto-completion disabled. That implies a record is not marked sent
   * until #completeNext() is called on it. This class starts a thread to asynchronously 'complete'
   * the sends. During completion, we can also make those requests fail. This error injection is
   * used in one of the tests.
   */
  private static class ProducerSendCompletionThread {

    private final MockProducer<Integer, Long> mockProducer;
    private final int maxErrors;
    private final int errorFrequency;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final ExecutorService injectorThread;
    private int numCompletions = 0;

    ProducerSendCompletionThread(MockProducer<Integer, Long> mockProducer) {
      // complete everything successfully
      this(mockProducer, 0, 0);
    }

    ProducerSendCompletionThread(
        MockProducer<Integer, Long> mockProducer, int maxErrors, int errorFrequency) {
      this.mockProducer = mockProducer;
      this.maxErrors = maxErrors;
      this.errorFrequency = errorFrequency;
      injectorThread = Executors.newSingleThreadExecutor();
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    ProducerSendCompletionThread start() {
      injectorThread.submit(
          () -> {
            int errorsInjected = 0;

            while (!done.get()) {
              boolean successful;

              if (errorsInjected < maxErrors && ((numCompletions + 1) % errorFrequency) == 0) {
                successful =
                    mockProducer.errorNext(
                        new InjectedErrorException("Injected Error #" + (errorsInjected + 1)));

                if (successful) {
                  errorsInjected++;
                }
              } else {
                successful = mockProducer.completeNext();
              }

              if (successful) {
                numCompletions++;
              } else {
                // wait a bit since there are no unsent records
                try {
                  Thread.sleep(1);
                } catch (InterruptedException e) {
                  // ok to retry.
                }
              }
            }
          });

      return this;
    }

    void shutdown() {
      done.set(true);
      injectorThread.shutdown();
      try {
        assertTrue(injectorThread.awaitTermination(10, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private abstract static class BaseAvroSerializableFunction
      implements SerializableFunction<Integer, byte[]> {
    static transient Serializer<AvroGeneratedUser> serializer = null;
    final String topic;
    final String schemaRegistryUrl;
    final boolean isKey;

    BaseAvroSerializableFunction(String topic, String schemaRegistryUrl, boolean isKey) {
      this.topic = topic;
      this.schemaRegistryUrl = schemaRegistryUrl;
      this.isKey = isKey;
    }

    static Serializer<AvroGeneratedUser> getSerializer(boolean isKey, String schemaRegistryUrl) {
      if (serializer == null) {
        SchemaRegistryClient mockRegistryClient =
            MockSchemaRegistry.getClientForScope(schemaRegistryUrl);
        Map<String, Object> map = new HashMap<>();
        map.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        map.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serializer = (Serializer) new KafkaAvroSerializer(mockRegistryClient);
        serializer.configure(map, isKey);
      }
      return serializer;
    }
  }

  private static class KeyAvroSerializableFunction extends BaseAvroSerializableFunction {
    KeyAvroSerializableFunction(String topic, String schemaRegistryUrl) {
      super(topic, schemaRegistryUrl, true);
    }

    @Override
    public byte[] apply(Integer i) {
      return getSerializer(isKey, schemaRegistryUrl)
          .serialize(topic, new AvroGeneratedUser("KeyName" + i, i, "color" + i));
    }
  }

  private static class ValueAvroSerializableFunction extends BaseAvroSerializableFunction {
    ValueAvroSerializableFunction(String topic, String schemaRegistryUrl) {
      super(topic, schemaRegistryUrl, false);
    }

    @Override
    public byte[] apply(Integer i) {
      return getSerializer(isKey, schemaRegistryUrl)
          .serialize(topic, new AvroGeneratedUser("ValueName" + i, i, "color" + i));
    }
  }
}
