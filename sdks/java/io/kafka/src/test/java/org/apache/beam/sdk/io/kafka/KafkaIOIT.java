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

import static org.apache.beam.sdk.io.synthetic.SyntheticOptions.fromJsonString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeFalse;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.kafka.KafkaIOTest.FailingLongSerializer;
import org.apache.beam.sdk.io.kafka.ReadFromKafkaDoFnTest.FailingDeserializer;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.BadRecordErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.ErrorSinkTransform;
import org.apache.beam.sdk.transforms.windowing.CalendarWindows;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * IO Integration test for {@link org.apache.beam.sdk.io.kafka.KafkaIO}.
 *
 * <p>{@see https://beam.apache.org/documentation/io/testing/#i-o-transform-integration-tests} for
 * more details.
 *
 * <p>NOTE: This test sets retention policy of the messages so that all messages are retained in the
 * topic so that we could read them back after writing.
 */
@RunWith(JUnit4.class)
public class KafkaIOIT {

  private static final String READ_TIME_METRIC_NAME = "read_time";

  private static final String WRITE_TIME_METRIC_NAME = "write_time";

  private static final String RUN_TIME_METRIC_NAME = "run_time";

  private static final String NAMESPACE = KafkaIOIT.class.getName();

  private static final String TEST_ID = UUID.randomUUID().toString();

  private static final String TIMESTAMP = Instant.now().toString();

  private static final Logger LOG = LoggerFactory.getLogger(KafkaIOIT.class);

  private static SyntheticSourceOptions sourceOptions;

  private static Options options;

  private static InfluxDBSettings settings;

  @Rule public ExpectedLogs kafkaIOITExpectedLogs = ExpectedLogs.none(KafkaIOIT.class);

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline writePipeline2 = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  private static ExperimentalOptions sdfPipelineOptions;

  static {
    sdfPipelineOptions = PipelineOptionsFactory.create().as(ExperimentalOptions.class);
    ExperimentalOptions.addExperiment(sdfPipelineOptions, "use_sdf_read");
    ExperimentalOptions.addExperiment(sdfPipelineOptions, "beam_fn_api");
    sdfPipelineOptions.as(TestPipelineOptions.class).setBlockOnRun(false);
  }

  @Rule public TestPipeline sdfReadPipeline = TestPipeline.fromOptions(sdfPipelineOptions);
  @Rule public TestPipeline sdfReadPipeline2 = TestPipeline.fromOptions(sdfPipelineOptions);

  private static KafkaContainer kafkaContainer;

  @BeforeClass
  public static void setup() throws IOException {
    // check kafka version first
    @Nullable String targetVer = System.getProperty("beam.target.kafka.version");
    if (!Strings.isNullOrEmpty(targetVer)) {
      String actualVer = AppInfoParser.getVersion();
      assertEquals(targetVer, actualVer);
    }

    options = IOITHelper.readIOTestPipelineOptions(Options.class);
    sourceOptions = fromJsonString(options.getSourceOptions(), SyntheticSourceOptions.class);
    if (options.isWithTestcontainers()) {
      setupKafkaContainer();
    } else {
      settings =
          InfluxDBSettings.builder()
              .withHost(options.getInfluxHost())
              .withDatabase(options.getInfluxDatabase())
              .withMeasurement(options.getInfluxMeasurement())
              .get();
    }
  }

  @AfterClass
  public static void afterClass() {
    if (kafkaContainer != null) {
      kafkaContainer.stop();
    }
  }

  @Test
  public void testKafkaIOReadsAndWritesCorrectlyInStreaming() throws IOException {
    // Use batch pipeline to write records.
    writePipeline
        .apply("Generate records", Read.from(new SyntheticBoundedSource(sourceOptions)))
        .apply("Measure write time", ParDo.of(new TimeMonitor<>(NAMESPACE, WRITE_TIME_METRIC_NAME)))
        .apply("Write to Kafka", writeToKafka().withTopic(options.getKafkaTopic()));

    // Use streaming pipeline to read Kafka records.
    readPipeline.getOptions().as(Options.class).setStreaming(true);
    PCollection<Long> count =
        readPipeline
            .apply("Read from unbounded Kafka", readFromKafka().withTopic(options.getKafkaTopic()))
            .apply(
                "Measure read time", ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC_NAME)))
            .apply("Window", Window.into(CalendarWindows.years(1)))
            .apply(
                "Counting element",
                Combine.globally(Count.<KafkaRecord<byte[], byte[]>>combineFn()).withoutDefaults());

    PipelineResult writeResult = writePipeline.run();
    PipelineResult.State writeState = writeResult.waitUntilFinish();
    // Fail the test if pipeline failed.
    assertNotEquals(PipelineResult.State.FAILED, writeState);

    PAssert.thatSingleton(count).isEqualTo(sourceOptions.numRecords);

    PipelineResult readResult = readPipeline.run();
    PipelineResult.State readState =
        readResult.waitUntilFinish(Duration.standardSeconds(options.getReadTimeout()));

    // call asynchronous deleteTopics first since cancelIfTimeouted is blocking.
    tearDownTopic(options.getKafkaTopic());
    cancelIfTimeouted(readResult, readState);

    if (!options.isWithTestcontainers()) {
      Set<NamedTestResult> metrics = readMetrics(writeResult, readResult);
      IOITMetrics.publishToInflux(TEST_ID, TIMESTAMP, metrics, settings);
    }
    assertNotEquals(PipelineResult.State.FAILED, readState);
  }

  @Test
  public void testKafkaIOReadsAndWritesCorrectlyInBatch() throws IOException {
    writePipeline
        .apply("Generate records", Read.from(new SyntheticBoundedSource(sourceOptions)))
        .apply("Measure write time", ParDo.of(new TimeMonitor<>(NAMESPACE, WRITE_TIME_METRIC_NAME)))
        .apply("Write to Kafka", writeToKafka().withTopic(options.getKafkaTopic()));

    PCollection<Long> count =
        readPipeline
            .apply(
                "Read from bounded Kafka",
                readFromBoundedKafka().withTopic(options.getKafkaTopic()))
            .apply(
                "Measure read time", ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC_NAME)))
            .apply("Counting element", Count.globally());

    PipelineResult writeResult = writePipeline.run();
    writeResult.waitUntilFinish();

    PAssert.thatSingleton(count).isEqualTo(sourceOptions.numRecords);

    PipelineResult readResult = readPipeline.run();
    PipelineResult.State readState =
        readResult.waitUntilFinish(Duration.standardSeconds(options.getReadTimeout()));

    // call asynchronous deleteTopics first since cancelIfTimeouted is blocking.
    tearDownTopic(options.getKafkaTopic());
    cancelIfTimeouted(readResult, readState);

    assertNotEquals(PipelineResult.State.FAILED, readState);

    if (!options.isWithTestcontainers()) {
      Set<NamedTestResult> metrics = readMetrics(writeResult, readResult);
      IOITMetrics.publishToInflux(TEST_ID, TIMESTAMP, metrics, settings);
    }
  }

  // Because of existing limitations in streaming testing, this is verified via a combination of
  // DoFns.  CrashOnExtra will throw an exception if we see any extra records beyond those we
  // expect, and LogFn acts as a sink we can inspect using ExpectedLogs to verify that we got all
  // those we expect.
  @Test
  public void testKafkaIOSDFResumesCorrectly() throws IOException {
    roundtripElements("first-pass", 4, writePipeline, sdfReadPipeline);
    roundtripElements("second-pass", 3, writePipeline2, sdfReadPipeline2);
  }

  private void roundtripElements(
      String recordPrefix, Integer recordCount, TestPipeline wPipeline, TestPipeline rPipeline)
      throws IOException {
    AdminClient client =
        AdminClient.create(
            ImmutableMap.of("bootstrap.servers", options.getKafkaBootstrapServerAddresses()));
    client.listTopics();
    Map<Integer, String> records = new HashMap<>();
    for (int i = 0; i < recordCount; i++) {
      records.put(i, recordPrefix + "-" + i);
    }

    wPipeline
        .apply("Generate Write Elements", Create.of(records))
        .apply(
            "Write to Kafka",
            KafkaIO.<Integer, String>write()
                .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
                .withTopic(options.getKafkaTopic() + "-resuming")
                .withKeySerializer(IntegerSerializer.class)
                .withValueSerializer(StringSerializer.class));

    wPipeline.run().waitUntilFinish(Duration.standardSeconds(10));

    rPipeline
        .apply(
            "Read from Kafka",
            KafkaIO.<Integer, String>read()
                .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
                .withConsumerConfigUpdates(
                    ImmutableMap.of(
                        "group.id",
                        "resuming-group",
                        "auto.offset.reset",
                        "earliest",
                        "enable.auto.commit",
                        "true"))
                .withTopic(options.getKafkaTopic() + "-resuming")
                .withKeyDeserializer(IntegerDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata())
        .apply("Get Values", Values.create())
        .apply(ParDo.of(new CrashOnExtra(records.values())))
        .apply(ParDo.of(new LogFn()));

    rPipeline.run().waitUntilFinish(Duration.standardSeconds(options.getReadTimeout()));

    for (String value : records.values()) {
      kafkaIOITExpectedLogs.verifyError(value);
    }
  }

  public static class CrashOnExtra extends DoFn<String, String> {
    final Set<String> expected;

    public CrashOnExtra(Collection<String> records) {
      expected = new HashSet<>(records);
    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> outputReceiver) {
      if (!expected.contains(element)) {
        throw new RuntimeException("Received unexpected element: " + element);
      } else {
        expected.remove(element);
        outputReceiver.output(element);
      }
    }
  }

  public static class LogFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> outputReceiver) {
      LOG.error(element);
      outputReceiver.output(element);
    }
  }

  // This test verifies that bad data from Kafka is properly sent to the error handler
  @Test
  public void testKafkaIOSDFReadWithErrorHandler() throws IOException {
    // TODO(https://github.com/apache/beam/issues/32704) re-enable when fixed, or remove the support
    //  for these old kafka-client versions
    String actualVer = AppInfoParser.getVersion();
    assumeFalse(actualVer.compareTo("2.0.0") >= 0 && actualVer.compareTo("2.3.0") < 0);
    writePipeline
        .apply(Create.of(KV.of("key", "val")))
        .apply(
            "Write to Kafka",
            KafkaIO.<String, String>write()
                .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
                .withTopic(options.getKafkaTopic() + "-failingDeserialization"));

    PipelineResult writeResult = writePipeline.run();
    PipelineResult.State writeState = writeResult.waitUntilFinish();
    assertNotEquals(PipelineResult.State.FAILED, writeState);

    BadRecordErrorHandler<PCollection<Long>> eh =
        sdfReadPipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
    sdfReadPipeline.apply(
        KafkaIO.<String, String>read()
            .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
            .withTopic(options.getKafkaTopic() + "-failingDeserialization")
            .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest"))
            .withKeyDeserializer(FailingDeserializer.class)
            .withValueDeserializer(FailingDeserializer.class)
            .withBadRecordErrorHandler(eh));
    eh.close();

    PAssert.thatSingleton(Objects.requireNonNull(eh.getOutput())).isEqualTo(1L);

    PipelineResult readResult = sdfReadPipeline.run();
    PipelineResult.State readState =
        readResult.waitUntilFinish(Duration.standardSeconds(options.getReadTimeout()));
    cancelIfTimeouted(readResult, readState);
    assertNotEquals(PipelineResult.State.FAILED, readState);
  }

  @Test
  public void testKafkaIOWriteWithErrorHandler() throws IOException {

    BadRecordErrorHandler<PCollection<Long>> eh =
        writePipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
    writePipeline
        .apply("Create single KV", Create.of(KV.of("key", 4L)))
        .apply(
            "Write to Kafka",
            KafkaIO.<String, Long>write()
                .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(FailingLongSerializer.class)
                .withTopic(options.getKafkaTopic() + "-failingSerialization")
                .withBadRecordErrorHandler(eh));
    eh.close();

    PAssert.thatSingleton(Objects.requireNonNull(eh.getOutput())).isEqualTo(1L);

    PipelineResult writeResult = writePipeline.run();
    PipelineResult.State writeState = writeResult.waitUntilFinish();
    assertNotEquals(PipelineResult.State.FAILED, writeState);
  }

  // This test roundtrips a single KV<Null,Null> to verify that externalWithMetadata
  // can handle null keys and values correctly.
  @Test
  public void testKafkaIOExternalRoundtripWithMetadataAndNullKeysAndValues() throws IOException {

    writePipeline
        .apply(Create.of(KV.<byte[], byte[]>of(null, null)))
        .apply(
            "Write to Kafka", writeToKafka().withTopic(options.getKafkaTopic() + "-nullRoundTrip"));

    PCollection<Row> rows =
        readPipeline.apply(
            KafkaIO.<byte[], byte[]>read()
                .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
                .withTopic(options.getKafkaTopic() + "-nullRoundTrip")
                .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest"))
                .withKeyDeserializerAndCoder(
                    ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
                .withValueDeserializerAndCoder(
                    ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
                .withMaxNumRecords(1)
                .externalWithMetadata());

    PAssert.thatSingleton(rows)
        .satisfies(
            actualRow -> {
              assertNull(actualRow.getString("key"));
              assertNull(actualRow.getString("value"));
              return null;
            });

    PipelineResult writeResult = writePipeline.run();
    writeResult.waitUntilFinish();

    PipelineResult readResult = readPipeline.run();
    PipelineResult.State readState =
        readResult.waitUntilFinish(Duration.standardSeconds(options.getReadTimeout()));

    cancelIfTimeouted(readResult, readState);
  }

  @Test
  public void testKafkaWithDynamicPartitions() throws IOException {
    AdminClient client =
        AdminClient.create(
            ImmutableMap.of("bootstrap.servers", options.getKafkaBootstrapServerAddresses()));
    String topicName = "DynamicTopicPartition-" + UUID.randomUUID();
    Map<Integer, String> records = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      records.put(i, String.valueOf(i));
    }
    Map<Integer, String> moreRecords = new HashMap<>();
    for (int i = 100; i < 200; i++) {
      moreRecords.put(i, String.valueOf(i));
    }
    try {
      client.createTopics(ImmutableSet.of(new NewTopic(topicName, 1, (short) 1)));
      client.createPartitions(ImmutableMap.of(topicName, NewPartitions.increaseTo(1)));

      writePipeline
          .apply("Generate Write Elements", Create.of(records))
          .apply(
              "Write to Kafka",
              KafkaIO.<Integer, String>write()
                  .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
                  .withTopic(topicName)
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(StringSerializer.class));

      writePipeline.run().waitUntilFinish(Duration.standardSeconds(15));

      Thread delayedWriteThread =
          new Thread(
              () -> {
                try {
                  Thread.sleep(20 * 1000); // wait 20 seconds before changing kafka
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }

                client.createPartitions(ImmutableMap.of(topicName, NewPartitions.increaseTo(2)));

                writePipeline
                    .apply("Second Pass generate Write Elements", Create.of(moreRecords))
                    .apply(
                        "Write more to Kafka",
                        KafkaIO.<Integer, String>write()
                            .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
                            .withTopic(topicName)
                            .withKeySerializer(IntegerSerializer.class)
                            .withValueSerializer(StringSerializer.class));

                writePipeline.run().waitUntilFinish(Duration.standardSeconds(15));
              });

      delayedWriteThread.start();

      PCollection<Integer> values =
          sdfReadPipeline
              .apply(
                  "Read from Kafka",
                  KafkaIO.<Integer, String>read()
                      .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
                      .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest"))
                      .withTopic(topicName)
                      .withDynamicRead(Duration.standardSeconds(5))
                      .withKeyDeserializer(IntegerDeserializer.class)
                      .withValueDeserializer(StringDeserializer.class))
              .apply("Key by Partition", ParDo.of(new KeyByPartition()))
              .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
              .apply("Group by Partition", GroupByKey.create())
              .apply("Get Partitions", Keys.create());

      PAssert.that(values).containsInAnyOrder(0, 1);

      PipelineResult readResult = sdfReadPipeline.run();

      PipelineResult.State readState =
          readResult.waitUntilFinish(Duration.standardSeconds(options.getReadTimeout() / 2));

      cancelIfTimeouted(readResult, readState);
      // Fail the test if pipeline failed.
      assertNotEquals(PipelineResult.State.FAILED, readState);
    } finally {
      client.deleteTopics(ImmutableSet.of(topicName));
    }
  }

  @Test
  public void testKafkaWithStopReadingFunction() {
    AlwaysStopCheckStopReadingFn checkStopReadingFn = new AlwaysStopCheckStopReadingFn();

    runWithStopReadingFn(checkStopReadingFn, "stop-reading", 0L);
  }

  private static class AlwaysStopCheckStopReadingFn implements CheckStopReadingFn {
    @Override
    public Boolean apply(TopicPartition input) {
      return true;
    }
  }

  @Test
  public void testKafkaWithDelayedStopReadingFunction() {
    DelayedCheckStopReadingFn checkStopReadingFn = new DelayedCheckStopReadingFn();

    runWithStopReadingFn(checkStopReadingFn, "delayed-stop-reading", sourceOptions.numRecords);
  }

  public static final Schema KAFKA_TOPIC_SCHEMA =
      Schema.builder()
          .addStringField("name")
          .addInt64Field("userId")
          .addInt64Field("age")
          .addBooleanField("ageIsEven")
          .addDoubleField("temperature")
          .addArrayField("childrenNames", Schema.FieldType.STRING)
          .build();

  public static final String SCHEMA_IN_JSON =
      "{\n"
          + "  \"type\": \"object\",\n"
          + "  \"properties\": {\n"
          + "    \"name\": {\n"
          + "      \"type\": \"string\"\n"
          + "    },\n"
          + "    \"userId\": {\n"
          + "      \"type\": \"integer\"\n"
          + "    },\n"
          + "    \"age\": {\n"
          + "      \"type\": \"integer\"\n"
          + "    },\n"
          + "    \"ageIsEven\": {\n"
          + "      \"type\": \"boolean\"\n"
          + "    },\n"
          + "    \"temperature\": {\n"
          + "      \"type\": \"number\"\n"
          + "    },\n"
          + "    \"childrenNames\": {\n"
          + "      \"type\": \"array\",\n"
          + "      \"items\": {\n"
          + "        \"type\": \"string\"\n"
          + "      }\n"
          + "    }\n"
          + "  }\n"
          + "}";

  private static final int FIVE_MINUTES_IN_MS = 5 * 60 * 1000;

  @Test(timeout = FIVE_MINUTES_IN_MS)
  public void testKafkaViaManagedSchemaTransformJson() {
    runReadWriteKafkaViaManagedSchemaTransforms(
        "JSON", SCHEMA_IN_JSON, JsonUtils.beamSchemaFromJsonSchema(SCHEMA_IN_JSON));
  }

  @Test(timeout = FIVE_MINUTES_IN_MS)
  public void testKafkaViaManagedSchemaTransformAvro() {
    runReadWriteKafkaViaManagedSchemaTransforms(
        "AVRO", AvroUtils.toAvroSchema(KAFKA_TOPIC_SCHEMA).toString(), KAFKA_TOPIC_SCHEMA);
  }

  public void runReadWriteKafkaViaManagedSchemaTransforms(
      String format, String schemaDefinition, Schema beamSchema) {
    String topicName = options.getKafkaTopic() + "-schema-transform" + UUID.randomUUID();
    PCollectionRowTuple.of(
            "input",
            writePipeline
                .apply("Generate records", GenerateSequence.from(0).to(1000))
                .apply(
                    "Transform to Beam Rows",
                    MapElements.into(TypeDescriptors.rows())
                        .via(
                            numb ->
                                Row.withSchema(beamSchema)
                                    .withFieldValue("name", numb.toString())
                                    .withFieldValue(
                                        "userId", Long.valueOf(numb.hashCode())) // User ID
                                    .withFieldValue("age", Long.valueOf(numb.intValue())) // Age
                                    .withFieldValue("ageIsEven", numb % 2 == 0) // ageIsEven
                                    .withFieldValue("temperature", new Random(numb).nextDouble())
                                    .withFieldValue(
                                        "childrenNames",
                                        Lists.newArrayList(
                                            Long.toString(numb + 1),
                                            Long.toString(numb + 2))) // childrenNames
                                    .build()))
                .setRowSchema(beamSchema))
        .apply(
            "Write to Kafka",
            Managed.write(Managed.KAFKA)
                .withConfig(
                    ImmutableMap.<String, Object>builder()
                        .put("topic", topicName)
                        .put("bootstrap_servers", options.getKafkaBootstrapServerAddresses())
                        .put("format", format)
                        .build()));

    PAssert.that(
            PCollectionRowTuple.empty(readPipeline)
                .apply(
                    "Read from unbounded Kafka",
                    // A timeout of 30s for local, container-based tests, and 2 minutes for
                    // real-kafka tests.
                    Managed.read(Managed.KAFKA)
                        .withConfig(
                            ImmutableMap.<String, Object>builder()
                                .put("format", format)
                                .put("auto_offset_reset_config", "earliest")
                                .put("schema", schemaDefinition)
                                .put("topic", topicName)
                                .put(
                                    "bootstrap_servers", options.getKafkaBootstrapServerAddresses())
                                .put(
                                    "max_read_time_seconds",
                                    options.isWithTestcontainers() ? 30 : 120)
                                .build()))
                .get("output"))
        .containsInAnyOrder(
            LongStream.range(0L, 1000L)
                .<Row>mapToObj(
                    numb ->
                        Row.withSchema(beamSchema)
                            .withFieldValue("name", Long.toString(numb)) // Name
                            .withFieldValue("userId", Long.valueOf(Long.hashCode(numb))) // User ID
                            .withFieldValue("age", Long.valueOf(numb)) // Age
                            .withFieldValue("ageIsEven", numb % 2 == 0) // ageIsEven
                            .withFieldValue("temperature", new Random(numb).nextDouble())
                            .withFieldValue(
                                "childrenNames",
                                Lists.newArrayList(
                                    Long.toString(numb + 1),
                                    Long.toString(numb + 2))) // childrenNames
                            .build())
                .collect(Collectors.toList()));

    PipelineResult writeResult = writePipeline.run();
    writeResult.waitUntilFinish();

    PipelineResult readResult = readPipeline.run();
    readResult.waitUntilFinish(Duration.standardSeconds(options.getReadTimeout()));
    assertEquals(PipelineResult.State.DONE, readResult.getState());
  }

  private static class DelayedCheckStopReadingFn implements CheckStopReadingFn {
    int checkCount = 0;

    @Override
    public Boolean apply(TopicPartition input) {
      if (checkCount >= 10) {
        return true;
      }
      checkCount++;
      return false;
    }
  }

  private void runWithStopReadingFn(
      CheckStopReadingFn function, String topicSuffix, Long expectedCount) {
    writePipeline
        .apply("Generate records", Read.from(new SyntheticBoundedSource(sourceOptions)))
        .apply("Measure write time", ParDo.of(new TimeMonitor<>(NAMESPACE, WRITE_TIME_METRIC_NAME)))
        .apply(
            "Write to Kafka",
            writeToKafka().withTopic(options.getKafkaTopic() + "-" + topicSuffix));

    readPipeline.getOptions().as(Options.class).setStreaming(true);
    PCollection<Long> count =
        readPipeline
            .apply(
                "Read from unbounded Kafka",
                readFromKafka()
                    .withTopic(options.getKafkaTopic() + "-" + topicSuffix)
                    .withCheckStopReadingFn(function))
            .apply(
                "Measure read time", ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC_NAME)))
            .apply("Window", Window.into(CalendarWindows.years(1)))
            .apply(
                "Counting element",
                Combine.globally(Count.<KafkaRecord<byte[], byte[]>>combineFn()).withoutDefaults());

    if (expectedCount == 0L) {
      PAssert.that(count).empty();
    } else {
      PAssert.thatSingleton(count).isEqualTo(expectedCount);
    }

    PipelineResult writeResult = writePipeline.run();
    writeResult.waitUntilFinish();

    PipelineResult readResult = readPipeline.run();
    readResult.waitUntilFinish(Duration.standardSeconds(options.getReadTimeout()));
  }

  @Test
  public void testWatermarkUpdateWithSparseMessages() throws IOException, InterruptedException {
    AdminClient client =
        AdminClient.create(
            ImmutableMap.of("bootstrap.servers", options.getKafkaBootstrapServerAddresses()));

    String topicName = "SparseDataTopicPartition-" + UUID.randomUUID();
    Map<Integer, String> records = new HashMap<>();
    for (int i = 1; i <= 5; i++) {
      records.put(i, String.valueOf(i));
    }

    try {
      client.createTopics(ImmutableSet.of(new NewTopic(topicName, 1, (short) 1)));

      writePipeline
          .apply("Generate Write Elements", Create.of(records))
          .apply(
              "Write to Kafka",
              KafkaIO.<Integer, String>write()
                  .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
                  .withTopic(topicName)
                  .withKeySerializer(IntegerSerializer.class)
                  .withValueSerializer(StringSerializer.class));

      writePipeline.run().waitUntilFinish(Duration.standardSeconds(15));

      client.createPartitions(ImmutableMap.of(topicName, NewPartitions.increaseTo(3)));

      sdfReadPipeline
          .apply(
              "Read from Kafka",
              KafkaIO.<Integer, String>read()
                  .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
                  .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest"))
                  .withTopic(topicName)
                  .withKeyDeserializer(IntegerDeserializer.class)
                  .withValueDeserializer(StringDeserializer.class)
                  .withoutMetadata())
          .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
          .apply("GroupKey", GroupByKey.create())
          .apply("GetValues", Values.create())
          .apply("Flatten", Flatten.iterables())
          .apply("LogValues", ParDo.of(new LogFn()));

      PipelineResult readResult = sdfReadPipeline.run();

      Thread.sleep(options.getReadTimeout() * 1000 * 2);

      for (String value : records.values()) {
        kafkaIOITExpectedLogs.verifyError(value);
      }

      // Only waiting 5 seconds here because we don't expect any processing at this point
      PipelineResult.State readState = readResult.waitUntilFinish(Duration.standardSeconds(5));

      cancelIfTimeouted(readResult, readState);
      // Fail the test if pipeline failed.
      assertNotEquals(readState, PipelineResult.State.FAILED);
    } finally {
      client.deleteTopics(ImmutableSet.of(topicName));
    }
  }

  private static class KeyByPartition
      extends DoFn<KafkaRecord<Integer, String>, KV<Integer, KafkaRecord<Integer, String>>> {

    @ProcessElement
    public void processElement(
        @Element KafkaRecord<Integer, String> record,
        OutputReceiver<KV<Integer, KafkaRecord<Integer, String>>> receiver) {
      receiver.output(KV.of(record.getPartition(), record));
    }
  }

  private Set<NamedTestResult> readMetrics(PipelineResult writeResult, PipelineResult readResult) {
    BiFunction<MetricsReader, String, NamedTestResult> supplier =
        (reader, metricName) -> {
          long start = reader.getStartTimeMetric(metricName);
          long end = reader.getEndTimeMetric(metricName);
          return NamedTestResult.create(TEST_ID, TIMESTAMP, metricName, (end - start) / 1e3);
        };

    NamedTestResult writeTime =
        supplier.apply(new MetricsReader(writeResult, NAMESPACE), WRITE_TIME_METRIC_NAME);
    NamedTestResult readTime =
        supplier.apply(new MetricsReader(readResult, NAMESPACE), READ_TIME_METRIC_NAME);
    NamedTestResult runTime =
        NamedTestResult.create(
            TEST_ID, TIMESTAMP, RUN_TIME_METRIC_NAME, writeTime.getValue() + readTime.getValue());

    return ImmutableSet.of(readTime, writeTime, runTime);
  }

  private void cancelIfTimeouted(PipelineResult readResult, PipelineResult.State readState)
      throws IOException {

    // TODO(lgajowy) this solution works for dataflow only - it returns null when
    //  waitUntilFinish(Duration duration) exceeds provided duration.
    if (readState == null) {
      readResult.cancel();
    }
  }

  /** Delete the topic after test run. */
  private void tearDownTopic(String topicName) {
    AdminClient client =
        AdminClient.create(
            ImmutableMap.of("bootstrap.servers", options.getKafkaBootstrapServerAddresses()));
    client.deleteTopics(Collections.singleton(topicName));
  }

  private KafkaIO.Write<byte[], byte[]> writeToKafka() {
    return KafkaIO.<byte[], byte[]>write()
        .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
        .withKeySerializer(ByteArraySerializer.class)
        .withValueSerializer(ByteArraySerializer.class);
  }

  private KafkaIO.Read<byte[], byte[]> readFromBoundedKafka() {
    return readFromKafka().withMaxNumRecords(sourceOptions.numRecords);
  }

  private KafkaIO.Read<byte[], byte[]> readFromKafka() {
    return KafkaIO.readBytes()
        .withBootstrapServers(options.getKafkaBootstrapServerAddresses())
        .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest"));
  }

  /** Pipeline options specific for this test. */
  public interface Options extends IOTestPipelineOptions, StreamingOptions {

    @Description("Options for synthetic source.")
    @Validation.Required
    String getSourceOptions();

    void setSourceOptions(String sourceOptions);

    @Description("Kafka bootstrap server addresses")
    @Default.String("localhost:9092")
    String getKafkaBootstrapServerAddresses();

    void setKafkaBootstrapServerAddresses(String address);

    @Description("Kafka topic")
    @Validation.Required
    String getKafkaTopic();

    void setKafkaTopic(String topic);

    @Description("Time to wait for the events to be processed by the read pipeline (in seconds)")
    @Validation.Required
    Integer getReadTimeout();

    void setReadTimeout(Integer readTimeout);

    @Description("Whether to use testcontainers")
    @Default.Boolean(false)
    Boolean isWithTestcontainers();

    void setWithTestcontainers(Boolean withTestcontainers);

    @Description("Kafka container version in format 'X.Y.Z'. Use when useTestcontainers is true")
    @Nullable
    String getKafkaContainerVersion();

    void setKafkaContainerVersion(String kafkaContainerVersion);
  }

  private static void setupKafkaContainer() {
    kafkaContainer =
        new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka")
                .withTag(options.getKafkaContainerVersion()));
    // Adding startup attempts to try and deflake
    kafkaContainer.withStartupAttempts(3);
    kafkaContainer.start();
    options.setKafkaBootstrapServerAddresses(kafkaContainer.getBootstrapServers());
  }
}
