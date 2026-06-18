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
package org.apache.beam.sdk.io.aws2.kinesis.testing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.KINESIS;

import java.io.Serializable;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws2.ITEnvironment;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.common.InitialPositionInStream;

/**
 * Integration test that writes and reads data to and from Kinesis.
 *
 * <p>By default this runs against Localstack, but you can use {@link KinesisIOIT.ITOptions} to
 * configure tests to run against a real AWS Kinesis stream.
 *
 * <pre>{@code
 * ./gradlew :sdks:java:io:amazon-web-services2:integrationTest \
 *   --info \
 *   --tests "org.apache.beam.sdk.io.aws2.kinesis.testing.KinesisIOIT" \
 *   -DintegrationTestPipelineOptions='["--awsRegion=eu-central-1","--useLocalstack=false"]'
 * }</pre>
 */
@RunWith(JUnit4.class)
public class KinesisIOIT implements Serializable {
  public interface ITOptions extends ITEnvironment.ITOptions {
    @Description("Kinesis stream name")
    @Default.String("beam-kinesisio-it")
    String getKinesisStream();

    void setKinesisStream(String value);

    @Description("Kinesis consumer ARN - set if want to test EFO")
    @Nullable
    String getConsumerArn();

    void setConsumerArn(@Nullable String value);

    @Description("Number of shards of stream")
    @Default.Integer(8)
    Integer getKinesisShards();

    void setKinesisShards(Integer count);

    @Description("Use record aggregation when writing to Kinesis")
    @Default.Boolean(true)
    Boolean getUseRecordAggregation();

    void setUseRecordAggregation(Boolean enabled);

    @Description("Create stream")
    @Default.Boolean(false)
    Boolean getCreateStream();

    void setCreateStream(Boolean createStream);

    @Description(
        "Create EFO consumer with the given name. If set, consumer ARN conf will be ignored.")
    @Nullable
    String getCreateConsumerWithName();

    void setCreateConsumerWithName(@Nullable String createConsumerWithName);
  }

  @ClassRule
  public static ITEnvironment<ITOptions> env =
      new ITEnvironment<>(
          KINESIS, ITOptions.class, "KINESIS_ERROR_PROBABILITY=0.01", "USE_SSL=true");

  private static Instant now = Instant.now();
  private final StringBuilder createdConsumerArn = new StringBuilder();

  @Rule public TestPipeline writePipeline = env.createTestPipeline();
  @Rule public TestPipeline readPipeline = env.createTestPipeline();
  @Rule public ExternalResource kinesisStream = CreateStream.optionally(env.options());

  @Rule
  public ExternalResource kinesisConsumer =
      CreateConsumer.optionally(env.options(), createdConsumerArn);

  /** Test which write and then read data for a Kinesis stream. */
  @Test
  public void testWriteThenRead() {
    // For some unclear reason localstack requires a timestamp in seconds
    if (env.options().getUseLocalstack()) {
      now = Instant.ofEpochMilli(Long.divideUnsigned(now.getMillis(), 1000L));
    }
    runWrite();
    runRead();
  }

  /** Write test dataset into Kinesis stream. */
  private void runWrite() {
    ITOptions options = env.options();
    KinesisIO.Write<TestRow> write =
        KinesisIO.<TestRow>write()
            .withStreamName(env.options().getKinesisStream())
            .withPartitioner(row -> row.name())
            .withSerializer(testRowToBytes);
    if (!options.getUseRecordAggregation()) {
      write = write.withRecordAggregationDisabled();
    }

    writePipeline
        .apply("Generate Sequence", GenerateSequence.from(0).to(options.getNumberOfRows()))
        .apply("Prepare TestRows", ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply("Write to Kinesis", write);

    writePipeline.run().waitUntilFinish();
  }

  /** Read test dataset from Kinesis stream. */
  private void runRead() {
    ITOptions options = env.options();
    int records = env.options().getNumberOfRows();

    String finalConsumerArn =
        createdConsumerArn.length() != 0 ? createdConsumerArn.toString() : options.getConsumerArn();
    PCollection<KinesisRecord> output =
        readPipeline.apply(
            KinesisIO.read()
                .withStreamName(options.getKinesisStream())
                .withConsumerArn(finalConsumerArn)
                .withMaxNumRecords(records)
                // to prevent endless running in case of error
                .withMaxReadTime(Duration.standardMinutes(5))
                .withInitialPositionInStream(InitialPositionInStream.AT_TIMESTAMP)
                .withInitialTimestampInStream(now));

    PAssert.thatSingleton(output.apply("Count All", Count.globally())).isEqualTo((long) records);

    PCollection<String> consolidatedHashcode =
        output
            .apply(ParDo.of(new ExtractDataValues()))
            .apply(Combine.globally(new HashingFn()).withoutDefaults());

    PAssert.that(consolidatedHashcode)
        .containsInAnyOrder(TestRow.getExpectedHashForRowCount(records));

    readPipeline.run().waitUntilFinish();
  }

  static class CreateStream extends ExternalResource {
    static ExternalResource optionally(ITOptions opts) {
      boolean create = opts.getCreateStream() || opts.getUseLocalstack();
      return create ? new CreateStream() : new ExternalResource() {};
    }

    private final String name = env.options().getKinesisStream();
    private final int shards = env.options().getKinesisShards();
    private final KinesisClient client = env.buildClient(KinesisClient.builder());

    @Override
    protected void before() throws Exception {
      client.createStream(b -> b.streamName(name).shardCount(shards));
      pollStreamInfo(client, name, 10);
    }

    @Override
    protected void after() {
      client.deleteStream(b -> b.streamName(name).build());
      client.close();
    }
  }

  static class CreateConsumer extends ExternalResource {
    private StringBuilder createdConsumerArn;

    CreateConsumer(StringBuilder createdConsumerArn) {
      this.createdConsumerArn = createdConsumerArn;
    }

    /**
     * Creates stream consumer and deletes it after test finishes.
     *
     * <p>This works only with real Kinesis. {@link
     * KinesisClient#registerStreamConsumer(RegisterStreamConsumerRequest)} throws an error with
     * Localstack:
     *
     * <p>{@code org.apache.beam.sdk.io.aws2.kinesis.testing.KinesisIOIT > testWriteThenRead FAILED
     * software.amazon.awssdk.services.kinesis.model.KinesisException: Malformed message body:
     * Attempt to decode value on failed cursor: DownField(ConsumerName) (input position 68)
     * (Service: Kinesis, Status Code: 400, Request ID: c801380b-cfb0-11ed-bb7d-cfac6a89e52a,
     * Extended Request ID:
     * EsukB8wgGDTE/adFZIEIWbqXSzMu3W9LVQeqKNUTzZ53rIkrTJXRWPL0YAJqyG0HyVxY1EHrOIKQsPTP5xgEFy0klB/HO0BV)
     * at
     * app//software.amazon.awssdk.core.internal.http.CombinedResponseHandler.handleErrorResponse(CombinedResponseHandler.java:125)
     * }
     */
    static ExternalResource optionally(ITOptions opts, StringBuilder createdConsumerArn) {
      return opts.getCreateConsumerWithName() != null
          ? new CreateConsumer(createdConsumerArn)
          : new ExternalResource() {};
    }

    private final String name = env.options().getKinesisStream();
    private final KinesisClient client = env.buildClient(KinesisClient.builder());

    @Override
    protected void before() throws Exception {
      DescribeStreamResponse streamInfo = pollStreamInfo(client, name, 15);

      String consumerName = env.options().getCreateConsumerWithName();
      RegisterStreamConsumerResponse consumerInfo =
          client.registerStreamConsumer(
              RegisterStreamConsumerRequest.builder()
                  .streamARN(streamInfo.streamDescription().streamARN())
                  .consumerName(consumerName)
                  .build());

      DescribeStreamConsumerResponse newConsumerInfo =
          pollConsumerInfo(client, streamInfo, consumerInfo, 10);
      createdConsumerArn.append(newConsumerInfo.consumerDescription().consumerARN());
    }

    @Override
    protected void after() {
      String arn = createdConsumerArn.toString();
      if (!arn.isEmpty()) {
        client.deregisterStreamConsumer(b -> b.consumerARN(arn).build());
      }
      client.close();
    }
  }

  private static final SerializableFunction<TestRow, byte[]> testRowToBytes =
      row -> row.name().getBytes(UTF_8);

  private static class ExtractDataValues extends DoFn<KinesisRecord, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new String(c.element().getDataAsBytes(), UTF_8));
    }
  }

  private static DescribeStreamResponse pollStreamInfo(
      KinesisClient client, String streamName, int attempts) throws InterruptedException {
    for (int i = 0; i <= attempts; ++i) {
      DescribeStreamResponse streamInfo = client.describeStream(b -> b.streamName(streamName));
      if (StreamStatus.ACTIVE == streamInfo.streamDescription().streamStatus()) {
        return streamInfo;
      }
      Thread.sleep(1000L);
    }
    throw new RuntimeException("Stream did not become active");
  }

  private static DescribeStreamConsumerResponse pollConsumerInfo(
      KinesisClient client,
      DescribeStreamResponse streamInfo,
      RegisterStreamConsumerResponse consumerInfo,
      int attempts)
      throws InterruptedException {
    for (int i = 0; i <= attempts; ++i) {
      DescribeStreamConsumerRequest request =
          DescribeStreamConsumerRequest.builder()
              .streamARN(streamInfo.streamDescription().streamARN())
              .consumerARN(consumerInfo.consumer().consumerARN())
              .build();
      DescribeStreamConsumerResponse newConsumerInfo = client.describeStreamConsumer(request);
      if (newConsumerInfo.consumerDescription().consumerStatus() == ConsumerStatus.ACTIVE) {
        return newConsumerInfo;
      }
      Thread.sleep(1000L);
    }
    throw new RuntimeException("Consumer did not become active");
  }
}
