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
package org.apache.beam.sdk.io.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test, that writes and reads data to and from real Kinesis. You need to provide {@link
 * KinesisTestOptions} in order to run this if you want to test it with production setup. By default
 * when no options are provided an instance of localstack is used.
 */
@RunWith(JUnit4.class)
public class KinesisIOIT implements Serializable {
  private static final String LOCALSTACK_VERSION = "0.12.18";

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  // Will be run in reverse order
  private static final List<ThrowingRunnable> teardownTasks = new ArrayList<>();

  private static KinesisTestOptions options;

  private static Instant now = Instant.now();

  @BeforeClass
  public static void setup() throws Exception {
    PipelineOptionsFactory.register(KinesisTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(KinesisTestOptions.class);

    if (options.getUseLocalstack()) {
      setupLocalstack();
    }
    if (options.getCreateStream()) {
      AmazonKinesis kinesisClient = createKinesisClient();
      teardownTasks.add(kinesisClient::shutdown);

      createStream(kinesisClient);
      teardownTasks.add(() -> deleteStream(kinesisClient));
    }
  }

  @AfterClass
  public static void teardown() {
    Lists.reverse(teardownTasks).forEach(KinesisIOIT::safeRun);
    teardownTasks.clear();
  }

  /** Test which write and then read data for a Kinesis stream. */
  @Test
  public void testWriteThenRead() {
    runWrite();
    runRead();
  }

  /** Write test dataset into Kinesis stream. */
  private void runWrite() {
    pipelineWrite
        .apply("Generate Sequence", GenerateSequence.from(0).to(options.getNumberOfRecords()))
        .apply("Prepare TestRows", ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply("Prepare Kinesis input records", ParDo.of(new ConvertToBytes()))
        .apply(
            "Write to Kinesis",
            KinesisIO.write()
                .withStreamName(options.getAwsKinesisStream())
                .withPartitioner(new RandomPartitioner())
                .withAWSClientsProvider(
                    options.getAwsAccessKey(),
                    options.getAwsSecretKey(),
                    Regions.fromName(options.getAwsKinesisRegion()),
                    options.getAwsServiceEndpoint(),
                    options.getAwsVerifyCertificate()));

    pipelineWrite.run().waitUntilFinish();
  }

  /** Read test dataset from Kinesis stream. */
  private void runRead() {
    PCollection<KinesisRecord> output =
        pipelineRead.apply(
            KinesisIO.read()
                .withStreamName(options.getAwsKinesisStream())
                .withAWSClientsProvider(
                    options.getAwsAccessKey(),
                    options.getAwsSecretKey(),
                    Regions.fromName(options.getAwsKinesisRegion()),
                    options.getAwsServiceEndpoint(),
                    options.getAwsVerifyCertificate())
                .withMaxNumRecords(options.getNumberOfRecords())
                // to prevent endless running in case of error
                .withMaxReadTime(Duration.standardMinutes(10L))
                .withInitialPositionInStream(InitialPositionInStream.AT_TIMESTAMP)
                .withInitialTimestampInStream(now)
                .withRequestRecordsLimit(1000));

    PAssert.thatSingleton(output.apply("Count All", Count.globally()))
        .isEqualTo((long) options.getNumberOfRecords());

    PCollection<String> consolidatedHashcode =
        output
            .apply(ParDo.of(new ExtractDataValues()))
            .apply("Hash row contents", Combine.globally(new HashingFn()).withoutDefaults());

    PAssert.that(consolidatedHashcode)
        .containsInAnyOrder(TestRow.getExpectedHashForRowCount(options.getNumberOfRecords()));

    pipelineRead.run().waitUntilFinish();
  }

  /** Necessary setup for localstack environment. */
  private static void setupLocalstack() {
    // For some unclear reason localstack requires a timestamp in seconds
    now = Instant.ofEpochMilli(Long.divideUnsigned(now.getMillis(), 1000L));

    LocalStackContainer kinesisContainer =
        new LocalStackContainer(
                DockerImageName.parse("localstack/localstack").withTag(LOCALSTACK_VERSION))
            .withServices(Service.KINESIS)
            .withEnv("USE_SSL", "true")
            .withStartupAttempts(3);

    kinesisContainer.start();
    teardownTasks.add(() -> kinesisContainer.stop());

    options.setAwsServiceEndpoint(kinesisContainer.getEndpointOverride(Service.KINESIS).toString());
    options.setAwsKinesisRegion(kinesisContainer.getRegion());
    options.setAwsAccessKey(kinesisContainer.getAccessKey());
    options.setAwsSecretKey(kinesisContainer.getSecretKey());
    options.setAwsVerifyCertificate(false);
    options.setCreateStream(true);
  }

  private static AmazonKinesis createKinesisClient() {
    AWSCredentials credentials =
        new BasicAWSCredentials(options.getAwsAccessKey(), options.getAwsSecretKey());
    AmazonKinesisClientBuilder clientBuilder =
        AmazonKinesisClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials));

    if (options.getAwsServiceEndpoint() != null) {
      clientBuilder.setEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(
              options.getAwsServiceEndpoint(), options.getAwsKinesisRegion()));
    } else {
      clientBuilder.setRegion(options.getAwsKinesisRegion());
    }

    return clientBuilder.build();
  }

  private static void createStream(AmazonKinesis kinesisClient) throws Exception {
    kinesisClient.createStream(options.getAwsKinesisStream(), options.getNumberOfShards());
    int attempts = 10;
    for (int i = 0; i <= attempts; ++i) {
      String streamStatus =
          kinesisClient
              .describeStream(options.getAwsKinesisStream())
              .getStreamDescription()
              .getStreamStatus();
      if ("ACTIVE".equals(streamStatus)) {
        return;
      }
      Thread.sleep(1000L);
    }
    throw new RuntimeException("Unable to initialize stream");
  }

  private static void deleteStream(AmazonKinesis kinesisClient) {
    kinesisClient.deleteStream(options.getAwsKinesisStream());
  }

  private static void safeRun(ThrowingRunnable task) {
    try {
      task.run();
    } catch (Throwable e) {
      LoggerFactory.getLogger(KinesisIOIT.class).warn("Cleanup task failed", e);
    }
  }

  /** Produces test rows. */
  private static class ConvertToBytes extends DoFn<TestRow, byte[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.valueOf(c.element().name()).getBytes(StandardCharsets.UTF_8));
    }
  }

  /** Read rows from Table. */
  private static class ExtractDataValues extends DoFn<KinesisRecord, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new String(c.element().getDataAsBytes(), StandardCharsets.UTF_8));
    }
  }

  private static final class RandomPartitioner implements KinesisPartitioner {
    @Override
    public String getPartitionKey(byte[] value) {
      Random rand = new Random();
      int n = rand.nextInt(options.getNumberOfShards()) + 1;
      return String.valueOf(n);
    }

    @Override
    public String getExplicitHashKey(byte[] value) {
      return null;
    }
  }
}
