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

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
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
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.localstack.LocalStackContainer;

/**
 * Integration test, that writes and reads data to and from real Kinesis. You need to provide {@link
 * KinesisTestOptions} in order to run this if you want to test it with production setup. By default
 * when no options are provided an instance of localstack is used.
 */
@RunWith(JUnit4.class)
public class KinesisIOIT implements Serializable {
  private static final String LOCALSTACK_VERSION = "0.11.3";

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  private static LocalStackContainer localstackContainer;
  private static String streamName;
  private static AmazonKinesis kinesisClient;

  private static KinesisTestOptions options;

  @BeforeClass
  public static void setup() throws Exception {
    PipelineOptionsFactory.register(KinesisTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(KinesisTestOptions.class);
    if (doUseLocalstack()) {
      setupLocalstack();
    }
    kinesisClient = createKinesisClient();
    streamName = "beam_test_kinesis" + UUID.randomUUID();
    createStream();
  }

  @AfterClass
  public static void teardown() {
    if (doUseLocalstack()) {
      System.clearProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY);
      System.clearProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY);
      localstackContainer.stop();
    }
    kinesisClient.deleteStream(streamName);
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
                .withStreamName(streamName)
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
                .withStreamName(streamName)
                .withAWSClientsProvider(
                    options.getAwsAccessKey(),
                    options.getAwsSecretKey(),
                    Regions.fromName(options.getAwsKinesisRegion()),
                    options.getAwsServiceEndpoint(),
                    options.getAwsVerifyCertificate())
                .withMaxNumRecords(options.getNumberOfRecords())
                // to prevent endless running in case of error
                .withMaxReadTime(Duration.standardMinutes(10L))
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
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
    System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
    System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");

    localstackContainer =
        new LocalStackContainer(LOCALSTACK_VERSION)
            .withServices(LocalStackContainer.Service.KINESIS)
            .withEnv("USE_SSL", "true")
            .withStartupAttempts(3);
    localstackContainer.start();

    options.setAwsServiceEndpoint(
        localstackContainer
            .getEndpointConfiguration(LocalStackContainer.Service.KINESIS)
            .getServiceEndpoint()
            .replace("http", "https"));
    options.setAwsKinesisRegion(
        localstackContainer
            .getEndpointConfiguration(LocalStackContainer.Service.KINESIS)
            .getSigningRegion());
    options.setAwsAccessKey(
        localstackContainer.getDefaultCredentialsProvider().getCredentials().getAWSAccessKeyId());
    options.setAwsSecretKey(
        localstackContainer.getDefaultCredentialsProvider().getCredentials().getAWSSecretKey());
    options.setNumberOfRecords(1000);
    options.setNumberOfShards(1);
    options.setAwsKinesisStream("beam_kinesis_test");
    options.setAwsVerifyCertificate(false);
  }

  private static AmazonKinesis createKinesisClient() {
    AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();

    AWSCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(options.getAwsAccessKey(), options.getAwsSecretKey()));
    clientBuilder.setCredentials(credentialsProvider);

    if (options.getAwsServiceEndpoint() != null) {
      AwsClientBuilder.EndpointConfiguration endpointConfiguration =
          new AwsClientBuilder.EndpointConfiguration(
              options.getAwsServiceEndpoint(), options.getAwsKinesisRegion());
      clientBuilder.setEndpointConfiguration(endpointConfiguration);
    } else {
      clientBuilder.setRegion(options.getAwsKinesisRegion());
    }

    return clientBuilder.build();
  }

  private static void createStream() throws Exception {
    kinesisClient.createStream(streamName, 1);
    int repeats = 10;
    for (int i = 0; i <= repeats; ++i) {
      String streamStatus =
          kinesisClient.describeStream(streamName).getStreamDescription().getStreamStatus();
      if ("ACTIVE".equals(streamStatus)) {
        break;
      }
      if (i == repeats) {
        throw new RuntimeException("Unable to initialize stream");
      }
      Thread.sleep(1000L);
    }
  }

  /** Check whether pipeline options were provided. If not, use localstack container. */
  private static boolean doUseLocalstack() {
    KinesisTestOptions defaults = PipelineOptionsFactory.fromArgs().as(KinesisTestOptions.class);
    return defaults.getAwsAccessKey().equals(options.getAwsAccessKey())
        && defaults.getAwsSecretKey().equals(options.getAwsSecretKey())
        && defaults.getAwsKinesisStream().equals(options.getAwsKinesisStream())
        && defaults.getAwsKinesisRegion().equals(options.getAwsKinesisRegion())
        && defaults.getNumberOfShards().equals(options.getNumberOfShards())
        && defaults.getNumberOfRecords().equals(options.getNumberOfRecords())
        && (options.getAwsServiceEndpoint() == null
            || options.getAwsServiceEndpoint().equals(defaults.getAwsServiceEndpoint()))
        && defaults.getAwsVerifyCertificate().equals(options.getAwsVerifyCertificate());
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
