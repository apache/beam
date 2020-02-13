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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.producer.IKinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link KinesisIO.Write}. */
@RunWith(JUnit4.class)
public class KinesisMockWriteTest {
  private static final String STREAM = "BEAM";
  private static final String PARTITION_KEY = "partitionKey";

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public final transient TestPipeline p2 = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void beforeTest() {
    KinesisServiceMock kinesisService = KinesisServiceMock.getInstance();
    kinesisService.init(STREAM, 1);
  }

  @Test
  public void testWriteBuildsCorrectly() {
    Properties properties = new Properties();
    properties.setProperty("KinesisEndpoint", "localhost");
    properties.setProperty("KinesisPort", "4567");

    KinesisIO.Write write =
        KinesisIO.write()
            .withStreamName(STREAM)
            .withPartitionKey(PARTITION_KEY)
            .withPartitioner(new BasicKinesisPartitioner())
            .withAWSClientsProvider(new FakeKinesisProvider())
            .withProducerProperties(properties)
            .withRetries(10);

    assertEquals(STREAM, write.getStreamName());
    assertEquals(PARTITION_KEY, write.getPartitionKey());
    assertEquals(properties, write.getProducerProperties());
    assertEquals(FakeKinesisProvider.class, write.getAWSClientsProvider().getClass());
    assertEquals(BasicKinesisPartitioner.class, write.getPartitioner().getClass());
    assertEquals(10, write.getRetries());

    assertEquals("localhost", write.getProducerProperties().getProperty("KinesisEndpoint"));
    assertEquals("4567", write.getProducerProperties().getProperty("KinesisPort"));
  }

  @Test
  public void testWriteValidationFailsMissingStreamName() {
    KinesisIO.Write write =
        KinesisIO.write()
            .withPartitionKey(PARTITION_KEY)
            .withAWSClientsProvider(new FakeKinesisProvider());

    thrown.expect(IllegalArgumentException.class);
    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingPartitioner() {
    KinesisIO.Write write =
        KinesisIO.write().withStreamName(STREAM).withAWSClientsProvider(new FakeKinesisProvider());

    thrown.expect(IllegalArgumentException.class);
    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsPartitionerAndPartitioneKey() {
    KinesisIO.Write write =
        KinesisIO.write()
            .withStreamName(STREAM)
            .withPartitionKey(PARTITION_KEY)
            .withPartitioner(new BasicKinesisPartitioner())
            .withAWSClientsProvider(new FakeKinesisProvider());

    thrown.expect(IllegalArgumentException.class);
    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingAWSClientsProvider() {
    KinesisIO.Write write =
        KinesisIO.write().withPartitionKey(PARTITION_KEY).withStreamName(STREAM);

    thrown.expect(IllegalArgumentException.class);
    write.expand(null);
  }

  @Test
  public void testSetInvalidProperty() {
    Properties properties = new Properties();
    properties.setProperty("KinesisPort", "qwe");

    KinesisIO.Write write =
        KinesisIO.write()
            .withStreamName(STREAM)
            .withPartitionKey(PARTITION_KEY)
            .withAWSClientsProvider(new FakeKinesisProvider())
            .withProducerProperties(properties);

    thrown.expect(IllegalArgumentException.class);
    write.expand(null);
  }

  @Test
  public void testWrite() {
    KinesisServiceMock kinesisService = KinesisServiceMock.getInstance();

    Properties properties = new Properties();
    properties.setProperty("KinesisEndpoint", "localhost");
    properties.setProperty("KinesisPort", "4567");
    properties.setProperty("VerifyCertificate", "false");

    Iterable<byte[]> data =
        ImmutableList.of(
            "1".getBytes(StandardCharsets.UTF_8),
            "2".getBytes(StandardCharsets.UTF_8),
            "3".getBytes(StandardCharsets.UTF_8));
    p.apply(Create.of(data))
        .apply(
            KinesisIO.write()
                .withStreamName(STREAM)
                .withPartitionKey(PARTITION_KEY)
                .withAWSClientsProvider(new FakeKinesisProvider())
                .withProducerProperties(properties));
    p.run().waitUntilFinish();

    assertEquals(3, kinesisService.getAddedRecords().get());
  }

  @Test
  public void testWriteFailed() {
    Iterable<byte[]> data = ImmutableList.of("1".getBytes(StandardCharsets.UTF_8));
    p.apply(Create.of(data))
        .apply(
            KinesisIO.write()
                .withStreamName(STREAM)
                .withPartitionKey(PARTITION_KEY)
                .withAWSClientsProvider(new FakeKinesisProvider().setFailedFlush(true))
                .withRetries(2));

    thrown.expect(RuntimeException.class);
    p.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadFromMockKinesis() {
    KinesisServiceMock kinesisService = KinesisServiceMock.getInstance();

    Iterable<byte[]> data =
        ImmutableList.of(
            "1".getBytes(StandardCharsets.UTF_8), "2".getBytes(StandardCharsets.UTF_8));
    p.apply(Create.of(data))
        .apply(
            KinesisIO.write()
                .withStreamName(STREAM)
                .withPartitionKey(PARTITION_KEY)
                .withAWSClientsProvider(new FakeKinesisProvider()));
    p.run().waitUntilFinish();
    assertEquals(2, kinesisService.getAddedRecords().get());

    List<List<AmazonKinesisMock.TestData>> testData = kinesisService.getShardedData();

    int noOfShards = 1;
    int noOfEventsPerShard = 2;
    PCollection<AmazonKinesisMock.TestData> result =
        p2.apply(
                KinesisIO.read()
                    .withStreamName(STREAM)
                    .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                    .withAWSClientsProvider(new AmazonKinesisMock.Provider(testData, 10))
                    .withMaxNumRecords(noOfShards * noOfEventsPerShard))
            .apply(ParDo.of(new KinesisMockReadTest.KinesisRecordToTestData()));
    PAssert.that(result).containsInAnyOrder(Iterables.concat(testData));
    p2.run().waitUntilFinish();
  }

  private static final class BasicKinesisPartitioner implements KinesisPartitioner {
    @Override
    public String getPartitionKey(byte[] value) {
      return String.valueOf(value.length);
    }

    @Override
    public String getExplicitHashKey(byte[] value) {
      return null;
    }
  }

  private static final class FakeKinesisProvider implements AWSClientsProvider {
    private boolean isExistingStream = true;
    private boolean isFailedFlush = false;

    public FakeKinesisProvider() {}

    public FakeKinesisProvider(boolean isExistingStream) {
      this.isExistingStream = isExistingStream;
    }

    public FakeKinesisProvider setFailedFlush(boolean failedFlush) {
      isFailedFlush = failedFlush;
      return this;
    }

    @Override
    public AmazonKinesis getKinesisClient() {
      return getMockedAmazonKinesisClient();
    }

    @Override
    public AmazonCloudWatch getCloudWatchClient() {
      throw new RuntimeException("Not implemented");
    }

    @Override
    public IKinesisProducer createKinesisProducer(KinesisProducerConfiguration config) {
      return new KinesisProducerMock(config, isFailedFlush);
    }

    private AmazonKinesis getMockedAmazonKinesisClient() {
      int statusCode = isExistingStream ? 200 : 404;
      SdkHttpMetadata httpMetadata = mock(SdkHttpMetadata.class);
      when(httpMetadata.getHttpStatusCode()).thenReturn(statusCode);

      DescribeStreamResult streamResult = mock(DescribeStreamResult.class);
      when(streamResult.getSdkHttpMetadata()).thenReturn(httpMetadata);

      AmazonKinesis client = mock(AmazonKinesis.class);
      when(client.describeStream(any(String.class))).thenReturn(streamResult);

      return client;
    }
  }
}
