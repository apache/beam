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
package org.apache.beam.sdk.io.aws2.kinesis;

import static java.util.function.Function.identity;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.SHARD_EVENTS;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.createRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.eventWithRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.mockRecords;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.mockShardIterators;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.mockShards;
import static org.apache.beam.sdk.io.aws2.kinesis.TestHelpers.record;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.concat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON;

import java.net.URI;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.StaticSupplier;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Read;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.kinesis.common.InitialPositionInStream;

/** Tests for {@link KinesisIO#read}. */
@RunWith(MockitoJUnitRunner.class)
public class KinesisIOReadTest {
  private static final String KEY = "key";
  private static final String SECRET = "secret";

  private static final int SHARDS = 3;

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Mock public KinesisClient client;

  @Before
  public void configureClientBuilderFactory() {
    MockClientBuilderFactory.set(p, KinesisClientBuilder.class, client);
    MockClientBuilderFactory.set(p, CloudWatchClientBuilder.class, mock(CloudWatchClient.class));
  }

  @Test
  public void testReadDefaults() {
    KinesisIO.Read readSpec =
        KinesisIO.read()
            .withStreamName("streamName")
            .withInitialPositionInStream(InitialPositionInStream.LATEST);

    assertThat(readSpec.getStreamName()).isEqualTo("streamName");
    assertThat(readSpec.getConsumerArn()).isNull();

    assertThat(readSpec.getInitialPosition())
        .isEqualTo(new StartingPoint(InitialPositionInStream.LATEST));
    assertThat(readSpec.getWatermarkPolicyFactory())
        .isEqualTo(WatermarkPolicyFactory.withArrivalTimePolicy());
    assertThat(readSpec.getUpToDateThreshold()).isEqualTo(Duration.ZERO);
    assertThat(readSpec.getMaxCapacityPerShard()).isEqualTo(null);
    assertThat(readSpec.getMaxNumRecords()).isEqualTo(Long.MAX_VALUE);
    assertThat(readSpec.getClientConfiguration()).isEqualTo(ClientConfiguration.builder().build());
  }

  @Test
  public void testReadFromShards() {
    List<List<Record>> records = createRecords(SHARDS, SHARD_EVENTS);
    mockShards(client, SHARDS);
    mockShardIterators(client, records);
    mockRecords(client, records, 10);

    readFromShards(identity(), concat(records));
  }

  @Test
  public void testReadWithEFOFromShards() {
    SubscribeToShardEvent shard0event = eventWithRecords(3);
    SubscribeToShardEvent shard1event = eventWithRecords(3);
    SubscribeToShardEvent shard2event = eventWithRecords(3);
    EFOStubbedKinesisAsyncClient asyncClientStub = new EFOStubbedKinesisAsyncClient(10);
    asyncClientStub.stubSubscribeToShard("0", shard0event);
    asyncClientStub.stubSubscribeToShard("1", shard1event);
    asyncClientStub.stubSubscribeToShard("2", shard1event);
    MockClientBuilderFactory.set(p, KinesisAsyncClientBuilder.class, asyncClientStub);
    Iterable<Record> expectedRecords =
        concat(shard0event.records(), shard1event.records(), shard2event.records());

    mockShards(client, 3);
    Read read =
        KinesisIO.read()
            .withStreamName("stream")
            .withConsumerArn("consumer")
            .withInitialPositionInStream(TRIM_HORIZON)
            .withArrivalTimeWatermarkPolicy()
            .withMaxNumRecords(9);

    PCollection<Record> result = p.apply(read).apply(ParDo.of(new KinesisIOReadTest.ToRecord()));
    PAssert.that(result).containsInAnyOrder(expectedRecords);
    p.run();
  }

  @Test
  public void testReadFromShardsWithLegacyProvider() {
    List<List<Record>> records = createRecords(SHARDS, SHARD_EVENTS);
    mockShards(client, SHARDS);
    mockShardIterators(client, records);
    mockRecords(client, records, 10);

    MockClientBuilderFactory.set(p, KinesisClientBuilder.class, null);
    readFromShards(read -> read.withAWSClientsProvider(Provider.of(client)), concat(records));
  }

  @Test(expected = PipelineExecutionException.class)
  public void testReadWithLimitExceeded() {
    when(client.listShards(any(ListShardsRequest.class)))
        .thenThrow(
            LimitExceededException.builder().message("ListShards rate limit exceeded").build());

    readFromShards(identity(), ImmutableList.of());
  }

  private void readFromShards(Function<Read, Read> fn, Iterable<Record> expected) {
    Read read =
        KinesisIO.read()
            .withStreamName("stream")
            .withInitialPositionInStream(TRIM_HORIZON)
            .withArrivalTimeWatermarkPolicy()
            .withMaxNumRecords(SHARDS * SHARD_EVENTS);

    PCollection<Record> result = p.apply(fn.apply(read)).apply(ParDo.of(new ToRecord()));
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testBuildWithBasicCredentials() {
    Region region = Region.US_EAST_1;
    AwsBasicCredentials credentials = AwsBasicCredentials.create(KEY, SECRET);
    StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);

    Read read = KinesisIO.read().withAWSClientsProvider(KEY, SECRET, region);

    assertThat(read.getClientConfiguration())
        .isEqualTo(ClientConfiguration.create(credentialsProvider, region, null));
  }

  @Test
  public void testBuildWithCredentialsProvider() {
    Region region = Region.US_EAST_1;
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

    Read read = KinesisIO.read().withAWSClientsProvider(credentialsProvider, region);

    assertThat(read.getClientConfiguration())
        .isEqualTo(ClientConfiguration.create(credentialsProvider, region, null));
  }

  @Test
  public void testBuildWithBasicCredentialsAndCustomEndpoint() {
    String customEndpoint = "localhost:9999";
    Region region = Region.US_WEST_1;
    AwsBasicCredentials credentials = AwsBasicCredentials.create("key", "secret");
    StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);

    Read read = KinesisIO.read().withAWSClientsProvider(KEY, SECRET, region, customEndpoint);

    assertThat(read.getClientConfiguration())
        .isEqualTo(
            ClientConfiguration.create(credentialsProvider, region, URI.create(customEndpoint)));
  }

  @Test
  public void testBuildWithCredentialsProviderAndCustomEndpoint() {
    String customEndpoint = "localhost:9999";
    Region region = Region.US_WEST_1;
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

    Read read =
        KinesisIO.read().withAWSClientsProvider(credentialsProvider, region, customEndpoint);

    assertThat(read.getClientConfiguration())
        .isEqualTo(
            ClientConfiguration.create(credentialsProvider, region, URI.create(customEndpoint)));
  }

  static class ToRecord extends DoFn<KinesisRecord, Record> {
    @ProcessElement
    public void processElement(@Element KinesisRecord rec, OutputReceiver<Record> out) {
      Instant arrival = rec.getApproximateArrivalTimestamp();
      out.output(record(arrival, rec.getDataAsBytes(), rec.getSequenceNumber()));
    }
  }

  static class Provider extends StaticSupplier<KinesisClient, Provider>
      implements AWSClientsProvider {
    static AWSClientsProvider of(KinesisClient client) {
      return new Provider().withObject(client);
    }

    @Override
    public KinesisClient getKinesisClient() {
      return get();
    }

    @Override
    public CloudWatchClient getCloudWatchClient() {
      return mock(CloudWatchClient.class);
    }
  }
}
