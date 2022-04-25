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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.concat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.Duration.standardSeconds;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON;

import java.net.URI;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
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
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;

/** Tests for {@link KinesisIO#read}. */
@RunWith(MockitoJUnitRunner.class)
public class KinesisIOReadTest {
  private static final String KEY = "key";
  private static final String SECRET = "secret";

  private static final int SHARDS = 3;
  private static final int SHARD_EVENTS = 100;

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Mock public KinesisClient client;

  @Before
  public void configureClientBuilderFactory() {
    MockClientBuilderFactory.set(p, KinesisClientBuilder.class, client);
    MockClientBuilderFactory.set(p, CloudWatchClientBuilder.class, mock(CloudWatchClient.class));
  }

  @Test
  public void testReadFromShards() {
    List<List<Record>> records = testRecords(SHARDS, SHARD_EVENTS);
    mockShards(SHARDS);
    mockShardIterators(records);
    mockRecords(records, 10);

    readFromShards(identity(), concat(records));
  }

  @Test
  public void testReadFromShardsWithLegacyProvider() {
    List<List<Record>> records = testRecords(SHARDS, SHARD_EVENTS);
    mockShards(SHARDS);
    mockShardIterators(records);
    mockRecords(records, 10);

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

  private static ArgumentMatcher<GetShardIteratorRequest> hasShardId(int id) {
    return req -> req != null && req.shardId().equals("" + id);
  }

  private static ArgumentMatcher<GetRecordsRequest> hasShardIterator(String id) {
    return req -> req != null && req.shardIterator().equals(id);
  }

  private void mockShardIterators(List<List<Record>> data) {
    for (int id = 0; id < data.size(); id++) {
      when(client.getShardIterator(argThat(hasShardId(id))))
          .thenReturn(GetShardIteratorResponse.builder().shardIterator(id + ":0").build());
    }
  }

  private void mockRecords(List<List<Record>> data, int limit) {
    BiFunction<List<Record>, String, GetRecordsResponse.Builder> resp =
        (recs, it) ->
            GetRecordsResponse.builder().millisBehindLatest(0L).records(recs).nextShardIterator(it);

    for (int shard = 0; shard < data.size(); shard++) {
      List<Record> records = data.get(shard);
      for (int i = 0; i < records.size(); i += limit) {
        int to = Math.max(i + limit, records.size());
        String nextIt = (to == records.size()) ? "done" : shard + ":" + to;
        when(client.getRecords(argThat(hasShardIterator(shard + ":" + i))))
            .thenReturn(resp.apply(records.subList(i, to), nextIt).build());
      }
    }
    when(client.getRecords(argThat(hasShardIterator("done"))))
        .thenReturn(resp.apply(ImmutableList.of(), "done").build());
  }

  private void mockShards(int count) {
    IntFunction<Shard> shard = i -> Shard.builder().shardId(Integer.toString(i)).build();
    List<Shard> shards = range(0, count).mapToObj(shard).collect(toList());
    when(client.listShards(any(ListShardsRequest.class)))
        .thenReturn(ListShardsResponse.builder().shards(shards).build());
  }

  private List<List<Record>> testRecords(int shards, int events) {
    final Instant now = DateTime.now().toInstant();
    Function<Integer, List<Record>> dataStream =
        shard -> range(0, events).mapToObj(off -> record(now, shard, off)).collect(toList());
    return range(0, shards).boxed().map(dataStream).collect(toList());
  }

  private static Record record(Instant now, int shard, int offset) {
    String seqNum = Integer.toString(shard * SHARD_EVENTS + offset);
    return record(now.plus(standardSeconds(offset)), seqNum.getBytes(UTF_8), seqNum);
  }

  private static Record record(Instant arrival, byte[] data, String seqNum) {
    return Record.builder()
        .approximateArrivalTimestamp(TimeUtil.toJava(arrival))
        .data(SdkBytes.fromByteArray(data))
        .sequenceNumber(seqNum)
        .partitionKey("")
        .build();
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
