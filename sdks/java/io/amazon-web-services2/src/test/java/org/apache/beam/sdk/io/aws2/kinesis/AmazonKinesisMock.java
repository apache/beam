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

import static java.lang.Integer.parseInt;
import static java.lang.Math.min;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.transform;
import static org.apache.commons.lang.builder.HashCodeBuilder.reflectionHashCode;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.mockito.Mockito;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.AddTagsToStreamRequest;
import software.amazon.awssdk.services.kinesis.model.AddTagsToStreamResponse;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DecreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.DecreaseStreamRetentionPeriodResponse;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeLimitsRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeLimitsResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.DisableEnhancedMonitoringRequest;
import software.amazon.awssdk.services.kinesis.model.DisableEnhancedMonitoringResponse;
import software.amazon.awssdk.services.kinesis.model.EnableEnhancedMonitoringRequest;
import software.amazon.awssdk.services.kinesis.model.EnableEnhancedMonitoringResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.IncreaseStreamRetentionPeriodResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.ListTagsForStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ListTagsForStreamResponse;
import software.amazon.awssdk.services.kinesis.model.MergeShardsRequest;
import software.amazon.awssdk.services.kinesis.model.MergeShardsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.RemoveTagsFromStreamRequest;
import software.amazon.awssdk.services.kinesis.model.RemoveTagsFromStreamResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.SplitShardRequest;
import software.amazon.awssdk.services.kinesis.model.SplitShardResponse;
import software.amazon.awssdk.services.kinesis.model.StartStreamEncryptionRequest;
import software.amazon.awssdk.services.kinesis.model.StartStreamEncryptionResponse;
import software.amazon.awssdk.services.kinesis.model.StopStreamEncryptionRequest;
import software.amazon.awssdk.services.kinesis.model.StopStreamEncryptionResponse;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountResponse;

/** Mock implementation of {@link KinesisClient} for testing. */
class AmazonKinesisMock implements KinesisClient {

  static class TestData implements Serializable {

    private final String data;
    private final Instant arrivalTimestamp;
    private final String sequenceNumber;

    public TestData(KinesisRecord record) {
      this(
          new String(record.getData().array(), StandardCharsets.UTF_8),
          record.getApproximateArrivalTimestamp(),
          record.getSequenceNumber());
    }

    public TestData(String data, Instant arrivalTimestamp, String sequenceNumber) {
      this.data = data;
      this.arrivalTimestamp = arrivalTimestamp;
      this.sequenceNumber = sequenceNumber;
    }

    public Record convertToRecord() {
      return Record.builder()
          .approximateArrivalTimestamp(TimeUtil.toJava(arrivalTimestamp))
          .data(SdkBytes.fromByteArray(data.getBytes(StandardCharsets.UTF_8)))
          .sequenceNumber(sequenceNumber)
          .partitionKey("")
          .build();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return reflectionHashCode(this);
    }

    @Override
    public String toString() {
      return "TestData{"
          + "data='"
          + data
          + '\''
          + ", arrivalTimestamp="
          + arrivalTimestamp
          + ", sequenceNumber='"
          + sequenceNumber
          + '\''
          + '}';
    }
  }

  static class Provider implements AWSClientsProvider {

    private final List<List<TestData>> shardedData;
    private final int numberOfRecordsPerGet;

    private int rateLimitDescribeStream = 0;

    public Provider(List<List<TestData>> shardedData, int numberOfRecordsPerGet) {
      this.shardedData = shardedData;
      this.numberOfRecordsPerGet = numberOfRecordsPerGet;
    }

    /**
     * Simulate an initially rate limited DescribeStream.
     *
     * @param rateLimitDescribeStream The number of rate limited requests before success
     */
    public Provider withRateLimitedDescribeStream(int rateLimitDescribeStream) {
      this.rateLimitDescribeStream = rateLimitDescribeStream;
      return this;
    }

    @Override
    public KinesisClient getKinesisClient() {
      return new AmazonKinesisMock(
              shardedData.stream()
                  .map(testDatas -> transform(testDatas, TestData::convertToRecord))
                  .collect(Collectors.toList()),
              numberOfRecordsPerGet)
          .withRateLimitedDescribeStream(rateLimitDescribeStream);
    }

    @Override
    public CloudWatchClient getCloudWatchClient() {
      return Mockito.mock(CloudWatchClient.class);
    }
  }

  private final List<List<Record>> shardedData;
  private final int numberOfRecordsPerGet;

  private int rateLimitDescribeStream = 0;

  public AmazonKinesisMock(List<List<Record>> shardedData, int numberOfRecordsPerGet) {
    this.shardedData = shardedData;
    this.numberOfRecordsPerGet = numberOfRecordsPerGet;
  }

  public AmazonKinesisMock withRateLimitedDescribeStream(int rateLimitDescribeStream) {
    this.rateLimitDescribeStream = rateLimitDescribeStream;
    return this;
  }

  @Override
  public String serviceName() {
    return null;
  }

  @Override
  public void close() {}

  @Override
  public GetRecordsResponse getRecords(GetRecordsRequest getRecordsRequest) {
    List<String> shardIteratorParts =
        Splitter.on(':').splitToList(getRecordsRequest.shardIterator());
    int shardId = parseInt(shardIteratorParts.get(0));
    int startingRecord = parseInt(shardIteratorParts.get(1));
    List<Record> shardData = shardedData.get(shardId);

    int toIndex = min(startingRecord + numberOfRecordsPerGet, shardData.size());
    int fromIndex = min(startingRecord, toIndex);
    return GetRecordsResponse.builder()
        .records(shardData.subList(fromIndex, toIndex))
        .nextShardIterator(String.format("%s:%s", shardId, toIndex))
        .millisBehindLatest(0L)
        .build();
  }

  @Override
  public GetShardIteratorResponse getShardIterator(
      GetShardIteratorRequest getShardIteratorRequest) {
    ShardIteratorType shardIteratorType = getShardIteratorRequest.shardIteratorType();

    String shardIterator;
    if (shardIteratorType == ShardIteratorType.TRIM_HORIZON) {
      shardIterator = String.format("%s:%s", getShardIteratorRequest.shardId(), 0);
    } else {
      throw new RuntimeException("Not implemented");
    }

    return GetShardIteratorResponse.builder().shardIterator(shardIterator).build();
  }

  @Override
  public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest) {
    if (rateLimitDescribeStream-- > 0) {
      throw LimitExceededException.builder().message("DescribeStream rate limit exceeded").build();
    }
    int nextShardId = 0;
    if (describeStreamRequest.exclusiveStartShardId() != null) {
      nextShardId = parseInt(describeStreamRequest.exclusiveStartShardId()) + 1;
    }
    boolean hasMoreShards = nextShardId + 1 < shardedData.size();

    List<Shard> shards = new ArrayList<>();
    if (nextShardId < shardedData.size()) {
      shards.add(Shard.builder().shardId(Integer.toString(nextShardId)).build());
    }

    DescribeStreamResponse.Builder builder =
        DescribeStreamResponse.builder()
            .streamDescription(
                s ->
                    s.hasMoreShards(hasMoreShards)
                        .shards(shards)
                        .streamName(describeStreamRequest.streamName()));
    builder.sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build());
    return builder.build();
  }

  @Override
  public AddTagsToStreamResponse addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public CreateStreamResponse createStream(CreateStreamRequest createStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DecreaseStreamRetentionPeriodResponse decreaseStreamRetentionPeriod(
      DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DeleteStreamResponse deleteStream(DeleteStreamRequest deleteStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DescribeLimitsResponse describeLimits(DescribeLimitsRequest describeLimitsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DescribeStreamConsumerResponse describeStreamConsumer(
      DescribeStreamConsumerRequest describeStreamConsumerRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DescribeStreamSummaryResponse describeStreamSummary(
      DescribeStreamSummaryRequest describeStreamSummaryRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DisableEnhancedMonitoringResponse disableEnhancedMonitoring(
      DisableEnhancedMonitoringRequest disableEnhancedMonitoringRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public EnableEnhancedMonitoringResponse enableEnhancedMonitoring(
      EnableEnhancedMonitoringRequest enableEnhancedMonitoringRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public IncreaseStreamRetentionPeriodResponse increaseStreamRetentionPeriod(
      IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListShardsResponse listShards(ListShardsRequest listShardsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListStreamConsumersResponse listStreamConsumers(
      ListStreamConsumersRequest listStreamConsumersRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListStreamsResponse listStreams(ListStreamsRequest listStreamsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListStreamsResponse listStreams() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListTagsForStreamResponse listTagsForStream(
      ListTagsForStreamRequest listTagsForStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public MergeShardsResponse mergeShards(MergeShardsRequest mergeShardsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public PutRecordResponse putRecord(PutRecordRequest putRecordRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public PutRecordsResponse putRecords(PutRecordsRequest putRecordsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public RegisterStreamConsumerResponse registerStreamConsumer(
      RegisterStreamConsumerRequest registerStreamConsumerRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public RemoveTagsFromStreamResponse removeTagsFromStream(
      RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SplitShardResponse splitShard(SplitShardRequest splitShardRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public StartStreamEncryptionResponse startStreamEncryption(
      StartStreamEncryptionRequest startStreamEncryptionRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public StopStreamEncryptionResponse stopStreamEncryption(
      StopStreamEncryptionRequest stopStreamEncryptionRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public UpdateShardCountResponse updateShardCount(
      UpdateShardCountRequest updateShardCountRequest) {
    throw new RuntimeException("Not implemented");
  }
}
