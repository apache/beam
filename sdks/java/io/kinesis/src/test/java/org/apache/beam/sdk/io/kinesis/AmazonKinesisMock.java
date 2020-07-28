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

import static java.lang.Integer.parseInt;
import static java.lang.Math.min;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.transform;
import static org.apache.commons.lang.builder.HashCodeBuilder.reflectionHashCode;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.AddTagsToStreamRequest;
import com.amazonaws.services.kinesis.model.AddTagsToStreamResult;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamResult;
import com.amazonaws.services.kinesis.model.DeregisterStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.DeregisterStreamConsumerResult;
import com.amazonaws.services.kinesis.model.DescribeLimitsRequest;
import com.amazonaws.services.kinesis.model.DescribeLimitsResult;
import com.amazonaws.services.kinesis.model.DescribeStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamConsumerResult;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryResult;
import com.amazonaws.services.kinesis.model.DisableEnhancedMonitoringRequest;
import com.amazonaws.services.kinesis.model.DisableEnhancedMonitoringResult;
import com.amazonaws.services.kinesis.model.EnableEnhancedMonitoringRequest;
import com.amazonaws.services.kinesis.model.EnableEnhancedMonitoringResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.IncreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ListStreamConsumersRequest;
import com.amazonaws.services.kinesis.model.ListStreamConsumersResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ListTagsForStreamRequest;
import com.amazonaws.services.kinesis.model.ListTagsForStreamResult;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.MergeShardsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.RegisterStreamConsumerRequest;
import com.amazonaws.services.kinesis.model.RegisterStreamConsumerResult;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamRequest;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.SplitShardRequest;
import com.amazonaws.services.kinesis.model.SplitShardResult;
import com.amazonaws.services.kinesis.model.StartStreamEncryptionRequest;
import com.amazonaws.services.kinesis.model.StartStreamEncryptionResult;
import com.amazonaws.services.kinesis.model.StopStreamEncryptionRequest;
import com.amazonaws.services.kinesis.model.StopStreamEncryptionResult;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.UpdateShardCountRequest;
import com.amazonaws.services.kinesis.model.UpdateShardCountResult;
import com.amazonaws.services.kinesis.producer.IKinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.waiters.AmazonKinesisWaiters;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.mockito.Mockito;

/** Mock implemenation of {@link AmazonKinesis} for testing. */
class AmazonKinesisMock implements AmazonKinesis {

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
      return new Record()
          .withApproximateArrivalTimestamp(arrivalTimestamp.toDate())
          .withData(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)))
          .withSequenceNumber(sequenceNumber)
          .withPartitionKey("");
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
    public AmazonKinesis getKinesisClient() {
      return new AmazonKinesisMock(
              shardedData.stream()
                  .map(testDatas -> transform(testDatas, TestData::convertToRecord))
                  .collect(Collectors.toList()),
              numberOfRecordsPerGet)
          .withRateLimitedDescribeStream(rateLimitDescribeStream);
    }

    @Override
    public AmazonCloudWatch getCloudWatchClient() {
      return Mockito.mock(AmazonCloudWatch.class);
    }

    @Override
    public IKinesisProducer createKinesisProducer(KinesisProducerConfiguration config) {
      throw new RuntimeException("Not implemented");
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
  public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
    List<String> shardIteratorParts =
        Splitter.on(':').splitToList(getRecordsRequest.getShardIterator());
    int shardId = parseInt(shardIteratorParts.get(0));
    int startingRecord = parseInt(shardIteratorParts.get(1));
    List<Record> shardData = shardedData.get(shardId);

    int toIndex = min(startingRecord + numberOfRecordsPerGet, shardData.size());
    int fromIndex = min(startingRecord, toIndex);
    return new GetRecordsResult()
        .withRecords(shardData.subList(fromIndex, toIndex))
        .withNextShardIterator(String.format("%s:%s", shardId, toIndex))
        .withMillisBehindLatest(0L);
  }

  @Override
  public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
    ShardIteratorType shardIteratorType =
        ShardIteratorType.fromValue(getShardIteratorRequest.getShardIteratorType());

    String shardIterator;
    if (shardIteratorType == ShardIteratorType.TRIM_HORIZON) {
      shardIterator = String.format("%s:%s", getShardIteratorRequest.getShardId(), 0);
    } else {
      throw new RuntimeException("Not implemented");
    }

    return new GetShardIteratorResult().withShardIterator(shardIterator);
  }

  @Override
  public DescribeStreamResult describeStream(String streamName, String exclusiveStartShardId) {
    if (rateLimitDescribeStream-- > 0) {
      throw new LimitExceededException("DescribeStream rate limit exceeded");
    }
    int nextShardId = 0;
    if (exclusiveStartShardId != null) {
      nextShardId = parseInt(exclusiveStartShardId) + 1;
    }
    boolean hasMoreShards = nextShardId + 1 < shardedData.size();

    List<Shard> shards = new ArrayList<>();
    if (nextShardId < shardedData.size()) {
      shards.add(new Shard().withShardId(Integer.toString(nextShardId)));
    }

    HttpResponse response = new HttpResponse(null, null);
    response.setStatusCode(200);
    DescribeStreamResult result = new DescribeStreamResult();
    result.setSdkHttpMetadata(SdkHttpMetadata.from(response));
    result.withStreamDescription(
        new StreamDescription()
            .withHasMoreShards(hasMoreShards)
            .withShards(shards)
            .withStreamName(streamName));
    return result;
  }

  @Override
  public void setEndpoint(String endpoint) {}

  @Override
  public void setRegion(Region region) {}

  @Override
  public AddTagsToStreamResult addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public CreateStreamResult createStream(CreateStreamRequest createStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public CreateStreamResult createStream(String streamName, Integer shardCount) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DecreaseStreamRetentionPeriodResult decreaseStreamRetentionPeriod(
      DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DeleteStreamResult deleteStream(DeleteStreamRequest deleteStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DeleteStreamResult deleteStream(String streamName) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DeregisterStreamConsumerResult deregisterStreamConsumer(
      DeregisterStreamConsumerRequest deregisterStreamConsumerRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DescribeLimitsResult describeLimits(DescribeLimitsRequest describeLimitsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DescribeStreamResult describeStream(String streamName) {
    return describeStream(streamName, null);
  }

  @Override
  public DescribeStreamResult describeStream(
      String streamName, Integer limit, String exclusiveStartShardId) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DescribeStreamConsumerResult describeStreamConsumer(
      DescribeStreamConsumerRequest describeStreamConsumerRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DescribeStreamSummaryResult describeStreamSummary(
      DescribeStreamSummaryRequest describeStreamSummaryRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DisableEnhancedMonitoringResult disableEnhancedMonitoring(
      DisableEnhancedMonitoringRequest disableEnhancedMonitoringRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public EnableEnhancedMonitoringResult enableEnhancedMonitoring(
      EnableEnhancedMonitoringRequest enableEnhancedMonitoringRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GetShardIteratorResult getShardIterator(
      String streamName, String shardId, String shardIteratorType) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GetShardIteratorResult getShardIterator(
      String streamName, String shardId, String shardIteratorType, String startingSequenceNumber) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(
      IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListShardsResult listShards(ListShardsRequest listShardsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListStreamConsumersResult listStreamConsumers(
      ListStreamConsumersRequest listStreamConsumersRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListStreamsResult listStreams() {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListStreamsResult listStreams(String exclusiveStartStreamName) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListStreamsResult listStreams(Integer limit, String exclusiveStartStreamName) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public ListTagsForStreamResult listTagsForStream(
      ListTagsForStreamRequest listTagsForStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public MergeShardsResult mergeShards(MergeShardsRequest mergeShardsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public MergeShardsResult mergeShards(
      String streamName, String shardToMerge, String adjacentShardToMerge) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public PutRecordResult putRecord(PutRecordRequest putRecordRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public PutRecordResult putRecord(String streamName, ByteBuffer data, String partitionKey) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public PutRecordResult putRecord(
      String streamName, ByteBuffer data, String partitionKey, String sequenceNumberForOrdering) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public PutRecordsResult putRecords(PutRecordsRequest putRecordsRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public RegisterStreamConsumerResult registerStreamConsumer(
      RegisterStreamConsumerRequest registerStreamConsumerRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public RemoveTagsFromStreamResult removeTagsFromStream(
      RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SplitShardResult splitShard(SplitShardRequest splitShardRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public SplitShardResult splitShard(
      String streamName, String shardToSplit, String newStartingHashKey) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public StartStreamEncryptionResult startStreamEncryption(
      StartStreamEncryptionRequest startStreamEncryptionRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public StopStreamEncryptionResult stopStreamEncryption(
      StopStreamEncryptionRequest stopStreamEncryptionRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public UpdateShardCountResult updateShardCount(UpdateShardCountRequest updateShardCountRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void shutdown() {}

  @Override
  public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public AmazonKinesisWaiters waiters() {
    throw new RuntimeException("Not implemented");
  }
}
