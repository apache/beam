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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import static java.lang.Integer.parseInt;
import static java.lang.Math.min;
import static org.apache.commons.lang.builder.HashCodeBuilder.reflectionHashCode;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.AddTagsToStreamRequest;
import com.amazonaws.services.kinesis.model.AddTagsToStreamResult;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
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
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamRequest;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.SplitShardRequest;
import com.amazonaws.services.kinesis.model.SplitShardResult;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.google.common.base.Function;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.joda.time.Instant;

/**
 * Mock implemenation of {@link AmazonKinesis} for testing.
 */
class AmazonKinesisMock implements AmazonKinesis {

  static class TestData implements Serializable {

    private final String data;
    private final Instant arrivalTimestamp;
    private final String sequenceNumber;

    public TestData(KinesisRecord record) {
      this(new String(record.getData().array()),
          record.getApproximateArrivalTimestamp(),
          record.getSequenceNumber());
    }

    public TestData(String data, Instant arrivalTimestamp, String sequenceNumber) {
      this.data = data;
      this.arrivalTimestamp = arrivalTimestamp;
      this.sequenceNumber = sequenceNumber;
    }

    public Record convertToRecord() {
      return new Record().
          withApproximateArrivalTimestamp(arrivalTimestamp.toDate()).
          withData(ByteBuffer.wrap(data.getBytes())).
          withSequenceNumber(sequenceNumber).
          withPartitionKey("");
    }

    @Override
    public boolean equals(Object obj) {
      return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return reflectionHashCode(this);
    }
  }

  static class Provider implements KinesisClientProvider {

    private final List<List<TestData>> shardedData;
    private final int numberOfRecordsPerGet;

    public Provider(List<List<TestData>> shardedData, int numberOfRecordsPerGet) {
      this.shardedData = shardedData;
      this.numberOfRecordsPerGet = numberOfRecordsPerGet;
    }

    @Override
    public AmazonKinesis get() {
      return new AmazonKinesisMock(transform(shardedData,
          new Function<List<TestData>, List<Record>>() {

            @Override
            public List<Record> apply(@Nullable List<TestData> testDatas) {
              return transform(testDatas, new Function<TestData, Record>() {

                @Override
                public Record apply(@Nullable TestData testData) {
                  return testData.convertToRecord();
                }
              });
            }
          }), numberOfRecordsPerGet);
    }
  }

  private final List<List<Record>> shardedData;
  private final int numberOfRecordsPerGet;

  public AmazonKinesisMock(List<List<Record>> shardedData, int numberOfRecordsPerGet) {
    this.shardedData = shardedData;
    this.numberOfRecordsPerGet = numberOfRecordsPerGet;
  }

  @Override
  public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
    String[] shardIteratorParts = getRecordsRequest.getShardIterator().split(":");
    int shardId = parseInt(shardIteratorParts[0]);
    int startingRecord = parseInt(shardIteratorParts[1]);
    List<Record> shardData = shardedData.get(shardId);

    int toIndex = min(startingRecord + numberOfRecordsPerGet, shardData.size());
    int fromIndex = min(startingRecord, toIndex);
    return new GetRecordsResult().
        withRecords(shardData.subList(fromIndex, toIndex)).
        withNextShardIterator(String.format("%s:%s", shardId, toIndex));
  }

  @Override
  public GetShardIteratorResult getShardIterator(
      GetShardIteratorRequest getShardIteratorRequest) {
    ShardIteratorType shardIteratorType = ShardIteratorType.fromValue(
        getShardIteratorRequest.getShardIteratorType());

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
    int nextShardId = 0;
    if (exclusiveStartShardId != null) {
      nextShardId = parseInt(exclusiveStartShardId) + 1;
    }
    boolean hasMoreShards = nextShardId + 1 < shardedData.size();

    List<Shard> shards = newArrayList();
    if (nextShardId < shardedData.size()) {
      shards.add(new Shard().withShardId(Integer.toString(nextShardId)));
    }

    return new DescribeStreamResult().withStreamDescription(
        new StreamDescription().withHasMoreShards(hasMoreShards).withShards(shards)
    );
  }

  @Override
  public void setEndpoint(String endpoint) {

  }

  @Override
  public void setRegion(Region region) {

  }

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
  public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public DescribeStreamResult describeStream(String streamName) {

    throw new RuntimeException("Not implemented");
  }

  @Override
  public DescribeStreamResult describeStream(String streamName,
      Integer limit, String exclusiveStartShardId) {
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
  public GetShardIteratorResult getShardIterator(String streamName,
      String shardId,
      String shardIteratorType) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public GetShardIteratorResult getShardIterator(String streamName,
      String shardId,
      String shardIteratorType,
      String startingSequenceNumber) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(
      IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
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
  public MergeShardsResult mergeShards(String streamName,
      String shardToMerge, String adjacentShardToMerge) {
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
  public PutRecordResult putRecord(String streamName, ByteBuffer data,
      String partitionKey, String sequenceNumberForOrdering) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public PutRecordsResult putRecords(PutRecordsRequest putRecordsRequest) {
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
  public SplitShardResult splitShard(String streamName,
      String shardToSplit, String newStartingHashKey) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public void shutdown() {

  }

  @Override
  public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    throw new RuntimeException("Not implemented");
  }
}
