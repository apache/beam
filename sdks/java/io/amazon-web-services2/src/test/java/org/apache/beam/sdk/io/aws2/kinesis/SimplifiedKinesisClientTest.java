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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Minutes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Datapoint;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsResponse;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.InitialPositionInStream;

/** * */
@RunWith(MockitoJUnitRunner.class)
public class SimplifiedKinesisClientTest {

  private static final String STREAM = "stream";
  private static final String SHARD_1 = "shard-01";
  private static final String SHARD_2 = "shard-02";
  private static final String SHARD_3 = "shard-03";
  private static final String SHARD_ITERATOR = "iterator";
  private static final String SEQUENCE_NUMBER = "abc123";
  private static final Instant CURRENT_TIMESTAMP = Instant.parse("2000-01-01T15:00:00.000Z");

  @Mock private KinesisClient kinesis;
  @Mock private CloudWatchClient cloudWatch;
  @Mock private Supplier<Instant> currentInstantSupplier;
  @InjectMocks private SimplifiedKinesisClient underTest;

  @Test
  public void shouldReturnIteratorStartingWithSequenceNumber() throws Exception {
    when(kinesis.getShardIterator(
            GetShardIteratorRequest.builder()
                .streamName(STREAM)
                .shardId(SHARD_1)
                .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                .startingSequenceNumber(SEQUENCE_NUMBER)
                .build()))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator(SHARD_ITERATOR).build());

    String stream =
        underTest.getShardIterator(
            STREAM, SHARD_1, ShardIteratorType.AT_SEQUENCE_NUMBER, SEQUENCE_NUMBER, null);

    assertThat(stream).isEqualTo(SHARD_ITERATOR);
  }

  @Test
  public void shouldReturnIteratorStartingtimestamp() throws Exception {
    Instant timestamp = Instant.now();
    when(kinesis.getShardIterator(
            GetShardIteratorRequest.builder()
                .streamName(STREAM)
                .shardId(SHARD_1)
                .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                .timestamp(TimeUtil.toJava(timestamp))
                .build()))
        .thenReturn(GetShardIteratorResponse.builder().shardIterator(SHARD_ITERATOR).build());

    String stream =
        underTest.getShardIterator(
            STREAM, SHARD_1, ShardIteratorType.AT_SEQUENCE_NUMBER, null, timestamp);

    assertThat(stream).isEqualTo(SHARD_ITERATOR);
  }

  @Test
  public void shouldHandleExpiredIterationExceptionForGetShardIterator() {
    shouldHandleGetShardIteratorError(
        ExpiredIteratorException.builder().build(), ExpiredIteratorException.class);
  }

  @Test
  public void shouldHandleLimitExceededExceptionForGetShardIterator() {
    shouldHandleGetShardIteratorError(
        LimitExceededException.builder().build(), KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleProvisionedThroughputExceededExceptionForGetShardIterator() {
    shouldHandleGetShardIteratorError(
        ProvisionedThroughputExceededException.builder().build(),
        KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleServiceErrorForGetShardIterator() {
    shouldHandleGetShardIteratorError(
        SdkServiceException.builder().build(), TransientKinesisException.class);
  }

  @Test
  public void shouldHandleClientErrorForGetShardIterator() {
    shouldHandleGetShardIteratorError(SdkClientException.builder().build(), RuntimeException.class);
  }

  @Test
  public void shouldHandleUnexpectedExceptionForGetShardIterator() {
    shouldHandleGetShardIteratorError(new NullPointerException(), RuntimeException.class);
  }

  private void shouldHandleGetShardIteratorError(
      Exception thrownException, Class<? extends Exception> expectedExceptionClass) {
    GetShardIteratorRequest request =
        GetShardIteratorRequest.builder()
            .streamName(STREAM)
            .shardId(SHARD_1)
            .shardIteratorType(ShardIteratorType.LATEST)
            .build();

    when(kinesis.getShardIterator(request)).thenThrow(thrownException);

    try {
      underTest.getShardIterator(STREAM, SHARD_1, ShardIteratorType.LATEST, null, null);
      failBecauseExceptionWasNotThrown(expectedExceptionClass);
    } catch (Exception e) {
      assertThat(e).isExactlyInstanceOf(expectedExceptionClass);
    } finally {
      reset(kinesis);
    }
  }

  @Test
  public void shouldListAllShardsForTrimHorizon() throws Exception {
    Shard shard1 = Shard.builder().shardId(SHARD_1).build();
    Shard shard2 = Shard.builder().shardId(SHARD_2).build();
    Shard shard3 = Shard.builder().shardId(SHARD_3).build();
    ShardFilter shardFilter = ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();

    when(kinesis.listShards(
            ListShardsRequest.builder()
                .streamName(STREAM)
                .shardFilter(shardFilter)
                .maxResults(1_000)
                .build()))
        .thenReturn(
            ListShardsResponse.builder().shards(shard1, shard2, shard3).nextToken(null).build());

    List<Shard> shards =
        underTest.listShardsAtPoint(
            STREAM, new StartingPoint(InitialPositionInStream.TRIM_HORIZON));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForTrimHorizonWithPagedResults() throws Exception {
    Shard shard1 = Shard.builder().shardId(SHARD_1).build();
    Shard shard2 = Shard.builder().shardId(SHARD_2).build();
    Shard shard3 = Shard.builder().shardId(SHARD_3).build();

    ShardFilter shardFilter = ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();

    String nextListShardsToken = "testNextToken";
    when(kinesis.listShards(
            ListShardsRequest.builder()
                .streamName(STREAM)
                .shardFilter(shardFilter)
                .maxResults(1_000)
                .build()))
        .thenReturn(
            ListShardsResponse.builder()
                .shards(shard1, shard2)
                .nextToken(nextListShardsToken)
                .build());

    when(kinesis.listShards(
            ListShardsRequest.builder()
                .maxResults(1_000)
                .shardFilter(shardFilter)
                .nextToken(nextListShardsToken)
                .build()))
        .thenReturn(ListShardsResponse.builder().shards(shard3).nextToken(null).build());

    List<Shard> shards =
        underTest.listShardsAtPoint(
            STREAM, new StartingPoint(InitialPositionInStream.TRIM_HORIZON));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForTimestampWithinStreamRetentionAfterStreamCreationTimestamp()
      throws Exception {
    Shard shard1 = Shard.builder().shardId(SHARD_1).build();
    Shard shard2 = Shard.builder().shardId(SHARD_2).build();
    Shard shard3 = Shard.builder().shardId(SHARD_3).build();

    int hoursDifference = 1;
    int retentionPeriodHours = hoursDifference * 3;
    Instant streamCreationTimestamp =
        CURRENT_TIMESTAMP.minus(Duration.standardHours(retentionPeriodHours));
    Instant startingPointTimestamp =
        streamCreationTimestamp.plus(Duration.standardHours(hoursDifference));

    when(currentInstantSupplier.get()).thenReturn(CURRENT_TIMESTAMP);

    when(kinesis.describeStreamSummary(
            DescribeStreamSummaryRequest.builder().streamName(STREAM).build()))
        .thenReturn(
            DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(
                    StreamDescriptionSummary.builder()
                        .retentionPeriodHours(retentionPeriodHours)
                        .streamCreationTimestamp(TimeUtil.toJava(streamCreationTimestamp))
                        .build())
                .build());

    ShardFilter shardFilter =
        ShardFilter.builder()
            .type(ShardFilterType.AT_TIMESTAMP)
            .timestamp(TimeUtil.toJava(startingPointTimestamp))
            .build();

    when(kinesis.listShards(
            ListShardsRequest.builder()
                .streamName(STREAM)
                .shardFilter(shardFilter)
                .maxResults(1_000)
                .build()))
        .thenReturn(
            ListShardsResponse.builder().shards(shard1, shard2, shard3).nextToken(null).build());

    List<Shard> shards =
        underTest.listShardsAtPoint(STREAM, new StartingPoint(startingPointTimestamp));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void
      shouldListAllShardsForTimestampWithRetriedDescribeStreamSummaryCallAfterStreamCreationTimestamp()
          throws TransientKinesisException {
    Shard shard1 = Shard.builder().shardId(SHARD_1).build();
    Shard shard2 = Shard.builder().shardId(SHARD_2).build();
    Shard shard3 = Shard.builder().shardId(SHARD_3).build();

    int hoursDifference = 1;
    int retentionPeriodHours = hoursDifference * 3;
    Instant streamCreationTimestamp =
        CURRENT_TIMESTAMP.minus(Duration.standardHours(retentionPeriodHours));
    Instant startingPointTimestamp =
        streamCreationTimestamp.plus(Duration.standardHours(hoursDifference));

    when(currentInstantSupplier.get()).thenReturn(CURRENT_TIMESTAMP);

    when(kinesis.describeStreamSummary(
            DescribeStreamSummaryRequest.builder().streamName(STREAM).build()))
        .thenThrow(
            LimitExceededException.builder().message("Fake Exception: Limit exceeded").build())
        .thenReturn(
            DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(
                    StreamDescriptionSummary.builder()
                        .retentionPeriodHours(retentionPeriodHours)
                        .streamCreationTimestamp(TimeUtil.toJava(streamCreationTimestamp))
                        .build())
                .build());

    ShardFilter shardFilter =
        ShardFilter.builder()
            .type(ShardFilterType.AT_TIMESTAMP)
            .timestamp(TimeUtil.toJava(startingPointTimestamp))
            .build();

    when(kinesis.listShards(
            ListShardsRequest.builder()
                .streamName(STREAM)
                .shardFilter(shardFilter)
                .maxResults(1_000)
                .build()))
        .thenReturn(
            ListShardsResponse.builder().shards(shard1, shard2, shard3).nextToken(null).build());

    List<Shard> shards =
        underTest.listShardsAtPoint(STREAM, new StartingPoint(startingPointTimestamp));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForTimestampOutsideStreamRetentionAfterStreamCreationTimestamp()
      throws Exception {
    Shard shard1 = Shard.builder().shardId(SHARD_1).build();
    Shard shard2 = Shard.builder().shardId(SHARD_2).build();
    Shard shard3 = Shard.builder().shardId(SHARD_3).build();

    int retentionPeriodHours = 3;
    int startingPointHours = 5;
    int hoursSinceStreamCreation = 6;

    Instant streamCreationTimestamp =
        CURRENT_TIMESTAMP.minus(Duration.standardHours(hoursSinceStreamCreation));
    Instant startingPointTimestampAfterStreamRetentionTimestamp =
        CURRENT_TIMESTAMP.minus(Duration.standardHours(startingPointHours));

    when(currentInstantSupplier.get()).thenReturn(CURRENT_TIMESTAMP);

    DescribeStreamSummaryRequest describeStreamRequest =
        DescribeStreamSummaryRequest.builder().streamName(STREAM).build();
    when(kinesis.describeStreamSummary(describeStreamRequest))
        .thenReturn(
            DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(
                    StreamDescriptionSummary.builder()
                        .retentionPeriodHours(retentionPeriodHours)
                        .streamCreationTimestamp(TimeUtil.toJava(streamCreationTimestamp))
                        .build())
                .build());

    ShardFilter shardFilter = ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();

    when(kinesis.listShards(
            ListShardsRequest.builder()
                .streamName(STREAM)
                .shardFilter(shardFilter)
                .maxResults(1_000)
                .build()))
        .thenReturn(
            ListShardsResponse.builder().shards(shard1, shard2, shard3).nextToken(null).build());

    List<Shard> shards =
        underTest.listShardsAtPoint(
            STREAM, new StartingPoint(startingPointTimestampAfterStreamRetentionTimestamp));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForTimestampBeforeStreamCreationTimestamp() throws Exception {
    Shard shard1 = Shard.builder().shardId(SHARD_1).build();
    Shard shard2 = Shard.builder().shardId(SHARD_2).build();
    Shard shard3 = Shard.builder().shardId(SHARD_3).build();

    Instant startingPointTimestamp = Instant.parse("2000-01-01T15:00:00.000Z");
    Instant streamCreationTimestamp = startingPointTimestamp.plus(Duration.standardHours(1));

    DescribeStreamSummaryRequest describeStreamRequest =
        DescribeStreamSummaryRequest.builder().streamName(STREAM).build();
    when(kinesis.describeStreamSummary(describeStreamRequest))
        .thenReturn(
            DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(
                    StreamDescriptionSummary.builder()
                        .streamCreationTimestamp(TimeUtil.toJava(streamCreationTimestamp))
                        .build())
                .build());

    ShardFilter shardFilter = ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();

    when(kinesis.listShards(
            ListShardsRequest.builder()
                .streamName(STREAM)
                .shardFilter(shardFilter)
                .maxResults(1_000)
                .build()))
        .thenReturn(
            ListShardsResponse.builder().shards(shard1, shard2, shard3).nextToken(null).build());

    List<Shard> shards =
        underTest.listShardsAtPoint(STREAM, new StartingPoint(startingPointTimestamp));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForLatest() throws Exception {
    Shard shard1 = Shard.builder().shardId(SHARD_1).build();
    Shard shard2 = Shard.builder().shardId(SHARD_2).build();
    Shard shard3 = Shard.builder().shardId(SHARD_3).build();

    when(kinesis.listShards(
            ListShardsRequest.builder()
                .streamName(STREAM)
                .shardFilter(ShardFilter.builder().type(ShardFilterType.AT_LATEST).build())
                .maxResults(1_000)
                .build()))
        .thenReturn(
            ListShardsResponse.builder().shards(shard1, shard2, shard3).nextToken(null).build());

    List<Shard> shards =
        underTest.listShardsAtPoint(STREAM, new StartingPoint(InitialPositionInStream.LATEST));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForExclusiveStartShardId() throws Exception {
    Shard shard1 = Shard.builder().shardId(SHARD_1).build();
    Shard shard2 = Shard.builder().shardId(SHARD_2).build();
    Shard shard3 = Shard.builder().shardId(SHARD_3).build();

    String exclusiveStartShardId = "exclusiveStartShardId";

    when(kinesis.listShards(
            ListShardsRequest.builder()
                .streamName(STREAM)
                .maxResults(1_000)
                .shardFilter(
                    ShardFilter.builder()
                        .type(ShardFilterType.AFTER_SHARD_ID)
                        .shardId(exclusiveStartShardId)
                        .build())
                .build()))
        .thenReturn(
            ListShardsResponse.builder().shards(shard1, shard2, shard3).nextToken(null).build());

    List<Shard> shards = underTest.listShardsFollowingClosedShard(STREAM, exclusiveStartShardId);

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldHandleExpiredIterationExceptionForShardListing() {
    shouldHandleShardListingError(
        ExpiredIteratorException.builder().build(), ExpiredIteratorException.class);
  }

  @Test
  public void shouldHandleLimitExceededExceptionForShardListing() {
    shouldHandleShardListingError(
        LimitExceededException.builder().build(), KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleProvisionedThroughputExceededExceptionForShardListing() {
    shouldHandleShardListingError(
        ProvisionedThroughputExceededException.builder().build(),
        KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleServiceErrorForShardListing() {
    shouldHandleShardListingError(
        SdkServiceException.builder().build(), TransientKinesisException.class);
  }

  @Test
  public void shouldHandleClientErrorForShardListing() {
    shouldHandleShardListingError(SdkClientException.builder().build(), RuntimeException.class);
  }

  @Test
  public void shouldHandleUnexpectedExceptionForShardListing() {
    shouldHandleShardListingError(new NullPointerException(), RuntimeException.class);
  }

  private void shouldHandleShardListingError(
      Exception thrownException, Class<? extends Exception> expectedExceptionClass) {
    when(kinesis.listShards(any(ListShardsRequest.class))).thenThrow(thrownException);
    try {
      underTest.listShardsAtPoint(STREAM, new StartingPoint(InitialPositionInStream.TRIM_HORIZON));
      failBecauseExceptionWasNotThrown(expectedExceptionClass);
    } catch (Exception e) {
      assertThat(e).isExactlyInstanceOf(expectedExceptionClass);
    } finally {
      reset(kinesis);
    }
  }

  @Test
  public void shouldCountBytesWhenSingleDataPointReturned() throws Exception {
    Instant countSince = new Instant("2017-04-06T10:00:00.000Z");
    Instant countTo = new Instant("2017-04-06T11:00:00.000Z");
    Minutes periodTime = Minutes.minutesBetween(countSince, countTo);
    GetMetricStatisticsRequest metricStatisticsRequest =
        underTest.createMetricStatisticsRequest(STREAM, countSince, countTo, periodTime);
    GetMetricStatisticsResponse result =
        GetMetricStatisticsResponse.builder()
            .datapoints(Datapoint.builder().sum(1.0).build())
            .build();

    when(cloudWatch.getMetricStatistics(metricStatisticsRequest)).thenReturn(result);

    long backlogBytes = underTest.getBacklogBytes(STREAM, countSince, countTo);

    assertThat(backlogBytes).isEqualTo(1L);
  }

  @Test
  public void shouldCountBytesWhenMultipleDataPointsReturned() throws Exception {
    Instant countSince = new Instant("2017-04-06T10:00:00.000Z");
    Instant countTo = new Instant("2017-04-06T11:00:00.000Z");
    Minutes periodTime = Minutes.minutesBetween(countSince, countTo);
    GetMetricStatisticsRequest metricStatisticsRequest =
        underTest.createMetricStatisticsRequest(STREAM, countSince, countTo, periodTime);
    GetMetricStatisticsResponse result =
        GetMetricStatisticsResponse.builder()
            .datapoints(
                Datapoint.builder().sum(1.0).build(),
                Datapoint.builder().sum(3.0).build(),
                Datapoint.builder().sum(2.0).build())
            .build();

    when(cloudWatch.getMetricStatistics(metricStatisticsRequest)).thenReturn(result);

    long backlogBytes = underTest.getBacklogBytes(STREAM, countSince, countTo);

    assertThat(backlogBytes).isEqualTo(6L);
  }

  @Test
  public void shouldNotCallCloudWatchWhenSpecifiedPeriodTooShort() throws Exception {
    Instant countSince = new Instant("2017-04-06T10:00:00.000Z");
    Instant countTo = new Instant("2017-04-06T10:00:02.000Z");

    long backlogBytes = underTest.getBacklogBytes(STREAM, countSince, countTo);

    assertThat(backlogBytes).isEqualTo(0L);
    verifyZeroInteractions(cloudWatch);
  }

  @Test
  public void shouldHandleLimitExceededExceptionForGetBacklogBytes() {
    shouldHandleGetBacklogBytesError(
        LimitExceededException.builder().build(), KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleProvisionedThroughputExceededExceptionForGetBacklogBytes() {
    shouldHandleGetBacklogBytesError(
        ProvisionedThroughputExceededException.builder().build(),
        KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleServiceErrorForGetBacklogBytes() {
    shouldHandleGetBacklogBytesError(
        SdkServiceException.builder().build(), TransientKinesisException.class);
  }

  @Test
  public void shouldHandleClientErrorForGetBacklogBytes() {
    shouldHandleGetBacklogBytesError(SdkClientException.builder().build(), RuntimeException.class);
  }

  @Test
  public void shouldHandleUnexpectedExceptionForGetBacklogBytes() {
    shouldHandleGetBacklogBytesError(new NullPointerException(), RuntimeException.class);
  }

  private void shouldHandleGetBacklogBytesError(
      Exception thrownException, Class<? extends Exception> expectedExceptionClass) {
    Instant countSince = new Instant("2017-04-06T10:00:00.000Z");
    Instant countTo = new Instant("2017-04-06T11:00:00.000Z");
    Minutes periodTime = Minutes.minutesBetween(countSince, countTo);
    GetMetricStatisticsRequest metricStatisticsRequest =
        underTest.createMetricStatisticsRequest(STREAM, countSince, countTo, periodTime);

    when(cloudWatch.getMetricStatistics(metricStatisticsRequest)).thenThrow(thrownException);
    try {
      underTest.getBacklogBytes(STREAM, countSince, countTo);
      failBecauseExceptionWasNotThrown(expectedExceptionClass);
    } catch (Exception e) {
      assertThat(e).isExactlyInstanceOf(expectedExceptionClass);
    } finally {
      reset(kinesis);
    }
  }

  @Test
  public void shouldReturnLimitedNumberOfRecords() throws Exception {
    final Integer limit = 100;

    doAnswer(
            (Answer<GetRecordsResponse>)
                invocation -> {
                  GetRecordsRequest request = (GetRecordsRequest) invocation.getArguments()[0];
                  List<Record> records = generateRecords(request.limit());
                  return GetRecordsResponse.builder()
                      .records(records)
                      .millisBehindLatest(1000L)
                      .build();
                })
        .when(kinesis)
        .getRecords(any(GetRecordsRequest.class));

    GetKinesisRecordsResult result = underTest.getRecords(SHARD_ITERATOR, STREAM, SHARD_1, limit);
    assertThat(result.getRecords().size()).isEqualTo(limit);
  }

  private List<Record> generateRecords(int num) {
    List<Record> records = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      byte[] value = new byte[1024];
      Arrays.fill(value, (byte) i);
      records.add(
          Record.builder()
              .sequenceNumber(String.valueOf(i))
              .partitionKey("key")
              .data(SdkBytes.fromByteBuffer(ByteBuffer.wrap(value)))
              .build());
    }
    return records;
  }
}
