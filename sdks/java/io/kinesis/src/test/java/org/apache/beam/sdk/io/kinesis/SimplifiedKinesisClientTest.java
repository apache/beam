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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryResult;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardFilter;
import com.amazonaws.services.kinesis.model.ShardFilterType;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescriptionSummary;
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

  @Mock private AmazonKinesis kinesis;
  @Mock private AmazonCloudWatch cloudWatch;
  @Mock private Supplier<Instant> currentInstantSupplier;
  @InjectMocks private SimplifiedKinesisClient underTest;

  @Test
  public void shouldReturnIteratorStartingWithSequenceNumber() throws Exception {
    when(kinesis.getShardIterator(
            new GetShardIteratorRequest()
                .withStreamName(STREAM)
                .withShardId(SHARD_1)
                .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                .withStartingSequenceNumber(SEQUENCE_NUMBER)))
        .thenReturn(new GetShardIteratorResult().withShardIterator(SHARD_ITERATOR));

    String stream =
        underTest.getShardIterator(
            STREAM, SHARD_1, ShardIteratorType.AT_SEQUENCE_NUMBER, SEQUENCE_NUMBER, null);

    assertThat(stream).isEqualTo(SHARD_ITERATOR);
  }

  @Test
  public void shouldReturnIteratorStartingWithTimestamp() throws Exception {
    Instant timestamp = Instant.now();
    when(kinesis.getShardIterator(
            new GetShardIteratorRequest()
                .withStreamName(STREAM)
                .withShardId(SHARD_1)
                .withShardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                .withTimestamp(timestamp.toDate())))
        .thenReturn(new GetShardIteratorResult().withShardIterator(SHARD_ITERATOR));

    String stream =
        underTest.getShardIterator(
            STREAM, SHARD_1, ShardIteratorType.AT_SEQUENCE_NUMBER, null, timestamp);

    assertThat(stream).isEqualTo(SHARD_ITERATOR);
  }

  @Test
  public void shouldHandleExpiredIterationExceptionForGetShardIterator() {
    shouldHandleGetShardIteratorError(
        new ExpiredIteratorException(""), ExpiredIteratorException.class);
  }

  @Test
  public void shouldHandleLimitExceededExceptionForGetShardIterator() {
    shouldHandleGetShardIteratorError(
        new LimitExceededException(""), KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleProvisionedThroughputExceededExceptionForGetShardIterator() {
    shouldHandleGetShardIteratorError(
        new ProvisionedThroughputExceededException(""), KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleServiceErrorForGetShardIterator() {
    shouldHandleGetShardIteratorError(
        newAmazonServiceException(ErrorType.Service), TransientKinesisException.class);
  }

  @Test
  public void shouldHandleClientErrorForGetShardIterator() {
    shouldHandleGetShardIteratorError(
        newAmazonServiceException(ErrorType.Client), RuntimeException.class);
  }

  @Test
  public void shouldHandleUnexpectedExceptionForGetShardIterator() {
    shouldHandleGetShardIteratorError(new NullPointerException(), RuntimeException.class);
  }

  private void shouldHandleGetShardIteratorError(
      Exception thrownException, Class<? extends Exception> expectedExceptionClass) {
    GetShardIteratorRequest request =
        new GetShardIteratorRequest()
            .withStreamName(STREAM)
            .withShardId(SHARD_1)
            .withShardIteratorType(ShardIteratorType.LATEST);

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
    Shard shard1 = new Shard().withShardId(SHARD_1);
    Shard shard2 = new Shard().withShardId(SHARD_2);
    Shard shard3 = new Shard().withShardId(SHARD_3);

    ShardFilter shardFilter = new ShardFilter().withType(ShardFilterType.AT_TRIM_HORIZON);

    when(kinesis.listShards(
            new ListShardsRequest()
                .withStreamName(STREAM)
                .withShardFilter(shardFilter)
                .withMaxResults(1_000)))
        .thenReturn(new ListShardsResult().withShards(shard1, shard2, shard3).withNextToken(null));

    List<Shard> shards =
        underTest.listShardsAtPoint(
            STREAM, new StartingPoint(InitialPositionInStream.TRIM_HORIZON));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForTrimHorizonWithPagedResults() throws Exception {
    Shard shard1 = new Shard().withShardId(SHARD_1);
    Shard shard2 = new Shard().withShardId(SHARD_2);
    Shard shard3 = new Shard().withShardId(SHARD_3);

    ShardFilter shardFilter = new ShardFilter().withType(ShardFilterType.AT_TRIM_HORIZON);

    String nextListShardsToken = "testNextToken";
    when(kinesis.listShards(
            new ListShardsRequest()
                .withStreamName(STREAM)
                .withShardFilter(shardFilter)
                .withMaxResults(1_000)))
        .thenReturn(
            new ListShardsResult().withShards(shard1, shard2).withNextToken(nextListShardsToken));

    when(kinesis.listShards(
            new ListShardsRequest()
                .withMaxResults(1_000)
                .withShardFilter(shardFilter)
                .withNextToken(nextListShardsToken)))
        .thenReturn(new ListShardsResult().withShards(shard3).withNextToken(null));

    List<Shard> shards =
        underTest.listShardsAtPoint(
            STREAM, new StartingPoint(InitialPositionInStream.TRIM_HORIZON));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForTimestampWithinStreamRetentionAfterStreamCreationTimestamp()
      throws Exception {
    Shard shard1 = new Shard().withShardId(SHARD_1);
    Shard shard2 = new Shard().withShardId(SHARD_2);
    Shard shard3 = new Shard().withShardId(SHARD_3);

    int hoursDifference = 1;
    int retentionPeriodHours = hoursDifference * 3;
    Instant streamCreationTimestamp =
        CURRENT_TIMESTAMP.minus(Duration.standardHours(retentionPeriodHours));
    Instant startingPointTimestamp =
        streamCreationTimestamp.plus(Duration.standardHours(hoursDifference));

    when(currentInstantSupplier.get()).thenReturn(CURRENT_TIMESTAMP);

    when(kinesis.describeStreamSummary(new DescribeStreamSummaryRequest().withStreamName(STREAM)))
        .thenReturn(
            new DescribeStreamSummaryResult()
                .withStreamDescriptionSummary(
                    new StreamDescriptionSummary()
                        .withRetentionPeriodHours(retentionPeriodHours)
                        .withStreamCreationTimestamp(streamCreationTimestamp.toDate())));

    ShardFilter shardFilter =
        new ShardFilter()
            .withType(ShardFilterType.AT_TIMESTAMP)
            .withTimestamp(startingPointTimestamp.toDate());

    when(kinesis.listShards(
            new ListShardsRequest()
                .withStreamName(STREAM)
                .withShardFilter(shardFilter)
                .withMaxResults(1_000)))
        .thenReturn(new ListShardsResult().withShards(shard1, shard2, shard3).withNextToken(null));

    List<Shard> shards =
        underTest.listShardsAtPoint(STREAM, new StartingPoint(startingPointTimestamp));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void
      shouldListAllShardsForTimestampWithRetriedDescribeStreamSummaryCallAfterStreamCreationTimestamp()
          throws TransientKinesisException {
    Shard shard1 = new Shard().withShardId(SHARD_1);
    Shard shard2 = new Shard().withShardId(SHARD_2);
    Shard shard3 = new Shard().withShardId(SHARD_3);

    int hoursDifference = 1;
    int retentionPeriodHours = hoursDifference * 3;
    Instant streamCreationTimestamp =
        CURRENT_TIMESTAMP.minus(Duration.standardHours(retentionPeriodHours));
    Instant startingPointTimestamp =
        streamCreationTimestamp.plus(Duration.standardHours(hoursDifference));

    when(currentInstantSupplier.get()).thenReturn(CURRENT_TIMESTAMP);

    when(kinesis.describeStreamSummary(new DescribeStreamSummaryRequest().withStreamName(STREAM)))
        .thenThrow(new LimitExceededException("Fake Exception: Limit exceeded"))
        .thenReturn(
            new DescribeStreamSummaryResult()
                .withStreamDescriptionSummary(
                    new StreamDescriptionSummary()
                        .withRetentionPeriodHours(retentionPeriodHours)
                        .withStreamCreationTimestamp(streamCreationTimestamp.toDate())));

    ShardFilter shardFilter =
        new ShardFilter()
            .withType(ShardFilterType.AT_TIMESTAMP)
            .withTimestamp(startingPointTimestamp.toDate());

    when(kinesis.listShards(
            new ListShardsRequest()
                .withStreamName(STREAM)
                .withShardFilter(shardFilter)
                .withMaxResults(1_000)))
        .thenReturn(new ListShardsResult().withShards(shard1, shard2, shard3).withNextToken(null));

    List<Shard> shards =
        underTest.listShardsAtPoint(STREAM, new StartingPoint(startingPointTimestamp));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForTimestampOutsideStreamRetentionAfterStreamCreationTimestamp()
      throws Exception {
    Shard shard1 = new Shard().withShardId(SHARD_1);
    Shard shard2 = new Shard().withShardId(SHARD_2);
    Shard shard3 = new Shard().withShardId(SHARD_3);

    int retentionPeriodHours = 3;
    int startingPointHours = 5;
    int hoursSinceStreamCreation = 6;

    Instant streamCreationTimestamp =
        CURRENT_TIMESTAMP.minus(Duration.standardHours(hoursSinceStreamCreation));
    Instant startingPointTimestampAfterStreamRetentionTimestamp =
        CURRENT_TIMESTAMP.minus(Duration.standardHours(startingPointHours));

    when(currentInstantSupplier.get()).thenReturn(CURRENT_TIMESTAMP);

    DescribeStreamSummaryRequest describeStreamRequest =
        new DescribeStreamSummaryRequest().withStreamName(STREAM);
    when(kinesis.describeStreamSummary(describeStreamRequest))
        .thenReturn(
            new DescribeStreamSummaryResult()
                .withStreamDescriptionSummary(
                    new StreamDescriptionSummary()
                        .withRetentionPeriodHours(retentionPeriodHours)
                        .withStreamCreationTimestamp(streamCreationTimestamp.toDate())));

    ShardFilter shardFilter = new ShardFilter().withType(ShardFilterType.AT_TRIM_HORIZON);

    when(kinesis.listShards(
            new ListShardsRequest()
                .withStreamName(STREAM)
                .withShardFilter(shardFilter)
                .withMaxResults(1_000)))
        .thenReturn(new ListShardsResult().withShards(shard1, shard2, shard3).withNextToken(null));

    List<Shard> shards =
        underTest.listShardsAtPoint(
            STREAM, new StartingPoint(startingPointTimestampAfterStreamRetentionTimestamp));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForTimestampBeforeStreamCreationTimestamp() throws Exception {
    Shard shard1 = new Shard().withShardId(SHARD_1);
    Shard shard2 = new Shard().withShardId(SHARD_2);
    Shard shard3 = new Shard().withShardId(SHARD_3);

    Instant startingPointTimestamp = Instant.parse("2000-01-01T15:00:00.000Z");
    Instant streamCreationTimestamp = startingPointTimestamp.plus(Duration.standardHours(1));

    DescribeStreamSummaryRequest describeStreamRequest =
        new DescribeStreamSummaryRequest().withStreamName(STREAM);
    when(kinesis.describeStreamSummary(describeStreamRequest))
        .thenReturn(
            new DescribeStreamSummaryResult()
                .withStreamDescriptionSummary(
                    new StreamDescriptionSummary()
                        .withStreamCreationTimestamp(streamCreationTimestamp.toDate())));

    ShardFilter shardFilter = new ShardFilter().withType(ShardFilterType.AT_TRIM_HORIZON);

    when(kinesis.listShards(
            new ListShardsRequest()
                .withStreamName(STREAM)
                .withShardFilter(shardFilter)
                .withMaxResults(1_000)))
        .thenReturn(new ListShardsResult().withShards(shard1, shard2, shard3).withNextToken(null));

    List<Shard> shards =
        underTest.listShardsAtPoint(STREAM, new StartingPoint(startingPointTimestamp));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForLatest() throws Exception {
    Shard shard1 = new Shard().withShardId(SHARD_1);
    Shard shard2 = new Shard().withShardId(SHARD_2);
    Shard shard3 = new Shard().withShardId(SHARD_3);

    when(kinesis.listShards(
            new ListShardsRequest()
                .withStreamName(STREAM)
                .withShardFilter(new ShardFilter().withType(ShardFilterType.AT_LATEST))
                .withMaxResults(1_000)))
        .thenReturn(new ListShardsResult().withShards(shard1, shard2, shard3).withNextToken(null));

    List<Shard> shards =
        underTest.listShardsAtPoint(STREAM, new StartingPoint(InitialPositionInStream.LATEST));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldListAllShardsForExclusiveStartShardId() throws Exception {
    Shard shard1 = new Shard().withShardId(SHARD_1);
    Shard shard2 = new Shard().withShardId(SHARD_2);
    Shard shard3 = new Shard().withShardId(SHARD_3);

    String exclusiveStartShardId = "exclusiveStartShardId";

    when(kinesis.listShards(
            new ListShardsRequest()
                .withStreamName(STREAM)
                .withMaxResults(1_000)
                .withShardFilter(
                    new ShardFilter()
                        .withType(ShardFilterType.AFTER_SHARD_ID)
                        .withShardId(exclusiveStartShardId))))
        .thenReturn(new ListShardsResult().withShards(shard1, shard2, shard3).withNextToken(null));

    List<Shard> shards = underTest.listShardsFollowingClosedShard(STREAM, exclusiveStartShardId);

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldHandleExpiredIterationExceptionForShardListing() {
    shouldHandleShardListingError(new ExpiredIteratorException(""), ExpiredIteratorException.class);
  }

  @Test
  public void shouldHandleLimitExceededExceptionForShardListing() {
    shouldHandleShardListingError(
        new LimitExceededException(""), KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleProvisionedThroughputExceededExceptionForShardListing() {
    shouldHandleShardListingError(
        new ProvisionedThroughputExceededException(""), KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleServiceErrorForShardListing() {
    shouldHandleShardListingError(
        newAmazonServiceException(ErrorType.Service), TransientKinesisException.class);
  }

  @Test
  public void shouldHandleClientErrorForShardListing() {
    shouldHandleShardListingError(
        newAmazonServiceException(ErrorType.Client), RuntimeException.class);
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
    GetMetricStatisticsResult result =
        new GetMetricStatisticsResult().withDatapoints(new Datapoint().withSum(1.0));

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
    GetMetricStatisticsResult result =
        new GetMetricStatisticsResult()
            .withDatapoints(
                new Datapoint().withSum(1.0),
                new Datapoint().withSum(3.0),
                new Datapoint().withSum(2.0));

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
        new LimitExceededException(""), KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleProvisionedThroughputExceededExceptionForGetBacklogBytes() {
    shouldHandleGetBacklogBytesError(
        new ProvisionedThroughputExceededException(""), KinesisClientThrottledException.class);
  }

  @Test
  public void shouldHandleServiceErrorForGetBacklogBytes() {
    shouldHandleGetBacklogBytesError(
        newAmazonServiceException(ErrorType.Service), TransientKinesisException.class);
  }

  @Test
  public void shouldHandleClientErrorForGetBacklogBytes() {
    shouldHandleGetBacklogBytesError(
        newAmazonServiceException(ErrorType.Client), RuntimeException.class);
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

  private AmazonServiceException newAmazonServiceException(ErrorType errorType) {
    AmazonServiceException exception = new AmazonServiceException("");
    exception.setErrorType(errorType);
    return exception;
  }

  @Test
  public void shouldReturnLimitedNumberOfRecords() throws Exception {
    final Integer limit = 100;

    doAnswer(
            (Answer<GetRecordsResult>)
                invocation -> {
                  GetRecordsRequest request = (GetRecordsRequest) invocation.getArguments()[0];
                  List<Record> records = generateRecords(request.getLimit());
                  return new GetRecordsResult().withRecords(records).withMillisBehindLatest(1000L);
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
          new Record()
              .withSequenceNumber(String.valueOf(i))
              .withPartitionKey("key")
              .withData(ByteBuffer.wrap(value)));
    }
    return records;
  }
}
