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
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescription;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.joda.time.Instant;
import org.joda.time.Minutes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
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

  @Mock private AmazonKinesis kinesis;
  @Mock private AmazonCloudWatch cloudWatch;
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
  public void shouldListAllShards() throws Exception {
    Shard shard1 = new Shard().withShardId(SHARD_1);
    Shard shard2 = new Shard().withShardId(SHARD_2);
    Shard shard3 = new Shard().withShardId(SHARD_3);
    when(kinesis.describeStream(STREAM, null))
        .thenReturn(
            new DescribeStreamResult()
                .withStreamDescription(
                    new StreamDescription().withShards(shard1, shard2).withHasMoreShards(true)));
    when(kinesis.describeStream(STREAM, SHARD_2))
        .thenReturn(
            new DescribeStreamResult()
                .withStreamDescription(
                    new StreamDescription().withShards(shard3).withHasMoreShards(false)));

    List<Shard> shards = underTest.listShards(STREAM);

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
    when(kinesis.describeStream(STREAM, null)).thenThrow(thrownException);
    try {
      underTest.listShards(STREAM);
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
