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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardFilter;
import com.amazonaws.services.kinesis.model.ShardFilterType;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescriptionSummary;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Minutes;

/** Wraps {@link AmazonKinesis} class providing much simpler interface and proper error handling. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SimplifiedKinesisClient {

  private static final String KINESIS_NAMESPACE = "AWS/Kinesis";
  private static final String INCOMING_RECORDS_METRIC = "IncomingBytes";
  private static final int PERIOD_GRANULARITY_IN_SECONDS = 60;
  private static final String SUM_STATISTIC = "Sum";
  private static final String STREAM_NAME_DIMENSION = "StreamName";
  private static final int LIST_SHARDS_MAX_RESULTS = 1_000;
  private static final Duration
      SPACING_FOR_TIMESTAMP_LIST_SHARDS_REQUEST_TO_NOT_EXCEED_TRIM_HORIZON =
          Duration.standardMinutes(5);
  private static final int DESCRIBE_STREAM_SUMMARY_MAX_ATTEMPTS = 10;
  private static final Duration DESCRIBE_STREAM_SUMMARY_INITIAL_BACKOFF =
      Duration.standardSeconds(1);

  private final AmazonKinesis kinesis;
  private final AmazonCloudWatch cloudWatch;
  private final Integer limit;
  private final Supplier<Instant> currentInstantSupplier;

  public SimplifiedKinesisClient(
      AmazonKinesis kinesis, AmazonCloudWatch cloudWatch, Integer limit) {
    this(kinesis, cloudWatch, limit, Instant::now);
  }

  SimplifiedKinesisClient(
      AmazonKinesis kinesis,
      AmazonCloudWatch cloudWatch,
      Integer limit,
      Supplier<Instant> currentInstantSupplier) {
    this.kinesis = checkNotNull(kinesis, "kinesis");
    this.cloudWatch = checkNotNull(cloudWatch, "cloudWatch");
    this.limit = limit;
    this.currentInstantSupplier = currentInstantSupplier;
  }

  public static SimplifiedKinesisClient from(AWSClientsProvider provider, Integer limit) {
    return new SimplifiedKinesisClient(
        provider.getKinesisClient(), provider.getCloudWatchClient(), limit);
  }

  public String getShardIterator(
      final String streamName,
      final String shardId,
      final ShardIteratorType shardIteratorType,
      final String startingSequenceNumber,
      final Instant timestamp)
      throws TransientKinesisException {
    final Date date = timestamp != null ? timestamp.toDate() : null;
    return wrapExceptions(
        () ->
            kinesis
                .getShardIterator(
                    new GetShardIteratorRequest()
                        .withStreamName(streamName)
                        .withShardId(shardId)
                        .withShardIteratorType(shardIteratorType)
                        .withStartingSequenceNumber(startingSequenceNumber)
                        .withTimestamp(date))
                .getShardIterator());
  }

  public List<Shard> listShardsAtPoint(final String streamName, final StartingPoint startingPoint)
      throws TransientKinesisException {
    ShardFilter shardFilter =
        wrapExceptions(() -> buildShardFilterForStartingPoint(streamName, startingPoint));
    return listShards(streamName, shardFilter);
  }

  private ShardFilter buildShardFilterForStartingPoint(
      String streamName, StartingPoint startingPoint) throws IOException, InterruptedException {
    InitialPositionInStream position = startingPoint.getPosition();
    switch (position) {
      case LATEST:
        return new ShardFilter().withType(ShardFilterType.AT_LATEST);
      case TRIM_HORIZON:
        return new ShardFilter().withType(ShardFilterType.AT_TRIM_HORIZON);
      case AT_TIMESTAMP:
        return buildShardFilterForTimestamp(streamName, startingPoint.getTimestamp());
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized '%s' position to create shard filter with", position));
    }
  }

  private ShardFilter buildShardFilterForTimestamp(
      String streamName, Instant startingPointTimestamp) throws IOException, InterruptedException {
    StreamDescriptionSummary streamDescription = describeStreamSummary(streamName);

    Instant streamCreationTimestamp = new Instant(streamDescription.getStreamCreationTimestamp());
    if (streamCreationTimestamp.isAfter(startingPointTimestamp)) {
      return new ShardFilter().withType(ShardFilterType.AT_TRIM_HORIZON);
    }

    Duration retentionPeriod = Duration.standardHours(streamDescription.getRetentionPeriodHours());

    Instant streamTrimHorizonTimestamp =
        currentInstantSupplier
            .get()
            .minus(retentionPeriod)
            .plus(SPACING_FOR_TIMESTAMP_LIST_SHARDS_REQUEST_TO_NOT_EXCEED_TRIM_HORIZON);
    if (startingPointTimestamp.isAfter(streamTrimHorizonTimestamp)) {
      return new ShardFilter()
          .withType(ShardFilterType.AT_TIMESTAMP)
          .withTimestamp(startingPointTimestamp.toDate());
    } else {
      return new ShardFilter().withType(ShardFilterType.AT_TRIM_HORIZON);
    }
  }

  private StreamDescriptionSummary describeStreamSummary(final String streamName)
      throws IOException, InterruptedException {
    // DescribeStreamSummary has limits that can be hit fairly easily if we are attempting
    // to configure multiple KinesisIO inputs in the same account. Retry up to
    // DESCRIBE_STREAM_SUMMARY_MAX_ATTEMPTS times if we end up hitting that limit.
    //
    // Only pass the wrapped exception up once that limit is reached. Use FluentBackoff
    // to implement the retry policy.
    FluentBackoff retryBackoff =
        FluentBackoff.DEFAULT
            .withMaxRetries(DESCRIBE_STREAM_SUMMARY_MAX_ATTEMPTS)
            .withInitialBackoff(DESCRIBE_STREAM_SUMMARY_INITIAL_BACKOFF);
    BackOff backoff = retryBackoff.backoff();
    Sleeper sleeper = Sleeper.DEFAULT;

    DescribeStreamSummaryRequest request = new DescribeStreamSummaryRequest();
    request.setStreamName(streamName);
    while (true) {
      try {
        return kinesis.describeStreamSummary(request).getStreamDescriptionSummary();
      } catch (LimitExceededException exc) {
        if (!BackOffUtils.next(sleeper, backoff)) {
          throw exc;
        }
      }
    }
  }

  public List<Shard> listShardsFollowingClosedShard(
      final String streamName, final String exclusiveStartShardId)
      throws TransientKinesisException {
    ShardFilter shardFilter =
        new ShardFilter()
            .withType(ShardFilterType.AFTER_SHARD_ID)
            .withShardId(exclusiveStartShardId);
    return listShards(streamName, shardFilter);
  }

  private List<Shard> listShards(final String streamName, final ShardFilter shardFilter)
      throws TransientKinesisException {
    return wrapExceptions(
        () -> {
          ImmutableList.Builder<Shard> shardsBuilder = ImmutableList.builder();

          String currentNextToken = null;
          do {
            ListShardsRequest request = new ListShardsRequest();
            request.setMaxResults(LIST_SHARDS_MAX_RESULTS);
            if (currentNextToken != null) {
              request.setNextToken(currentNextToken);
            } else {
              request.setStreamName(streamName);
            }
            request.setShardFilter(shardFilter);

            ListShardsResult response = kinesis.listShards(request);
            List<Shard> shards = response.getShards();
            shardsBuilder.addAll(shards);
            currentNextToken = response.getNextToken();
          } while (currentNextToken != null);

          return shardsBuilder.build();
        });
  }

  /**
   * Gets records from Kinesis and deaggregates them if needed.
   *
   * @return list of deaggregated records
   * @throws TransientKinesisException - in case of recoverable situation
   */
  public GetKinesisRecordsResult getRecords(String shardIterator, String streamName, String shardId)
      throws TransientKinesisException {
    return getRecords(shardIterator, streamName, shardId, limit);
  }

  /**
   * Gets records from Kinesis and deaggregates them if needed.
   *
   * @return list of deaggregated records
   * @throws TransientKinesisException - in case of recoverable situation
   */
  public GetKinesisRecordsResult getRecords(
      final String shardIterator,
      final String streamName,
      final String shardId,
      final Integer limit)
      throws TransientKinesisException {
    return wrapExceptions(
        () -> {
          GetRecordsResult response =
              kinesis.getRecords(
                  new GetRecordsRequest().withShardIterator(shardIterator).withLimit(limit));
          return new GetKinesisRecordsResult(
              UserRecord.deaggregate(response.getRecords()),
              response.getNextShardIterator(),
              response.getMillisBehindLatest(),
              streamName,
              shardId);
        });
  }

  /**
   * Gets total size in bytes of all events that remain in Kinesis stream after specified instant.
   *
   * @return total size in bytes of all Kinesis events after specified instant
   */
  public long getBacklogBytes(String streamName, Instant countSince)
      throws TransientKinesisException {
    return getBacklogBytes(streamName, countSince, new Instant());
  }

  /**
   * Gets total size in bytes of all events that remain in Kinesis stream between specified
   * instants.
   *
   * @return total size in bytes of all Kinesis events after specified instant
   */
  public long getBacklogBytes(
      final String streamName, final Instant countSince, final Instant countTo)
      throws TransientKinesisException {
    return wrapExceptions(
        () -> {
          Minutes period = Minutes.minutesBetween(countSince, countTo);
          if (period.isLessThan(Minutes.ONE)) {
            return 0L;
          }

          GetMetricStatisticsRequest request =
              createMetricStatisticsRequest(streamName, countSince, countTo, period);

          long totalSizeInBytes = 0;
          GetMetricStatisticsResult result = cloudWatch.getMetricStatistics(request);
          for (Datapoint point : result.getDatapoints()) {
            totalSizeInBytes += point.getSum().longValue();
          }
          return totalSizeInBytes;
        });
  }

  GetMetricStatisticsRequest createMetricStatisticsRequest(
      String streamName, Instant countSince, Instant countTo, Minutes period) {
    return new GetMetricStatisticsRequest()
        .withNamespace(KINESIS_NAMESPACE)
        .withMetricName(INCOMING_RECORDS_METRIC)
        .withPeriod(period.getMinutes() * PERIOD_GRANULARITY_IN_SECONDS)
        .withStartTime(countSince.toDate())
        .withEndTime(countTo.toDate())
        .withStatistics(Collections.singletonList(SUM_STATISTIC))
        .withDimensions(
            Collections.singletonList(
                new Dimension().withName(STREAM_NAME_DIMENSION).withValue(streamName)));
  }

  /**
   * Wraps Amazon specific exceptions into more friendly format.
   *
   * @throws TransientKinesisException - in case of recoverable situation, i.e. the request rate is
   *     too high, Kinesis remote service failed, network issue, etc.
   * @throws ExpiredIteratorException - if iterator needs to be refreshed
   * @throws RuntimeException - in all other cases
   */
  private <T> T wrapExceptions(Callable<T> callable) throws TransientKinesisException {
    try {
      return callable.call();
    } catch (ExpiredIteratorException e) {
      throw e;
    } catch (LimitExceededException | ProvisionedThroughputExceededException e) {
      throw new KinesisClientThrottledException(
          "Too many requests to Kinesis. Wait some time and retry.", e);
    } catch (AmazonServiceException e) {
      if (e.getErrorType() == AmazonServiceException.ErrorType.Service) {
        throw new TransientKinesisException("Kinesis backend failed. Wait some time and retry.", e);
      }
      throw new RuntimeException("Kinesis client side failure", e);
    } catch (AmazonClientException e) {
      if (e.isRetryable()) {
        throw new TransientKinesisException("Retryable client failure", e);
      }
      throw new RuntimeException("Not retryable client failure", e);
    } catch (Exception e) {
      throw new RuntimeException("Unknown kinesis failure, when trying to reach kinesis", e);
    }
  }
}
