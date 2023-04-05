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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Minutes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Datapoint;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsResponse;
import software.amazon.awssdk.services.cloudwatch.model.Statistic;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
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
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/** Wraps {@link KinesisClient} class providing much simpler interface and proper error handling. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SimplifiedKinesisClient implements AutoCloseable {

  private static final String KINESIS_NAMESPACE = "AWS/Kinesis";
  private static final String INCOMING_RECORDS_METRIC = "IncomingBytes";
  private static final int PERIOD_GRANULARITY_IN_SECONDS = 60;
  private static final String STREAM_NAME_DIMENSION = "StreamName";
  private static final int LIST_SHARDS_MAX_RESULTS = 1_000;
  private static final Duration
      SPACING_FOR_TIMESTAMP_LIST_SHARDS_REQUEST_TO_NOT_EXCEED_TRIM_HORIZON =
          Duration.standardMinutes(5);
  private static final int DESCRIBE_STREAM_SUMMARY_MAX_ATTEMPTS = 10;
  private static final Duration DESCRIBE_STREAM_SUMMARY_INITIAL_BACKOFF =
      Duration.standardSeconds(1);

  private final LazyResource<KinesisClient> kinesis;
  private final LazyResource<CloudWatchClient> cloudWatch;
  private final Integer limit;

  SimplifiedKinesisClient(
      Supplier<KinesisClient> kinesisSupplier,
      Supplier<CloudWatchClient> cloudWatchSupplier,
      Integer limit) {
    this.kinesis = new LazyResource<>(checkNotNull(kinesisSupplier, "kinesis"));
    this.cloudWatch = new LazyResource<>(checkNotNull(cloudWatchSupplier, "cloudWatch"));
    this.limit = limit;
  }

  public String getShardIterator(
      final String streamName,
      final String shardId,
      final ShardIteratorType shardIteratorType,
      final String startingSequenceNumber,
      final Instant timestamp)
      throws TransientKinesisException {
    return wrapExceptions(
        () ->
            kinesis
                .get()
                .getShardIterator(
                    GetShardIteratorRequest.builder()
                        .streamName(streamName)
                        .shardId(shardId)
                        .shardIteratorType(shardIteratorType)
                        .startingSequenceNumber(startingSequenceNumber)
                        .timestamp(TimeUtil.toJava(timestamp))
                        .build())
                .shardIterator());
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
        return ShardFilter.builder().type(ShardFilterType.AT_LATEST).build();
      case TRIM_HORIZON:
        return ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();
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

    Instant streamCreationTimestamp = TimeUtil.toJoda(streamDescription.streamCreationTimestamp());
    if (streamCreationTimestamp.isAfter(startingPointTimestamp)) {
      return ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();
    }

    Duration retentionPeriod = Duration.standardHours(streamDescription.retentionPeriodHours());

    Instant streamTrimHorizonTimestamp =
        Instant.now()
            .minus(retentionPeriod)
            .plus(SPACING_FOR_TIMESTAMP_LIST_SHARDS_REQUEST_TO_NOT_EXCEED_TRIM_HORIZON);
    if (startingPointTimestamp.isAfter(streamTrimHorizonTimestamp)) {
      return ShardFilter.builder()
          .type(ShardFilterType.AT_TIMESTAMP)
          .timestamp(TimeUtil.toJava(startingPointTimestamp))
          .build();
    } else {
      return ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();
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

    DescribeStreamSummaryRequest request =
        DescribeStreamSummaryRequest.builder().streamName(streamName).build();
    while (true) {
      try {
        return kinesis.get().describeStreamSummary(request).streamDescriptionSummary();
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
        ShardFilter.builder()
            .type(ShardFilterType.AFTER_SHARD_ID)
            .shardId(exclusiveStartShardId)
            .build();
    return listShards(streamName, shardFilter);
  }

  private List<Shard> listShards(final String streamName, final ShardFilter shardFilter)
      throws TransientKinesisException {
    return wrapExceptions(
        () -> {
          ImmutableList.Builder<Shard> shardsBuilder = ImmutableList.builder();

          String currentNextToken = null;
          do {
            ListShardsRequest.Builder reqBuilder =
                ListShardsRequest.builder()
                    .maxResults(LIST_SHARDS_MAX_RESULTS)
                    .shardFilter(shardFilter);
            if (currentNextToken != null) {
              reqBuilder.nextToken(currentNextToken);
            } else {
              reqBuilder.streamName(streamName);
            }

            ListShardsResponse response = kinesis.get().listShards(reqBuilder.build());
            shardsBuilder.addAll(response.shards());
            currentNextToken = response.nextToken();
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
          GetRecordsRequest request =
              GetRecordsRequest.builder().shardIterator(shardIterator).limit(limit).build();
          GetRecordsResponse response = kinesis.get().getRecords(request);
          List<Record> records = response.records();
          return new GetKinesisRecordsResult(
              deaggregate(records),
              response.nextShardIterator(),
              response.millisBehindLatest(),
              streamName,
              shardId);
        });
  }

  public static List<KinesisClientRecord> deaggregate(List<Record> records) {
    return records.isEmpty()
        ? ImmutableList.of()
        : new AggregatorUtil()
            .deaggregate(
                records.stream().map(KinesisClientRecord::fromRecord).collect(Collectors.toList()));
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
          GetMetricStatisticsResponse response = cloudWatch.get().getMetricStatistics(request);
          for (Datapoint point : response.datapoints()) {
            totalSizeInBytes += point.sum().longValue();
          }
          return totalSizeInBytes;
        });
  }

  GetMetricStatisticsRequest createMetricStatisticsRequest(
      String streamName, Instant countSince, Instant countTo, Minutes period) {
    return GetMetricStatisticsRequest.builder()
        .namespace(KINESIS_NAMESPACE)
        .metricName(INCOMING_RECORDS_METRIC)
        .period(period.getMinutes() * PERIOD_GRANULARITY_IN_SECONDS)
        .startTime(TimeUtil.toJava(countSince))
        .endTime(TimeUtil.toJava(countTo))
        .statistics(Statistic.SUM)
        .dimensions(Dimension.builder().name(STREAM_NAME_DIMENSION).value(streamName).build())
        .build();
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
    } catch (SdkServiceException e) {
      if (e.isThrottlingException()
          || SdkDefaultRetrySetting.RETRYABLE_STATUS_CODES.contains(e.statusCode())) {
        throw new TransientKinesisException("Kinesis backend failed. Wait some time and retry.", e);
      }
      throw e; // others, such as 4xx, are not retryable
    } catch (SdkClientException e) {
      if (SdkDefaultRetrySetting.RETRYABLE_EXCEPTIONS.contains(e.getClass())) {
        throw new TransientKinesisException("Retryable failure", e);
      }
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Unknown kinesis failure, when trying to reach kinesis", e);
    }
  }

  @Override
  public void close() throws Exception {
    try (AutoCloseable c1 = kinesis;
        AutoCloseable c2 = cloudWatch) {
      // nothing to do
    }
  }

  /** Memoizing supplier that closes resources appropriately. */
  private static class LazyResource<T extends AutoCloseable> implements Supplier<T>, AutoCloseable {
    private final Supplier<T> initializer;
    private volatile T resource = null;

    private LazyResource(Supplier<T> initializer) {
      this.initializer = initializer;
    }

    @Override
    public void close() throws Exception {
      T res = resource;
      if (res != null) {
        res.close();
      }
    }

    @Override
    public T get() {
      T res = resource;
      if (res == null) {
        synchronized (this) {
          res = resource; // need to read again in synchronized
          if (res == null) {
            resource = res = initializer.get();
          }
        }
      }
      return res;
    }
  }
}
