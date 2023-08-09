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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.joda.time.Minutes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/** Wraps {@link KinesisClient} class providing much simpler interface and proper error handling. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SimplifiedKinesisClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SimplifiedKinesisClient.class);

  private static final String KINESIS_NAMESPACE = "AWS/Kinesis";
  private static final String INCOMING_RECORDS_METRIC = "IncomingBytes";
  private static final int PERIOD_GRANULARITY_IN_SECONDS = 60;
  private static final String STREAM_NAME_DIMENSION = "StreamName";

  private final LazyResource<KinesisClient> kinesis;
  private final LazyResource<CloudWatchClient> cloudWatch;
  private final @Nullable Integer limit;

  SimplifiedKinesisClient(
      Supplier<KinesisClient> kinesisSupplier,
      Supplier<CloudWatchClient> cloudWatchSupplier,
      @Nullable Integer limit) {
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
        () -> {
          GetShardIteratorRequest request =
              GetShardIteratorRequest.builder()
                  .streamName(streamName)
                  .shardId(shardId)
                  .shardIteratorType(shardIteratorType)
                  .startingSequenceNumber(startingSequenceNumber)
                  .timestamp(TimeUtil.toJava(timestamp))
                  .build();

          LOG.info("Starting getIterator request {}", request);
          return kinesis.get().getShardIterator(request).shardIterator();
        });
  }

  public List<Shard> listShardsFollowingClosedShard(
      final String streamName, final String exclusiveStartShardId)
      throws TransientKinesisException {
    ShardFilter shardFilter =
        ShardFilter.builder()
            .type(ShardFilterType.AFTER_SHARD_ID)
            .shardId(exclusiveStartShardId)
            .build();
    return wrapExceptions(
        () -> ShardListingUtils.listShards(kinesis.get(), streamName, shardFilter));
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
  private static <T> T wrapExceptions(Callable<T> callable) throws TransientKinesisException {
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
