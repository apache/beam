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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.joda.time.Minutes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Datapoint;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsResponse;
import software.amazon.awssdk.services.cloudwatch.model.Statistic;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/** Wraps {@link KinesisClient} class providing much simpler interface and proper error handling. */
class SimplifiedKinesisClient {

  private static final String KINESIS_NAMESPACE = "AWS/Kinesis";
  private static final String INCOMING_RECORDS_METRIC = "IncomingBytes";
  private static final int PERIOD_GRANULARITY_IN_SECONDS = 60;
  private static final String STREAM_NAME_DIMENSION = "StreamName";

  private final KinesisClient kinesis;
  private final KinesisAsyncClient kinesisAsync;
  private final CloudWatchClient cloudWatch;
  private final Integer limit;

  public SimplifiedKinesisClient(
      KinesisClient kinesis,
      KinesisAsyncClient kinesisAsync,
      CloudWatchClient cloudWatch,
      Integer limit) {
    this.kinesis = checkNotNull(kinesis, "kinesis");
    this.kinesisAsync = checkNotNull(kinesisAsync, "kinesisAsync");
    this.cloudWatch = checkNotNull(cloudWatch, "cloudWatch");
    this.limit = limit;
  }

  public static SimplifiedKinesisClient from(AWSClientsProvider provider, Integer limit) {
    return new SimplifiedKinesisClient(
        provider.getKinesisClient(),
        provider.getKinesisAsyncClient(),
        provider.getCloudWatchClient(),
        limit);
  }

  public CompletableFuture<Void> subscribeToShard(
      final String consumerArn,
      final String shardId,
      final ShardIteratorType shardIteratorType,
      final String startingSequenceNumber,
      final Instant timestamp,
      final SubscribeToShardResponseHandler.Visitor visitor,
      final Consumer<Throwable> onError)
      throws TransientKinesisException {
    SubscribeToShardRequest request =
        SubscribeToShardRequest.builder()
            .consumerARN(consumerArn)
            .shardId(shardId)
            .startingPosition(
                s ->
                    s.type(shardIteratorType)
                        .sequenceNumber(startingSequenceNumber)
                        .timestamp(TimeUtil.toJava(timestamp)))
            .build();
    SubscribeToShardResponseHandler responseHandler =
        SubscribeToShardResponseHandler.builder().subscriber(visitor).onError(onError).build();
    return wrapExceptions(() -> kinesisAsync.subscribeToShard(request, responseHandler));
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

  public List<Shard> listShards(final String streamName) throws TransientKinesisException {
    return wrapExceptions(
        () -> {
          List<Shard> shards = Lists.newArrayList();
          String lastShardId = null;

          StreamDescription description;
          do {
            description =
                kinesis
                    .describeStream(
                        DescribeStreamRequest.builder()
                            .streamName(streamName)
                            .exclusiveStartShardId(lastShardId)
                            .build())
                    .streamDescription();

            shards.addAll(description.shards());
            lastShardId = shards.get(shards.size() - 1).shardId();
          } while (description.hasMoreShards());

          return shards;
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
          GetRecordsResponse response =
              kinesis.getRecords(
                  GetRecordsRequest.builder().shardIterator(shardIterator).limit(limit).build());
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
          GetMetricStatisticsResponse response = cloudWatch.getMetricStatistics(request);
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
      throw new TransientKinesisException(
          "Too many requests to Kinesis. Wait some time and retry.", e);
    } catch (SdkServiceException e) {
      throw new TransientKinesisException("Kinesis backend failed. Wait some time and retry.", e);
    } catch (SdkClientException e) {
      if (e.retryable()) {
        throw new TransientKinesisException("Retryable client failure", e);
      }
      throw new RuntimeException("Not retryable client failure", e);
    } catch (Exception e) {
      throw new RuntimeException("Unknown kinesis failure, when trying to reach kinesis", e);
    }
  }
}
