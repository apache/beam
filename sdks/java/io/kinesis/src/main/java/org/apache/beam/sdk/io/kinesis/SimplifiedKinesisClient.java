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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescription;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Minutes;

/** Wraps {@link AmazonKinesis} class providing much simpler interface and proper error handling. */
class SimplifiedKinesisClient {

  private static final String KINESIS_NAMESPACE = "AWS/Kinesis";
  private static final String INCOMING_RECORDS_METRIC = "IncomingBytes";
  private static final int PERIOD_GRANULARITY_IN_SECONDS = 60;
  private static final String SUM_STATISTIC = "Sum";
  private static final String STREAM_NAME_DIMENSION = "StreamName";
  private static final int LIST_SHARDS_DESCRIBE_STREAM_MAX_ATTEMPTS = 10;
  private static final Duration LIST_SHARDS_DESCRIBE_STREAM_INITIAL_BACKOFF =
      Duration.standardSeconds(1);
  private final AmazonKinesis kinesis;
  private final AmazonCloudWatch cloudWatch;
  private final Integer limit;

  public SimplifiedKinesisClient(
      AmazonKinesis kinesis, AmazonCloudWatch cloudWatch, Integer limit) {
    this.kinesis = checkNotNull(kinesis, "kinesis");
    this.cloudWatch = checkNotNull(cloudWatch, "cloudWatch");
    this.limit = limit;
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

  public List<Shard> listShards(final String streamName) throws TransientKinesisException {
    return wrapExceptions(
        () -> {
          List<Shard> shards = Lists.newArrayList();
          String lastShardId = null;

          // DescribeStream has limits that can be hit fairly easily if we are attempting
          // to configure multiple KinesisIO inputs in the same account. Retry up to
          // LIST_SHARDS_DESCRIBE_STREAM_MAX_ATTEMPTS times if we end up hitting that limit.
          //
          // Only pass the wrapped exception up once that limit is reached. Use FluentBackoff
          // to implement the retry policy.
          FluentBackoff retryBackoff =
              FluentBackoff.DEFAULT
                  .withMaxRetries(LIST_SHARDS_DESCRIBE_STREAM_MAX_ATTEMPTS)
                  .withInitialBackoff(LIST_SHARDS_DESCRIBE_STREAM_INITIAL_BACKOFF);
          StreamDescription description = null;
          do {
            BackOff backoff = retryBackoff.backoff();
            Sleeper sleeper = Sleeper.DEFAULT;
            while (true) {
              try {
                description =
                    kinesis.describeStream(streamName, lastShardId).getStreamDescription();
                break;
              } catch (LimitExceededException exc) {
                if (!BackOffUtils.next(sleeper, backoff)) {
                  throw exc;
                }
              }
            }

            shards.addAll(description.getShards());
            lastShardId = shards.get(shards.size() - 1).getShardId();
          } while (description.getHasMoreShards());

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
