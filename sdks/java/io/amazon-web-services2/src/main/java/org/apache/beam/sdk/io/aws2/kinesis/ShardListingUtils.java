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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.InitialPositionInStream;

class ShardListingUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ShardListingUtils.class);

  private static final int LIST_SHARDS_MAX_RESULTS = 1_000;
  private static final Duration
      SPACING_FOR_TIMESTAMP_LIST_SHARDS_REQUEST_TO_NOT_EXCEED_TRIM_HORIZON =
          Duration.standardMinutes(5);
  private static final int DESCRIBE_STREAM_SUMMARY_MAX_ATTEMPTS = 10;
  private static final Duration DESCRIBE_STREAM_SUMMARY_INITIAL_BACKOFF =
      Duration.standardSeconds(1);

  static List<Shard> listShardsAtPoint(
      KinesisClient kinesisClient, final String streamName, final StartingPoint startingPoint)
      throws TransientKinesisException {
    ShardFilter shardFilter =
        wrapExceptions(
            () -> buildShardFilterForStartingPoint(kinesisClient, streamName, startingPoint));
    return listShards(kinesisClient, streamName, shardFilter);
  }

  static ShardFilter buildShardFilterForStartingPoint(
      KinesisClient kinesisClient, String streamName, StartingPoint startingPoint)
      throws IOException, InterruptedException {
    InitialPositionInStream position = startingPoint.getPosition();
    switch (position) {
      case LATEST:
        return ShardFilter.builder().type(ShardFilterType.AT_LATEST).build();
      case TRIM_HORIZON:
        return ShardFilter.builder().type(ShardFilterType.AT_TRIM_HORIZON).build();
      case AT_TIMESTAMP:
        return buildShardFilterForTimestamp(
            kinesisClient, streamName, startingPoint.getTimestamp());
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized '%s' position to create shard filter with", position));
    }
  }

  private static ShardFilter buildShardFilterForTimestamp(
      KinesisClient kinesisClient, String streamName, Instant startingPointTimestamp)
      throws IOException, InterruptedException {
    StreamDescriptionSummary streamDescription = describeStreamSummary(kinesisClient, streamName);

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

  private static StreamDescriptionSummary describeStreamSummary(
      KinesisClient kinesisClient, final String streamName)
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
        LOG.info("Executing request: {}", request);
        return kinesisClient.describeStreamSummary(request).streamDescriptionSummary();
      } catch (LimitExceededException exc) {
        if (!BackOffUtils.next(sleeper, backoff)) {
          throw exc;
        }
      }
    }
  }

  static List<Shard> listShards(
      KinesisClient kinesisClient, final String streamName, final ShardFilter shardFilter)
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

            ListShardsRequest request = reqBuilder.build();
            LOG.info("Executing request: {}", request);
            ListShardsResponse response = kinesisClient.listShards(request);
            shardsBuilder.addAll(response.shards());
            currentNextToken = response.nextToken();
          } while (currentNextToken != null);

          return shardsBuilder.build();
        });
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
}
