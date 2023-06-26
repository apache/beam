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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.kinesis.common.InitialPositionInStream;

@RunWith(MockitoJUnitRunner.class)
public class ShardListingUtilsTest {
  private static final String STREAM = "stream";
  private static final String SHARD_1 = "shard-01";
  private static final String SHARD_2 = "shard-02";
  private static final String SHARD_3 = "shard-03";
  private static final Instant CURRENT_TIMESTAMP = Instant.parse("2000-01-01T15:00:00.000Z");

  @Mock private KinesisClient kinesis;

  @After
  public void afterEach() {
    DateTimeUtils.setCurrentMillisSystem();
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
        ShardListingUtils.listShardsAtPoint(
            kinesis, STREAM, new StartingPoint(InitialPositionInStream.TRIM_HORIZON));

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
        ShardListingUtils.listShardsAtPoint(
            kinesis, STREAM, new StartingPoint(InitialPositionInStream.TRIM_HORIZON));

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
    DateTimeUtils.setCurrentMillisFixed(CURRENT_TIMESTAMP.getMillis());

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
        ShardListingUtils.listShardsAtPoint(
            kinesis, STREAM, new StartingPoint(startingPointTimestamp));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void
      shouldListAllShardsForTimestampWithRetriedDescribeStreamSummaryCallAfterStreamCreationTimestamp()
          throws IOException, InterruptedException {
    Shard shard1 = Shard.builder().shardId(SHARD_1).build();
    Shard shard2 = Shard.builder().shardId(SHARD_2).build();
    Shard shard3 = Shard.builder().shardId(SHARD_3).build();

    int hoursDifference = 1;
    int retentionPeriodHours = hoursDifference * 3;
    Instant streamCreationTimestamp =
        CURRENT_TIMESTAMP.minus(Duration.standardHours(retentionPeriodHours));
    Instant startingPointTimestamp =
        streamCreationTimestamp.plus(Duration.standardHours(hoursDifference));

    DateTimeUtils.setCurrentMillisFixed(CURRENT_TIMESTAMP.getMillis());
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
        ShardListingUtils.listShardsAtPoint(
            kinesis, STREAM, new StartingPoint(startingPointTimestamp));

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
    DateTimeUtils.setCurrentMillisFixed(CURRENT_TIMESTAMP.getMillis());

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
        ShardListingUtils.listShardsAtPoint(
            kinesis,
            STREAM,
            new StartingPoint(startingPointTimestampAfterStreamRetentionTimestamp));

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
        ShardListingUtils.listShardsAtPoint(
            kinesis, STREAM, new StartingPoint(startingPointTimestamp));

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
        ShardListingUtils.listShardsAtPoint(
            kinesis, STREAM, new StartingPoint(InitialPositionInStream.LATEST));

    assertThat(shards).containsOnly(shard1, shard2, shard3);
  }

  @Test
  public void shouldThrowLimitExceededExceptionForShardListing() {
    shouldThrowShardListingError(
        LimitExceededException.builder().build(), LimitExceededException.class);
  }

  @Test
  public void shouldThrowProvisionedThroughputExceededExceptionForShardListing() {
    shouldThrowShardListingError(
        ProvisionedThroughputExceededException.builder().build(),
        ProvisionedThroughputExceededException.class);
  }

  @Test
  public void shouldThrowServiceErrorForShardListing() {
    shouldThrowShardListingError(
        SdkServiceException.builder().statusCode(HttpStatusCode.GATEWAY_TIMEOUT).build(),
        SdkServiceException.class);
  }

  private void shouldThrowShardListingError(
      Exception thrownException, Class<? extends Exception> expectedExceptionClass) {
    when(kinesis.listShards(any(ListShardsRequest.class))).thenThrow(thrownException);
    try {
      ShardListingUtils.listShardsAtPoint(
          kinesis, STREAM, new StartingPoint(InitialPositionInStream.TRIM_HORIZON));
      failBecauseExceptionWasNotThrown(expectedExceptionClass);
    } catch (Exception e) {
      assertThat(e).isExactlyInstanceOf(expectedExceptionClass);
    } finally {
      reset(kinesis);
    }
  }
}
