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

import static java.math.BigInteger.ONE;
import static java.util.Arrays.stream;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.MAX_BYTES_PER_RECORD;
import static org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.MAX_BYTES_PER_REQUEST;
import static org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.MAX_RECORDS_PER_REQUEST;
import static org.apache.beam.sdk.io.aws2.kinesis.KinesisPartitioner.MAX_HASH_KEY;
import static org.apache.beam.sdk.io.aws2.kinesis.KinesisPartitioner.MIN_HASH_KEY;
import static org.apache.beam.sdk.io.aws2.kinesis.KinesisPartitioner.explicitRandomPartitioner;
import static org.apache.beam.sdk.io.common.TestRow.getExpectedValues;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.concat;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.transform;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.Duration.ZERO;
import static org.joda.time.Duration.millis;
import static org.joda.time.Duration.standardSeconds;
import static org.mockito.AdditionalMatchers.and;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.common.RetryConfiguration;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.AggregatedWriter;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.PartitionKeyHasher;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.ShardRanges;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.assertj.core.api.ThrowableAssert;
import org.joda.time.DateTimeUtils;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.Shard;

/** Tests for {@link KinesisIO#write()}. */
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KinesisIOWriteTest extends PutRecordsHelpers {
  private static final String STREAM = "test";

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Mock public KinesisAsyncClient client;

  @Before
  public void configure() {
    MockClientBuilderFactory.set(pipeline, KinesisAsyncClientBuilder.class, client);

    CompletableFuture<ListShardsResponse> errorResp = new CompletableFuture<>();
    errorResp.completeExceptionally(new RuntimeException("Unavailable, retried later"));
    when(client.listShards(any(ListShardsRequest.class))).thenReturn(errorResp);
  }

  @After
  public void resetTimeSource() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  private KinesisIO.Write<TestRow> kinesisWrite() {
    return KinesisIO.<TestRow>write()
        .withStreamName(STREAM)
        .withPartitioner(partitionByName)
        .withSerializer(bytesOfId); // 4 bytes
  }

  @Test
  public void testWrite() {
    Supplier<List<List<TestRow>>> capturedRecords = captureBatchRecords(client);

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(kinesisWrite().withRecordAggregationDisabled());

    pipeline.run().waitUntilFinish();

    List<List<TestRow>> requests = capturedRecords.get();
    assertThat(concat(requests)).containsExactlyInAnyOrderElementsOf(getExpectedValues(0, 100));
  }

  @Test
  public void testWriteWithBatchMaxRecords() {
    Supplier<List<List<TestRow>>> capturedRecords = captureBatchRecords(client);

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(kinesisWrite().withBatchMaxRecords(1).withRecordAggregationDisabled());

    pipeline.run().waitUntilFinish();

    List<List<TestRow>> requests = capturedRecords.get();
    for (List<TestRow> request : requests) {
      assertThat(request.size()).isEqualTo(1);
    }

    assertThat(concat(requests)).containsExactlyInAnyOrderElementsOf(getExpectedValues(0, 100));
  }

  @Test
  public void testWriteWithBatchMaxBytes() {
    Supplier<List<List<TestRow>>> capturedRecords = captureBatchRecords(client);

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(
            // roughly 13 bytes per TestRow
            kinesisWrite().withBatchMaxBytes(15).withRecordAggregationDisabled());

    pipeline.run().waitUntilFinish();

    List<List<TestRow>> requests = capturedRecords.get();
    for (List<TestRow> request : requests) {
      assertThat(request.size()).isEqualTo(1);
    }

    assertThat(concat(requests)).containsExactlyInAnyOrderElementsOf(getExpectedValues(0, 100));
  }

  @Test
  public void testWriteFailure() {
    when(client.putRecords(anyRequest()))
        .thenReturn(
            completedFuture(successResponse),
            supplyAsync(() -> checkNotNull(null, "putRecords failed")),
            completedFuture(successResponse));

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(kinesisWrite().withRecordAggregationDisabled());

    assertThatThrownBy(() -> pipeline.run().waitUntilFinish())
        .isInstanceOf(Pipeline.PipelineExecutionException.class)
        .hasMessageContaining("putRecords failed");
  }

  @Test
  public void testWriteWithPartialSuccess() {
    when(client.putRecords(anyRequest()))
        .thenReturn(completedFuture(partialSuccessResponse(70, 30)))
        .thenReturn(completedFuture(partialSuccessResponse(10, 20)))
        .thenReturn(completedFuture(successResponse));

    // minimize delay due to retries
    RetryConfiguration retry = RetryConfiguration.builder().maxBackoff(millis(1)).build();

    pipeline
        .apply(Create.of(100))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(
            kinesisWrite()
                .withClientConfiguration(ClientConfiguration.builder().retry(retry).build())
                .withRecordAggregationDisabled());

    pipeline.run().waitUntilFinish();

    InOrder ordered = inOrder(client);
    ordered.verify(client).putRecords(argThat(containsAll(getExpectedValues(0, 100))));
    ordered.verify(client).putRecords(argThat(containsAll(getExpectedValues(70, 100))));
    ordered.verify(client).putRecords(argThat(containsAll(getExpectedValues(80, 100))));
  }

  @Test
  public void testWriteAggregatedByDefault() {
    when(client.putRecords(anyRequest())).thenReturn(completedFuture(successResponse));

    pipeline
        .apply(Create.of(100))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(kinesisWrite().withPartitioner(row -> "a"));

    pipeline.run().waitUntilFinish();
    verify(client).putRecords(argThat(hasSize(1)));
  }

  @Test
  public void testWriteAggregatedShardAware() {
    mockShardRanges(MIN_HASH_KEY, MAX_HASH_KEY.shiftRight(1)); // 2 shards
    when(client.putRecords(anyRequest())).thenReturn(completedFuture(successResponse));

    pipeline
        .apply(Create.of(100))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(kinesisWrite().withPartitioner(row -> row.id().toString()));

    pipeline.run().waitUntilFinish();
    verify(client).putRecords(argThat(hasSize(2))); // 1 aggregated record per shard
    verify(client).listShards(any(ListShardsRequest.class));
  }

  @Test
  public void testWriteAggregatedShardRefreshPending() {
    CompletableFuture<ListShardsResponse> resp = new CompletableFuture<>();
    when(client.listShards(any(ListShardsRequest.class))).thenReturn(resp);

    when(client.putRecords(anyRequest())).thenReturn(completedFuture(successResponse));

    pipeline
        .apply(Create.of(100))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(kinesisWrite().withPartitioner(row -> row.id().toString()));

    pipeline.run().waitUntilFinish();
    resp.complete(ListShardsResponse.builder().build()); // complete list shards after pipeline

    // while shards are unknown, each row is aggregated into an individual aggregated record
    verify(client).putRecords(argThat(hasSize(100)));
    verify(client).listShards(any(ListShardsRequest.class));
  }

  @Test
  public void testWriteAggregatedShardRefreshDisabled() {
    when(client.putRecords(anyRequest())).thenReturn(completedFuture(successResponse));

    pipeline
        .apply(Create.of(100))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(
            kinesisWrite()
                .withRecordAggregation(b -> b.shardRefreshInterval(ZERO)) // disable refresh
                .withPartitioner(row -> row.id().toString()));

    pipeline.run().waitUntilFinish();

    // each row is aggregated into an individual aggregated record
    verify(client).putRecords(argThat(hasSize(100)));
    verify(client, times(0)).listShards(any(ListShardsRequest.class)); // disabled
  }

  @Test
  public void testWriteAggregatedUsingExplicitPartitioner() {
    when(client.putRecords(anyRequest())).thenReturn(completedFuture(successResponse));

    pipeline
        .apply(Create.of(100))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(kinesisWrite().withPartitioner(explicitRandomPartitioner(2)));

    pipeline.run().waitUntilFinish();
    verify(client).putRecords(argThat(hasSize(2))); // configuration of partitioner
    verify(client, times(0))
        .listShards(any(ListShardsRequest.class)); // disabled for explicit partitioner
  }

  @Test
  public void testWriteAggregatedWithMaxBytes() {
    when(client.putRecords(anyRequest())).thenReturn(completedFuture(successResponse));

    // overhead protocol + key overhead + 500 records, each 4 bytes data + overhead
    final int expectedBytes = 20 + 3 + 500 * 10;

    pipeline
        .apply(Create.of(1000))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(
            kinesisWrite()
                .withPartitioner(row -> "a")
                .withRecordAggregation(
                    b -> b.maxBytes(expectedBytes).maxBufferedTime(standardSeconds(5))));

    pipeline.run().waitUntilFinish();

    // 2 aggregated records of expectedBytes each
    verify(client).putRecords(and(argThat(hasSize(2)), argThat(hasRecordSize(expectedBytes))));
  }

  @Test
  public void testWriteAggregatedWithMaxBytesAndBatchMaxBytes() {
    when(client.putRecords(anyRequest())).thenReturn(completedFuture(successResponse));

    // overhead protocol + key overhead + 500 records, each 4 bytes data + overhead
    final int expectedBytes = 20 + 3 + 500 * 10;

    pipeline
        .apply(Create.of(1000))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(
            kinesisWrite()
                .withPartitioner(row -> "a")
                .withBatchMaxBytes(expectedBytes + 1) // limit to aggregated data + key of 1 byte
                .withRecordAggregation(
                    b -> b.maxBytes(expectedBytes).maxBufferedTime(standardSeconds(5))));

    pipeline.run().waitUntilFinish();

    // 2 requests with 1 aggregated record of expectedBytes
    verify(client, times(2))
        .putRecords(and(argThat(hasSize(1)), argThat(hasRecordSize(expectedBytes))));
  }

  @Test
  public void testWriteAggregatedWithMaxBytesAndBatchMaxRecords() {
    when(client.putRecords(anyRequest())).thenReturn(completedFuture(successResponse));

    // overhead protocol + key overhead + 500 records, each 4 bytes data + overhead
    final int expectedBytes = 20 + 3 + 500 * 10;

    pipeline
        .apply(Create.of(1000))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(
            kinesisWrite()
                .withPartitioner(row -> "a")
                .withBatchMaxRecords(1) // 1 aggregated record per request
                .withRecordAggregation(
                    b -> b.maxBytes(expectedBytes).maxBufferedTime(standardSeconds(5))));

    pipeline.run().waitUntilFinish();

    // 2 requests with 1 aggregated record of expectedBytes
    verify(client, times(2))
        .putRecords(and(argThat(hasSize(1)), argThat(hasRecordSize(expectedBytes))));
  }

  @Test
  public void testWriteAggregatedWithMaxBufferTime() throws Throwable {
    when(client.putRecords(anyRequest())).thenReturn(completedFuture(successResponse));

    Write<TestRow> write =
        kinesisWrite()
            .withPartitioner(r -> r.id().toString())
            .withRecordAggregation(b -> b.maxBufferedTime(millis(100)).maxBufferedTimeJitter(0.2));

    DateTimeUtils.setCurrentMillisFixed(0);
    AggregatedWriter<TestRow> writer =
        new AggregatedWriter<>(pipeline.getOptions(), write, write.recordAggregation());

    writer.startBundle();

    for (int i = 1; i <= 3; i++) {
      writer.write(TestRow.fromSeed(i));
    }

    // forward clock, don't trigger timeouts yet
    DateTimeUtils.setCurrentMillisFixed(50);
    writer.write(TestRow.fromSeed(4));

    // forward clock to trigger timeout for TestRow 1-3
    DateTimeUtils.setCurrentMillisFixed(100);
    writer.write(TestRow.fromSeed(5));

    // forward clock to trigger timeout for TestRow 4-5
    DateTimeUtils.setCurrentMillisFixed(200);
    writer.write(TestRow.fromSeed(6));

    writer.finishBundle();
    writer.close();

    InOrder ordered = inOrder(client);
    ordered.verify(client).putRecords(argThat(hasPartitions("1", "2", "3")));
    ordered.verify(client).putRecords(argThat(hasPartitions("4", "5")));
    ordered.verify(client).putRecords(argThat(hasPartitions("6")));
    ordered.verify(client).close();
    verifyNoMoreInteractions(client);
  }

  @Test
  public void testWriteAggregatedWithShardsRefresh() throws Throwable {
    when(client.putRecords(anyRequest())).thenReturn(completedFuture(successResponse));

    Write<TestRow> write =
        kinesisWrite()
            .withPartitioner(r -> r.id().toString())
            .withRecordAggregation(b -> b.shardRefreshInterval(millis(1000)));

    DateTimeUtils.setCurrentMillisFixed(1);
    AggregatedWriter<TestRow> writer =
        new AggregatedWriter<>(pipeline.getOptions(), write, write.recordAggregation());

    // initially, no shards known
    for (int i = 1; i <= 3; i++) {
      writer.write(TestRow.fromSeed(i));
    }

    // forward clock, trigger timeouts and refresh shards
    DateTimeUtils.setCurrentMillisFixed(1500);
    mockShardRanges(MIN_HASH_KEY);

    for (int i = 1; i <= 10; i++) {
      writer.write(TestRow.fromSeed(i)); // all aggregated into one record
    }

    writer.finishBundle();
    writer.close();

    InOrder ordered = inOrder(client);
    ordered.verify(client).putRecords(argThat(hasPartitions("1", "2", "3")));
    ordered.verify(client).putRecords(argThat(hasExplicitPartitions(MIN_HASH_KEY.toString())));
    ordered.verify(client).close();
    verify(client, times(2)).listShards(any(ListShardsRequest.class));
    verifyNoMoreInteractions(client);
  }

  @Test
  public void testShardRangesRefresh() {
    BigInteger shard1 = MIN_HASH_KEY;
    BigInteger shard2 = MAX_HASH_KEY.shiftRight(2);
    BigInteger shard3 = MAX_HASH_KEY.shiftRight(1);

    when(client.listShards(argThat(isRequest(STREAM, null))))
        .thenReturn(completedFuture(listShardsResponse("a", shard(shard1))));
    when(client.listShards(argThat(isRequest(null, "a"))))
        .thenReturn(completedFuture(listShardsResponse("b", shard(shard2))));
    when(client.listShards(argThat(isRequest(null, "b"))))
        .thenReturn(completedFuture(listShardsResponse(null, shard(shard3))));

    PartitionKeyHasher pkHasher = new PartitionKeyHasher();
    ShardRanges shardRanges = ShardRanges.of(STREAM);
    shardRanges.refreshPeriodically(client, Instant::now);

    verify(client, times(3)).listShards(any(ListShardsRequest.class));

    BigInteger hashKeyA = pkHasher.hashKey("a");
    assertThat(shardRanges.shardAwareHashKey(hashKeyA)).isEqualTo(shard1);
    assertThat(hashKeyA).isBetween(shard1, shard2.subtract(ONE));

    BigInteger hashKeyB = pkHasher.hashKey("b");
    assertThat(shardRanges.shardAwareHashKey(hashKeyB)).isEqualTo(shard3);
    assertThat(hashKeyB).isBetween(shard3, MAX_HASH_KEY);

    BigInteger hashKeyC = pkHasher.hashKey("c");
    assertThat(shardRanges.shardAwareHashKey(hashKeyC)).isEqualTo(shard2);
    assertThat(hashKeyC).isBetween(shard2, shard3.subtract(ONE));
  }

  @Test
  public void validateMissingStreamName() {
    assertThrown(identity())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("streamName is required");
  }

  @Test
  public void validateEmptyStreamName() {
    assertThrown(w -> w.withStreamName(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("streamName cannot be empty");
  }

  @Test
  public void validateMissingPartitioner() {
    assertThrown(w -> w.withStreamName(STREAM).withSerializer(bytesOfId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("partitioner is required");
  }

  @Test
  public void validateMissingSerializer() {
    assertThrown(w -> w.withStreamName(STREAM).withPartitioner(partitionByName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("serializer is required");
  }

  @Test
  public void validateInvalidConcurrentRequests() {
    assertThrown(w -> w.withConcurrentRequests(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("concurrentRequests must be > 0");
  }

  @Test
  public void validateBatchMaxRecordsTooLow() {
    assertThrown(w -> w.withBatchMaxRecords(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("batchMaxRecords must be in [1," + MAX_RECORDS_PER_REQUEST + "]");
  }

  @Test
  public void validateBatchMaxRecordsTooHigh() {
    assertThrown(w -> w.withBatchMaxRecords(MAX_RECORDS_PER_REQUEST + 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("batchMaxRecords must be in [1," + MAX_RECORDS_PER_REQUEST + "]");
  }

  @Test
  public void validateBatchMaxBytesTooLow() {
    assertThrown(w -> w.withBatchMaxBytes(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("batchMaxBytes must be in [1," + MAX_BYTES_PER_REQUEST + "]");
  }

  @Test
  public void validateBatchMaxBytesTooHigh() {
    assertThrown(w -> w.withBatchMaxBytes(MAX_BYTES_PER_REQUEST + 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("batchMaxBytes must be in [1," + MAX_BYTES_PER_REQUEST + "]");
  }

  @Test
  public void validateRecordAggregationMaxBytesAboveLimit() {
    assertThrown(w -> w.withRecordAggregation(b -> b.maxBytes(MAX_BYTES_PER_RECORD + 1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxBytes must be positive and <= " + MAX_BYTES_PER_RECORD);
  }

  private Shard shard(BigInteger lowerRange) {
    return Shard.builder()
        .hashKeyRange(HashKeyRange.builder().startingHashKey(lowerRange.toString()).build())
        .build();
  }

  private ListShardsResponse listShardsResponse(String nextToken, Shard... shards) {
    return ListShardsResponse.builder().shards(shards).nextToken(nextToken).build();
  }

  protected ArgumentMatcher<ListShardsRequest> isRequest(String stream, String nextToken) {
    return req ->
        req != null
            && Objects.equal(stream, req.streamName())
            && Objects.equal(nextToken, req.nextToken());
  }

  private void mockShardRanges(BigInteger... lowerBounds) {
    List<Shard> shards = stream(lowerBounds).map(lower -> shard(lower)).collect(toList());

    when(client.listShards(any(ListShardsRequest.class)))
        .thenReturn(completedFuture(ListShardsResponse.builder().shards(shards).build()));
  }

  private ThrowableAssert assertThrown(Function<Write<TestRow>, Write<TestRow>> writeConfig) {
    pipeline.enableAbandonedNodeEnforcement(false);
    PCollection<TestRow> input = mock(PCollection.class);
    when(input.getPipeline()).thenReturn(pipeline);
    input.getPipeline(); // satisfy mockito
    return (ThrowableAssert)
        assertThatThrownBy(() -> writeConfig.apply(KinesisIO.write()).expand(input));
  }

  private static KinesisPartitioner<TestRow> partitionByName = row -> row.name();

  private static SerializableFunction<TestRow, byte[]> bytesOfId = row -> bytesOf(row.id());

  private Supplier<List<List<TestRow>>> captureBatchRecords(KinesisAsyncClient mock) {
    ArgumentCaptor<PutRecordsRequest> cap = ArgumentCaptor.forClass(PutRecordsRequest.class);
    when(mock.putRecords(cap.capture())).thenReturn(completedFuture(successResponse));
    return () -> transform(cap.getAllValues(), req -> transform(req.records(), this::toTestRow));
  }

  private static class GenerateTestRows extends DoFn<Integer, TestRow> {
    @ProcessElement
    public void processElement(@Element Integer rowCount, OutputReceiver<TestRow> out) {
      for (TestRow row : getExpectedValues(0, rowCount)) {
        out.output(row);
      }
    }
  }
}
