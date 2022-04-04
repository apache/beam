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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.function.Function.identity;
import static org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.MAX_BYTES_PER_RECORD;
import static org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.MAX_BYTES_PER_REQUEST;
import static org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.MAX_RECORDS_PER_REQUEST;
import static org.apache.beam.sdk.io.common.TestRow.getExpectedValues;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.concat;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.transform;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.AggregatedWriter;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.assertj.core.api.ThrowableAssert;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

/** Tests for {@link KinesisIO#write()}. */
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KinesisIOWriteTest extends PutRecordsHelpers {
  private static final String STREAM = "test";

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Mock public KinesisAsyncClient client;

  @Before
  public void configureClientBuilderFactory() {
    MockClientBuilderFactory.set(pipeline, KinesisAsyncClientBuilder.class, client);
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
    verify(client).close();
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
    verify(client).close();
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
    verify(client).close();
  }

  @Test
  public void testWriteFailure() {
    when(client.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(
            completedFuture(PutRecordsResponse.builder().build()),
            supplyAsync(() -> checkNotNull(null, "putRecords failed")),
            completedFuture(PutRecordsResponse.builder().build()));

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(kinesisWrite().withRecordAggregationDisabled());

    assertThatThrownBy(() -> pipeline.run().waitUntilFinish())
        .isInstanceOf(Pipeline.PipelineExecutionException.class)
        .hasMessageContaining("putRecords failed");

    eventually(3, () -> verify(client).close());
  }

  @Test
  public void testWriteWithPartialSuccess() {
    when(client.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(completedFuture(partialSuccessResponse(70, 30)))
        .thenReturn(completedFuture(partialSuccessResponse(10, 20)))
        .thenReturn(completedFuture(PutRecordsResponse.builder().build()));

    pipeline
        .apply(Create.of(100))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(
            kinesisWrite()
                // .withRetryConfiguration(RetryConfiguration.fixed(10, Duration.millis(1)))
                .withRecordAggregationDisabled());

    pipeline.run().waitUntilFinish();

    InOrder ordered = inOrder(client);
    ordered.verify(client).putRecords(argThat(containsAll(getExpectedValues(0, 100))));
    ordered.verify(client).putRecords(argThat(containsAll(getExpectedValues(70, 100))));
    ordered.verify(client).putRecords(argThat(containsAll(getExpectedValues(80, 100))));
    verify(client).close();
  }

  @Test
  public void testWriteAggregated() {
    when(client.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(completedFuture(PutRecordsResponse.builder().build()));

    pipeline
        .apply(Create.of(100))
        .apply(ParDo.of(new GenerateTestRows()))
        .apply(kinesisWrite().withPartitioner(row -> "a"));

    pipeline.run().waitUntilFinish();
    verify(client).putRecords(argThat(hasSize(1)));
    verify(client).close();
  }

  @Test
  public void testWriteAggregatedWithMaxBytes() {
    when(client.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(completedFuture(PutRecordsResponse.builder().build()));

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
    verify(client).close();
  }

  @Test
  public void testWriteAggregatedWithMaxBytesAndBatchMaxBytes() {
    when(client.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(completedFuture(PutRecordsResponse.builder().build()));

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
    verify(client).close();
  }

  @Test
  public void testWriteAggregatedWithMaxBytesAndBatchMaxRecords() {
    when(client.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(completedFuture(PutRecordsResponse.builder().build()));

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
    verify(client).close();
  }

  @Test
  public void testWriteAggregatedWithMaxBufferTime() throws Throwable {
    when(client.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(completedFuture(PutRecordsResponse.builder().build()));

    Write<TestRow> write =
        kinesisWrite()
            .withPartitioner(r -> r.id().toString())
            .withRecordAggregation(
                b -> b.maxBufferedTime(Duration.millis(100)).maxBufferedTimeJitter(0.2));

    AggregatedWriter<TestRow> writer =
        new AggregatedWriter<>(pipeline.getOptions(), write, write.recordAggregation());

    writer.startBundle();

    DateTimeUtils.setCurrentMillisFixed(0);
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
    ordered
        .verify(client)
        .putRecords(and(argThat(hasSize(3)), argThat(hasPartitions("1", "2", "3"))));
    ordered.verify(client).putRecords(and(argThat(hasSize(2)), argThat(hasPartitions("4", "5"))));
    ordered.verify(client).putRecords(and(argThat(hasSize(1)), argThat(hasPartitions("6"))));
    ordered.verify(client).close();
    verifyNoMoreInteractions(client);
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
    when(mock.putRecords(cap.capture()))
        .thenReturn(completedFuture(PutRecordsResponse.builder().build()));
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

  private void eventually(int attempts, Runnable fun) {
    for (int i = 0; i < attempts - 1; i++) {
      try {
        Thread.sleep(i * 100);
        fun.run();
        return;
      } catch (AssertionError | InterruptedException t) {
      }
    }
    fun.run();
  }
}
