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
package org.apache.beam.sdk.io.aws2.sqs;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.Duration.millis;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.AsyncBatchWriteHandler;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.common.RetryConfiguration;
import org.apache.beam.sdk.io.aws2.sqs.SqsIO.WriteBatches;
import org.apache.beam.sdk.io.aws2.sqs.SqsIO.WriteBatches.EntryMapperFn;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/** Tests for {@link WriteBatches}. */
@RunWith(MockitoJUnitRunner.class)
public class SqsIOWriteBatchesTest {
  private static final EntryMapperFn.Builder<String> SET_MESSAGE_BODY =
      SendMessageBatchRequestEntry.Builder::messageBody;
  private static final SendMessageBatchResponse SUCCESS =
      SendMessageBatchResponse.builder().build();

  @Rule public TestPipeline p = TestPipeline.create();
  @Mock public SqsAsyncClient sqs;
  @Rule public ExpectedLogs logs = ExpectedLogs.none(AsyncBatchWriteHandler.class);

  @Before
  public void configureClientBuilderFactory() {
    MockClientBuilderFactory.set(p, SqsAsyncClientBuilder.class, sqs);
  }

  @Test
  public void testSchemaEntryMapper() throws Exception {
    SchemaRegistry registry = p.getSchemaRegistry();

    Map<String, MessageAttributeValue> attributes =
        ImmutableMap.of("key", MessageAttributeValue.builder().stringValue("value").build());
    Map<String, MessageSystemAttributeValue> systemAttributes =
        ImmutableMap.of(
            "key",
            MessageSystemAttributeValue.builder()
                .binaryValue(SdkBytes.fromString("bytes", UTF_8))
                .build());

    SendMessageRequest input =
        SendMessageRequest.builder()
            .messageBody("body")
            .delaySeconds(3)
            .messageAttributes(attributes)
            .messageSystemAttributesWithStrings(systemAttributes)
            .build();

    SqsIO.WriteBatches.EntryMapperFn<SendMessageRequest> mapper =
        new SqsIO.WriteBatches.SchemaEntryMapper<>(
            registry.getSchema(SendMessageRequest.class),
            registry.getSchema(SendMessageBatchRequestEntry.class),
            registry.getToRowFunction(SendMessageRequest.class),
            registry.getFromRowFunction(SendMessageBatchRequestEntry.class));

    assertThat(mapper.apply("1", input))
        .isEqualTo(
            SendMessageBatchRequestEntry.builder()
                .id("1")
                .messageBody("body")
                .delaySeconds(3)
                .messageAttributes(attributes)
                .messageSystemAttributesWithStrings(systemAttributes)
                .build());
  }

  @Test
  public void testWrite() {
    // write uses writeBatches with batch size 1
    when(sqs.sendMessageBatch(anyRequest())).thenReturn(completedFuture(SUCCESS));

    SendMessageRequest.Builder msgBuilder = SendMessageRequest.builder().queueUrl("queue");
    Set<SendMessageRequest> messages =
        range(0, 100)
            .mapToObj(i -> msgBuilder.messageBody("test" + i).build())
            .collect(Collectors.toSet());

    p.apply(Create.of(messages)).apply(SqsIO.write());
    p.run().waitUntilFinish();

    ArgumentCaptor<SendMessageBatchRequest> captor =
        ArgumentCaptor.forClass(SendMessageBatchRequest.class);
    verify(sqs, times(100)).sendMessageBatch(captor.capture());

    for (SendMessageBatchRequest req : captor.getAllValues()) {
      assertThat(req.queueUrl()).isEqualTo("queue");
      assertThat(req.entries()).hasSize(1);
      for (SendMessageBatchRequestEntry entry : req.entries()) {
        assertTrue(messages.remove(msgBuilder.messageBody(entry.messageBody()).build()));
      }
    }
    assertTrue(messages.isEmpty());
  }

  @Test
  public void testWriteBatches() {
    when(sqs.sendMessageBatch(anyRequest())).thenReturn(completedFuture(SUCCESS));

    p.apply(Create.of(23))
        .apply(ParDo.of(new CreateMessages()))
        .apply(SqsIO.<String>writeBatches().withEntryMapper(SET_MESSAGE_BODY).to("queue"));

    p.run().waitUntilFinish();

    verify(sqs).sendMessageBatch(request("queue", range(0, 10)));
    verify(sqs).sendMessageBatch(request("queue", range(10, 20)));
    verify(sqs).sendMessageBatch(request("queue", range(20, 23)));

    verify(sqs).close();
    verifyNoMoreInteractions(sqs);
  }

  @Test
  public void testWriteBatchesFailure() {
    when(sqs.sendMessageBatch(anyRequest()))
        .thenReturn(
            completedFuture(SUCCESS),
            supplyAsync(() -> checkNotNull(null, "sendMessageBatch failed")),
            completedFuture(SUCCESS));

    p.apply(Create.of(23))
        .apply(ParDo.of(new CreateMessages()))
        .apply(SqsIO.<String>writeBatches().withEntryMapper(SET_MESSAGE_BODY).to("queue"));

    assertThatThrownBy(() -> p.run().waitUntilFinish())
        .isInstanceOf(Pipeline.PipelineExecutionException.class)
        .hasMessageContaining("sendMessageBatch failed");
  }

  @Test
  public void testWriteBatchesPartialSuccess() {
    SendMessageBatchRequestEntry[] entries = entries(range(0, 10));
    when(sqs.sendMessageBatch(anyRequest()))
        .thenReturn(
            completedFuture(partialSuccessResponse(entries[2].id(), entries[3].id())),
            completedFuture(partialSuccessResponse(entries[3].id())),
            completedFuture(SUCCESS));

    p.apply(Create.of(23))
        .apply(ParDo.of(new CreateMessages()))
        .apply(SqsIO.<String>writeBatches().withEntryMapper(SET_MESSAGE_BODY).to("queue"));

    p.run().waitUntilFinish();

    verify(sqs).sendMessageBatch(request("queue", entries));
    verify(sqs).sendMessageBatch(request("queue", entries[2], entries[3]));
    verify(sqs).sendMessageBatch(request("queue", entries[3]));
    verify(sqs).sendMessageBatch(request("queue", range(10, 20)));
    verify(sqs).sendMessageBatch(request("queue", range(20, 23)));

    verify(sqs).close();
    verifyNoMoreInteractions(sqs);

    logs.verifyInfo("retry after partial failure: code REASON for 2 record(s)");
    logs.verifyInfo("retry after partial failure: code REASON for 1 record(s)");
  }

  @Test
  public void testWriteCustomBatches() {
    when(sqs.sendMessageBatch(anyRequest())).thenReturn(completedFuture(SUCCESS));

    p.apply(Create.of(8))
        .apply(ParDo.of(new CreateMessages()))
        .apply(
            SqsIO.<String>writeBatches()
                .withEntryMapper(SET_MESSAGE_BODY)
                .withBatchSize(3)
                .to("queue"));

    p.run().waitUntilFinish();

    verify(sqs).sendMessageBatch(request("queue", range(0, 3)));
    verify(sqs).sendMessageBatch(request("queue", range(3, 6)));
    verify(sqs).sendMessageBatch(request("queue", range(6, 8)));

    verify(sqs).close();
    verifyNoMoreInteractions(sqs);
  }

  @Test
  public void testWriteBatchesWithTimeout() {
    when(sqs.sendMessageBatch(anyRequest())).thenReturn(completedFuture(SUCCESS));

    p.apply(Create.of(5))
        .apply(ParDo.of(new CreateMessages()))
        .apply(
            // simulate delay between messages > batch timeout
            SqsIO.<String>writeBatches()
                .withEntryMapper(withDelay(millis(100), SET_MESSAGE_BODY))
                .withBatchTimeout(millis(150))
                .to("queue"));

    p.run().waitUntilFinish();

    SendMessageBatchRequestEntry[] entries = entries(range(0, 5));
    // due to added delay, batches are timed out on arrival of every 3rd msg
    verify(sqs).sendMessageBatch(request("queue", entries[0], entries[1], entries[2]));
    verify(sqs).sendMessageBatch(request("queue", entries[3], entries[4]));
  }

  @Test
  public void testWriteBatchesToDynamic() {
    when(sqs.sendMessageBatch(anyRequest())).thenReturn(completedFuture(SUCCESS));

    // minimize delay due to retries
    RetryConfiguration retry = RetryConfiguration.builder().maxBackoff(millis(1)).build();

    p.apply(Create.of(10))
        .apply(ParDo.of(new CreateMessages()))
        .apply(
            SqsIO.<String>writeBatches()
                .withEntryMapper(SET_MESSAGE_BODY)
                .withClientConfiguration(ClientConfiguration.builder().retry(retry).build())
                .withBatchSize(3)
                .to(msg -> Integer.valueOf(msg) % 2 == 0 ? "even" : "uneven"));

    p.run().waitUntilFinish();

    // id generator creates ids in range of [0, batch size * (queues + 1))
    SendMessageBatchRequestEntry[] entries = entries(range(0, 9), range(9, 10));

    verify(sqs).sendMessageBatch(request("even", entries[0], entries[2], entries[4]));
    verify(sqs).sendMessageBatch(request("uneven", entries[1], entries[3], entries[5]));
    verify(sqs).sendMessageBatch(request("even", entries[6], entries[8]));
    verify(sqs).sendMessageBatch(request("uneven", entries[7], entries[9]));

    verify(sqs).close();
    verifyNoMoreInteractions(sqs);
  }

  @Test
  public void testWriteBatchesToDynamicWithTimeout() {
    when(sqs.sendMessageBatch(anyRequest())).thenReturn(completedFuture(SUCCESS));

    p.apply(Create.of(5))
        .apply(ParDo.of(new CreateMessages()))
        .apply(
            // simulate delay between messages > batch timeout
            SqsIO.<String>writeBatches()
                .withEntryMapper(withDelay(millis(100), SET_MESSAGE_BODY))
                .withBatchTimeout(millis(150))
                .to(msg -> Integer.valueOf(msg) % 2 == 0 ? "even" : "uneven"));

    p.run().waitUntilFinish();

    SendMessageBatchRequestEntry[] entries = entries(range(0, 5));
    // due to added delay, dynamic batches are timed out on arrival of every 2nd msg (per batch)
    verify(sqs).sendMessageBatch(request("even", entries[0], entries[2]));
    verify(sqs).sendMessageBatch(request("uneven", entries[1], entries[3]));
    verify(sqs).sendMessageBatch(request("even", entries[4]));
  }

  private SendMessageBatchRequest anyRequest() {
    return any();
  }

  private SendMessageBatchRequest request(String queue, SendMessageBatchRequestEntry... entries) {
    return SendMessageBatchRequest.builder()
        .queueUrl(queue)
        .entries(Arrays.asList(entries))
        .build();
  }

  private SendMessageBatchRequest request(String queue, IntStream msgs) {
    return request(queue, entries(msgs));
  }

  private SendMessageBatchRequestEntry[] entries(IntStream... msgStreams) {
    return Arrays.stream(msgStreams)
        .flatMap(msgs -> Streams.mapWithIndex(msgs, this::entry))
        .toArray(SendMessageBatchRequestEntry[]::new);
  }

  private SendMessageBatchRequestEntry entry(int msg, long id) {
    return SendMessageBatchRequestEntry.builder()
        .id(Long.toString(id))
        .messageBody(Integer.toString(msg))
        .build();
  }

  private SendMessageBatchResponse partialSuccessResponse(String... failedIds) {
    Stream<BatchResultErrorEntry> errors =
        Arrays.stream(failedIds)
            .map(BatchResultErrorEntry.builder()::id)
            .map(b -> b.code("REASON").build());
    return SendMessageBatchResponse.builder().failed(errors.collect(toList())).build();
  }

  private static class CreateMessages extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(@Element Integer count, OutputReceiver<String> out) {
      for (int i = 0; i < count; i++) {
        out.output(Integer.toString(i));
      }
    }
  }

  private static <T> EntryMapperFn.Builder<T> withDelay(
      Duration delay, EntryMapperFn.Builder<T> builder) {
    return (t1, t2) -> {
      builder.accept(t1, t2);
      try {
        Thread.sleep(delay.getMillis());
      } catch (InterruptedException e) {
      }
    };
  }
}
