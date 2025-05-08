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
package org.apache.beam.sdk.io.aws2.sqs.providers;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName.SENT_TIMESTAMP;
import static software.amazon.awssdk.services.sqs.model.QueueAttributeName.VISIBILITY_TIMEOUT;

import java.util.List;
import java.util.function.Consumer;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

@RunWith(MockitoJUnitRunner.class)
public class SqsReadSchemaTransformProviderTest {
  @Rule public TestPipeline p = TestPipeline.create();
  @Mock public SqsClient sqs;

  @Before
  public void configureClientBuilderFactory() {
    MockClientBuilderFactory.set(p, SqsClientBuilder.class, sqs);

    when(sqs.getQueueAttributes(any(Consumer.class)))
        .thenReturn(
            GetQueueAttributesResponse.builder()
                .attributes(ImmutableMap.of(VISIBILITY_TIMEOUT, "600"))
                .build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildTransformWithRowRequiredNotPresent() {
    SqsReadSchemaTransformProvider provider = new SqsReadSchemaTransformProvider();

    Row.withSchema(provider.configurationSchema()).withFieldValue("max_num_records", 10L).build();
  }

  @Test
  public void testBuildTransformWithRow() {
    SqsReadSchemaTransformProvider provider = new SqsReadSchemaTransformProvider();

    Row transformConfigRow =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("queue_url", "https://somedummy.url")
            .withFieldValue("max_read_time_secs", 10L)
            .build();

    provider.from(transformConfigRow);
  }

  @Test
  public void testReadOnce() {
    List<Message> expected = range(0, 10).mapToObj(this::message).collect(toList());

    when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(
            ReceiveMessageResponse.builder().messages(expected).build(),
            ReceiveMessageResponse.builder().build());

    ArgumentCaptor<DeleteMessageBatchRequest> deleteReq =
        ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
    when(sqs.deleteMessageBatch(deleteReq.capture()))
        .thenReturn(DeleteMessageBatchResponse.builder().build());

    SqsReadSchemaTransformProvider provider = new SqsReadSchemaTransformProvider();

    Row readConfig =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("queue_url", "https://somedummy.url")
            .withFieldValue("max_read_time_secs", 10L)
            .build();

    PCollection<Message> result =
        PCollectionRowTuple.empty(p)
            .apply(provider.from(readConfig))
            .get(SqsReadSchemaTransformProvider.OUTPUT_TAG)
            .apply(ParDo.of(new ToMessage()));

    // all expected messages are read
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  private Message message(int i) {
    return Message.builder()
        .messageId("id" + i)
        .body("body" + i)
        .receiptHandle("handle" + i)
        .attributes(ImmutableMap.of(SENT_TIMESTAMP, Integer.toString(i)))
        .build();
  }

  static class ToMessage extends DoFn<Row, Message> {
    @ProcessElement
    public void processElement(@Element Row msg, OutputReceiver<Message> out) {
      out.output(
          Message.builder()
              .messageId(msg.getString("message_id"))
              .body(msg.getString("body"))
              .receiptHandle(msg.getString("receipt_handle"))
              .attributes(ImmutableMap.of(SENT_TIMESTAMP, msg.getInt64("timestamp").toString()))
              .build());
    }
  }
}
