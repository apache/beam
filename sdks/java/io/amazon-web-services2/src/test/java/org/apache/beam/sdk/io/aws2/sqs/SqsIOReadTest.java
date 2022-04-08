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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName.SENT_TIMESTAMP;
import static software.amazon.awssdk.services.sqs.model.QueueAttributeName.VISIBILITY_TIMEOUT;

import java.net.URI;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.sqs.SqsIO.Read;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/** Tests for {@link Read}. */
@RunWith(MockitoJUnitRunner.class)
public class SqsIOReadTest {
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

  @Test
  public void testReadOnce() {
    readOnce(identity());
  }

  @Test
  public void testReadOnceWithLegacyProvider() {
    MockClientBuilderFactory.set(p, SqsClientBuilder.class, null);
    readOnce(read -> read.withSqsClientProvider(StaticSqsClientProvider.of(sqs)));
  }

  private void readOnce(Function<Read, Read> fn) {
    List<Message> expected = range(0, 10).mapToObj(this::message).collect(toList());

    when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(
            ReceiveMessageResponse.builder().messages(expected).build(),
            ReceiveMessageResponse.builder().build());

    ArgumentCaptor<DeleteMessageBatchRequest> deleteReq =
        ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
    when(sqs.deleteMessageBatch(deleteReq.capture()))
        .thenReturn(DeleteMessageBatchResponse.builder().build());

    PCollection<Message> result =
        p.apply(fn.apply(SqsIO.read().withMaxNumRecords(expected.size())))
            .apply(ParDo.of(new ToMessage()));

    // all expected messages are read
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();

    List<String> deletedHandles =
        deleteReq.getAllValues().stream()
            .flatMap(b -> b.entries().stream())
            .map(r -> r.receiptHandle())
            .collect(toList());

    // all messages are deleted
    assertThat(deletedHandles)
        .containsExactlyInAnyOrderElementsOf(Lists.transform(expected, Message::receiptHandle));
  }

  @Test
  public void testBuildWithCredentialsProviderAndRegion() {
    Region region = Region.US_EAST_1;
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

    Read read = SqsIO.read().withSqsClientProvider(credentialsProvider, region.id());
    assertThat(read.clientConfiguration())
        .isEqualTo(ClientConfiguration.create(credentialsProvider, region, null));
  }

  @Test
  public void testBuildWithCredentialsProviderAndRegionAndEndpoint() {
    Region region = Region.US_EAST_1;
    AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
    URI endpoint = URI.create("localhost:9999");

    Read read = SqsIO.read().withSqsClientProvider(credentialsProvider, region.id(), endpoint);
    assertThat(read.clientConfiguration())
        .isEqualTo(ClientConfiguration.create(credentialsProvider, region, endpoint));
  }

  private Message message(int i) {
    return Message.builder()
        .messageId("id" + i)
        .body("body" + i)
        .receiptHandle("handle" + i)
        .attributes(ImmutableMap.of(SENT_TIMESTAMP, Integer.toString(i)))
        .build();
  }

  static class ToMessage extends DoFn<SqsMessage, Message> {
    @ProcessElement
    public void processElement(@Element SqsMessage msg, OutputReceiver<Message> out) {
      out.output(
          Message.builder()
              .messageId(msg.getMessageId())
              .body(msg.getBody())
              .receiptHandle(msg.getReceiptHandle())
              .attributes(ImmutableMap.of(SENT_TIMESTAMP, Long.toString(msg.getTimeStamp())))
              .build());
    }
  }
}
