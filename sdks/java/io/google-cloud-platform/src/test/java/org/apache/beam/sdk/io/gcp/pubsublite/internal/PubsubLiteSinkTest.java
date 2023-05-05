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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.io.gcp.pubsublite.PublisherOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

@RunWith(JUnit4.class)
public class PubsubLiteSinkTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  abstract static class PublisherFakeService extends FakeApiService
      implements Publisher<MessageMetadata> {}

  @Spy private PublisherFakeService publisher;

  private PublisherOptions defaultOptions() {
    return PublisherOptions.newBuilder()
        .setTopicPath(
            TopicPath.newBuilder()
                .setProject(ProjectNumber.of(9))
                .setName(TopicName.of("abc"))
                .setLocation(CloudZone.of(CloudRegion.of("us-east1"), 'a'))
                .build())
        .build();
  }

  private final PubsubLiteSink sink = new PubsubLiteSink(defaultOptions());

  @Captor
  final ArgumentCaptor<PubSubMessage> publishedMessageCaptor =
      ArgumentCaptor.forClass(PubSubMessage.class);

  private void runWith(PubSubMessage... messages) {
    pipeline
        .apply(Create.of(Arrays.stream(messages).collect(Collectors.toList())))
        .apply(ParDo.of(sink));
    pipeline.run();
  }

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    PerServerPublisherCache.PUBLISHER_CACHE.set(defaultOptions(), publisher);
  }

  @Test
  public void singleMessagePublishes() throws Exception {
    when(publisher.publish(PubSubMessage.newBuilder().build()))
        .thenReturn(ApiFutures.immediateFuture(MessageMetadata.of(Partition.of(1), Offset.of(2))));
    runWith(PubSubMessage.newBuilder().build());
    verify(publisher).publish(PubSubMessage.newBuilder().build());
  }

  @Test
  public void manyMessagePublishes() throws Exception {
    PubSubMessage message1 = PubSubMessage.newBuilder().build();
    PubSubMessage message2 =
        PubSubMessage.newBuilder().setKey(ByteString.copyFromUtf8("abc")).build();
    when(publisher.publish(message1))
        .thenReturn(ApiFutures.immediateFuture(MessageMetadata.of(Partition.of(1), Offset.of(2))));
    when(publisher.publish(message2))
        .thenReturn(ApiFutures.immediateFuture(MessageMetadata.of(Partition.of(85), Offset.of(3))));
    runWith(message1, message2);
    verify(publisher, times(2)).publish(publishedMessageCaptor.capture());
    assertThat(publishedMessageCaptor.getAllValues(), containsInAnyOrder(message1, message2));
  }

  @Test
  public void singleExceptionWhenProcessing() {
    PubSubMessage message1 = PubSubMessage.newBuilder().build();
    when(publisher.publish(message1))
        .thenReturn(
            ApiFutures.immediateFailedFuture(new CheckedApiException(Code.INTERNAL).underlying));
    PipelineExecutionException e =
        assertThrows(PipelineExecutionException.class, () -> runWith(message1));
    verify(publisher).publish(message1);
    Optional<CheckedApiException> statusOr = ExtractStatus.extract(e.getCause());
    assertTrue(statusOr.isPresent());
    assertThat(statusOr.get().code(), equalTo(Code.INTERNAL));
  }

  @Test
  public void exceptionMixedWithOK() throws Exception {
    PubSubMessage message1 = PubSubMessage.newBuilder().build();
    PubSubMessage message2 =
        PubSubMessage.newBuilder().setKey(ByteString.copyFromUtf8("abc")).build();
    PubSubMessage message3 =
        PubSubMessage.newBuilder().setKey(ByteString.copyFromUtf8("def")).build();
    SettableApiFuture<MessageMetadata> future1 = SettableApiFuture.create();
    SettableApiFuture<MessageMetadata> future2 = SettableApiFuture.create();
    SettableApiFuture<MessageMetadata> future3 = SettableApiFuture.create();
    CountDownLatch startedLatch = new CountDownLatch(3);
    when(publisher.publish(message1))
        .then(
            invocation -> {
              startedLatch.countDown();
              return future1;
            });
    when(publisher.publish(message2))
        .then(
            invocation -> {
              startedLatch.countDown();
              return future2;
            });
    when(publisher.publish(message3))
        .then(
            invocation -> {
              startedLatch.countDown();
              return future3;
            });
    ExecutorService exec = Executors.newCachedThreadPool();
    exec.execute(
        () -> {
          try {
            startedLatch.await();
            future1.set(MessageMetadata.of(Partition.of(1), Offset.of(2)));
            future2.setException(new CheckedApiException(Code.INTERNAL).underlying);
            future3.set(MessageMetadata.of(Partition.of(1), Offset.of(3)));
          } catch (InterruptedException e) {
            fail();
            throw new RuntimeException(e);
          }
        });
    PipelineExecutionException e =
        assertThrows(PipelineExecutionException.class, () -> runWith(message1, message2, message3));
    verify(publisher, times(3)).publish(publishedMessageCaptor.capture());
    assertThat(
        publishedMessageCaptor.getAllValues(), containsInAnyOrder(message1, message2, message3));
    Optional<CheckedApiException> statusOr = ExtractStatus.extract(e.getCause());
    assertTrue(statusOr.isPresent());
    assertThat(statusOr.get().code(), equalTo(Code.INTERNAL));
    exec.shutdownNow();
  }
}
