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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.core.ApiService.Listener;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.internal.wire.Subscriber;
import com.google.cloud.pubsublite.internal.wire.SubscriberFactory;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.FlowControlRequest;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
public class BufferingPullSubscriberTest {
  private final SubscriberFactory underlyingFactory = mock(SubscriberFactory.class);
  private final Subscriber underlying = mock(Subscriber.class);
  private final Offset initialOffset = Offset.of(5);
  private final FlowControlSettings flowControlSettings =
      ((Supplier<FlowControlSettings>)
              () -> {
                try {
                  return FlowControlSettings.builder()
                      .setBytesOutstanding(10)
                      .setMessagesOutstanding(20)
                      .build();
                } catch (StatusException e) {
                  throw e.getStatus().asRuntimeException();
                }
              })
          .get();
  // Initialized in setUp.
  private PullSubscriber subscriber;
  private Consumer<ImmutableList<com.google.cloud.pubsublite.SequencedMessage>> messageConsumer;
  private Listener errorListener;

  private static SequencedMessage newMessage(Timestamp publishTime, Offset offset, long sizeBytes) {
    return SequencedMessage.newBuilder()
        .setMessage(PubSubMessage.getDefaultInstance())
        .setPublishTime(publishTime)
        .setCursor(Cursor.newBuilder().setOffset(offset.value()))
        .setSizeBytes(sizeBytes)
        .build();
  }

  private static com.google.cloud.pubsublite.SequencedMessage toWrapper(SequencedMessage proto) {
    return com.google.cloud.pubsublite.SequencedMessage.fromProto(proto);
  }

  @Before
  public void setUp() throws Exception {
    when(underlying.startAsync()).thenReturn(underlying);
    SeekRequest seek =
        SeekRequest.newBuilder()
            .setCursor(Cursor.newBuilder().setOffset(initialOffset.value()).build())
            .build();
    when(underlying.seek(seek)).thenReturn(ApiFutures.immediateFuture(initialOffset));
    FlowControlRequest flow =
        FlowControlRequest.newBuilder()
            .setAllowedBytes(flowControlSettings.bytesOutstanding())
            .setAllowedMessages(flowControlSettings.messagesOutstanding())
            .build();
    when(underlyingFactory.New(any()))
        .thenAnswer(
            args -> {
              messageConsumer = args.getArgument(0);
              return underlying;
            });
    doAnswer(
            (Answer<Void>)
                args -> {
                  errorListener = args.getArgument(0);
                  return null;
                })
        .when(underlying)
        .addListener(any(), any());

    subscriber = new BufferingPullSubscriber(underlyingFactory, flowControlSettings, initialOffset);

    InOrder inOrder = inOrder(underlyingFactory, underlying);
    inOrder.verify(underlyingFactory).New(any());
    inOrder.verify(underlying).addListener(any(), any());
    inOrder.verify(underlying).startAsync();
    inOrder.verify(underlying).awaitRunning();
    inOrder.verify(underlying).allowFlow(flow);
    inOrder.verify(underlying).seek(seek);

    assertThat(messageConsumer, notNullValue());
    assertThat(errorListener, notNullValue());
  }

  @Test
  public void createDestroy() {}

  @Test
  public void pullAfterErrorThrows() {
    errorListener.failed(null, Status.INTERNAL.asException());
    StatusException e = assertThrows(StatusException.class, subscriber::pull);
    assertThat(e.getStatus().getCode(), equalTo(Code.INTERNAL));
  }

  @Test
  public void emptyPull() throws StatusException {
    assertThat(subscriber.pull(), empty());
  }

  @Test
  public void pullEmptiesForNext() throws StatusException {
    SequencedMessage message1 = newMessage(Timestamps.EPOCH, Offset.of(10), 10);
    SequencedMessage message2 = newMessage(Timestamps.EPOCH, Offset.of(11), 10);
    messageConsumer.accept(ImmutableList.of(toWrapper(message1), toWrapper(message2)));
    assertThat(subscriber.pull(), containsInAnyOrder(message1, message2));
    assertThat(subscriber.pull(), empty());
  }

  @Test
  public void multipleBatchesAggregatedReturnsTokens() throws StatusException {
    SequencedMessage message1 = newMessage(Timestamps.EPOCH, Offset.of(10), 10);
    SequencedMessage message2 = newMessage(Timestamps.EPOCH, Offset.of(11), 20);
    SequencedMessage message3 = newMessage(Timestamps.EPOCH, Offset.of(12), 30);
    messageConsumer.accept(ImmutableList.of(toWrapper(message1), toWrapper(message2)));
    messageConsumer.accept(ImmutableList.of(toWrapper(message3)));
    assertThat(subscriber.pull(), containsInAnyOrder(message1, message2, message3));
    assertThat(subscriber.pull(), empty());

    FlowControlRequest flowControlRequest =
        FlowControlRequest.newBuilder().setAllowedMessages(3).setAllowedBytes(60).build();
    verify(underlying).allowFlow(flowControlRequest);
  }

  @Test
  public void closeStops() throws Exception {
    when(underlying.stopAsync()).thenReturn(underlying);
    subscriber.close();
    verify(underlying).stopAsync();
    verify(underlying).awaitTerminated();
  }
}
