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
package org.apache.beam.sdk.io.solace;

import com.google.auto.value.AutoValue;
import com.google.common.util.concurrent.SettableFuture;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPProperties;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.beam.sdk.io.solace.MockProducer.MockSuccessProducer;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.MessageProducer;
import org.apache.beam.sdk.io.solace.broker.MessageReceiver;
import org.apache.beam.sdk.io.solace.broker.PublishResultHandler;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.data.Solace.PublishResult;
import org.apache.beam.sdk.io.solace.write.PublishPhaser;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
public abstract class MockSessionService extends SessionService {
  public static int ackWindowSizeForTesting = 87;
  public static boolean callbackOnReactor = true;

  public abstract @Nullable SerializableFunction<Integer, BytesXMLMessage> recordFn();

  public abstract int minMessagesReceived();

  public abstract @Nullable SubmissionMode mode();

  public abstract Function<PublishResultHandler, MockProducer> mockProducerFn();

  private final ConcurrentHashMap<String, PublishPhaser> publishedResultsReceiver = new ConcurrentHashMap<>();

  public static Builder builder() {
    return new AutoValue_MockSessionService.Builder()
        .minMessagesReceived(0)
        .mockProducerFn(MockSuccessProducer::new);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder recordFn(
        @Nullable SerializableFunction<Integer, BytesXMLMessage> recordFn);

    public abstract Builder minMessagesReceived(int minMessagesReceived);

    public abstract Builder mode(@Nullable SubmissionMode mode);

    public abstract Builder mockProducerFn(
        Function<PublishResultHandler, MockProducer> mockProducerFn);

    public abstract MockSessionService build();
  }

  private MessageReceiver messageReceiver = null;
  private MockProducer messageProducer = null;

  @Override
  public void close() {}

  @Override
  public MessageReceiver getReceiver() {
    if (messageReceiver == null) {
      messageReceiver = new MockReceiver(recordFn(), minMessagesReceived());
    }
    return messageReceiver;
  }

  @Override
  public MessageProducer getInitializedProducer(SubmissionMode mode) {
    if (messageProducer == null) {
      messageProducer = mockProducerFn().apply(new PublishResultHandler(publishedResultsReceiver));
    }
    return messageProducer;
  }

  @Override
  public ConcurrentHashMap<String, PublishPhaser> getPublishedResults() {
    return publishedResultsReceiver;
  }

  @Override
  public void connect() {}

  @Override
  public JCSMPProperties initializeSessionProperties(JCSMPProperties baseProperties) {
    // Let's override some properties that will be overriden by the connector
    // Opposite of the mode, to test that is overriden
    baseProperties.setProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR, callbackOnReactor);

    baseProperties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, ackWindowSizeForTesting);

    return baseProperties;
  }

  public static class MockReceiver implements MessageReceiver {
    private final AtomicInteger counter = new AtomicInteger();
    private final SerializableFunction<Integer, BytesXMLMessage> getRecordFn;
    private final int minMessagesReceived;

    public MockReceiver(
        SerializableFunction<Integer, BytesXMLMessage> getRecordFn, int minMessagesReceived) {
      this.getRecordFn = getRecordFn;
      this.minMessagesReceived = minMessagesReceived;
    }

    @Override
    public void start() {}

    @Override
    public BytesXMLMessage receive() throws IOException {
      return getRecordFn.apply(counter.getAndIncrement());
    }

    @Override
    public void close() {}

    @Override
    public boolean isEOF() {
      return counter.get() >= minMessagesReceived;
    }
  }
}
