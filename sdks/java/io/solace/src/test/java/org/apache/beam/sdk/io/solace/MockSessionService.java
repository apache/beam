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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPProperties;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.io.solace.SolaceIO.SubmissionMode;
import org.apache.beam.sdk.io.solace.broker.MessageReceiver;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

public class MockSessionService extends SessionService {

  private final SerializableFunction<Integer, BytesXMLMessage> getRecordFn;
  private MessageReceiver messageReceiver = null;
  private final int minMessagesReceived;
  private final @Nullable SubmissionMode mode;

  public MockSessionService(
      SerializableFunction<Integer, BytesXMLMessage> getRecordFn,
      int minMessagesReceived,
      @Nullable SubmissionMode mode) {
    this.getRecordFn = getRecordFn;
    this.minMessagesReceived = minMessagesReceived;
    this.mode = mode;
  }

  public MockSessionService(
      SerializableFunction<Integer, BytesXMLMessage> getRecordFn, int minMessagesReceived) {
    this(getRecordFn, minMessagesReceived, null);
  }

  @Override
  public void close() {}

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public MessageReceiver createReceiver() {
    if (messageReceiver == null) {
      messageReceiver = new MockReceiver(getRecordFn, minMessagesReceived);
    }
    return messageReceiver;
  }

  @Override
  public void connect() {}

  public static class MockReceiver implements MessageReceiver, Serializable {
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
    public boolean isClosed() {
      return false;
    }

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

  @Override
  public JCSMPProperties initializeSessionProperties(JCSMPProperties baseProperties) {
    // Let's override some properties that will be overriden by the connector
    // Opposite of the mode, to test that is overriden
    baseProperties.setProperty(
        JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR, mode == SubmissionMode.HIGHER_THROUGHPUT);

    baseProperties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 87);

    return baseProperties;
  }
}
