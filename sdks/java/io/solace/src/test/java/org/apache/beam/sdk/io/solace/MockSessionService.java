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
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.io.solace.broker.MessageReceiver;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class MockSessionService implements SessionService {

  private final SerializableFunction<Integer, BytesXMLMessage> getRecordFn;
  private final AtomicInteger ackCounter;
  private MessageReceiver messageReceiver = null;
  private final int isEndOfStreamAfterCount;

  public MockSessionService(
      SerializableFunction<Integer, BytesXMLMessage> getRecordFn, int isEndOfStreamAfterCount) {
    this.getRecordFn = getRecordFn;
    this.isEndOfStreamAfterCount = isEndOfStreamAfterCount;
    this.ackCounter = new AtomicInteger();
  }

  public MockSessionService(
      SerializableFunction<Integer, BytesXMLMessage> getRecordFn,
      AtomicInteger ackCounter,
      int isEndOfStreamAfterCount) {
    this.getRecordFn = getRecordFn;
    this.isEndOfStreamAfterCount = isEndOfStreamAfterCount;
    this.ackCounter = ackCounter;
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
      messageReceiver = new MockReceiver(getRecordFn, ackCounter, isEndOfStreamAfterCount);
    }
    return messageReceiver;
  }

  @Override
  public void connect() {}

  public static class MockReceiver implements MessageReceiver, Serializable {
    private final AtomicInteger receiveCounter = new AtomicInteger();
    private final AtomicInteger ackCounter;
    private final SerializableFunction<Integer, BytesXMLMessage> getRecordFn;
    private final int isEndOfStreamAfterCount;

    public MockReceiver(
        SerializableFunction<Integer, BytesXMLMessage> getRecordFn,
        AtomicInteger ackCounter,
        int isEndOfStreamAfterCount) {
      this.getRecordFn = getRecordFn;
      this.ackCounter = ackCounter;
      this.isEndOfStreamAfterCount = isEndOfStreamAfterCount;
    }

    @Override
    public void start() {}

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public BytesXMLMessage receive() throws IOException {
      return getRecordFn.apply(receiveCounter.getAndIncrement());
    }

    @Override
    public void ack(long ackId) throws IOException {
      ackCounter.getAndIncrement();
    }

    @Override
    public boolean isEOF() {
      return receiveCounter.get() >= isEndOfStreamAfterCount;
    }
  }
}
