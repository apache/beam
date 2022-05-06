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
package org.apache.beam.sdk.io.pulsar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class FakePulsarReader implements Reader<byte[]> {

  private String topic;
  private List<FakeMessage> fakeMessages = new ArrayList<>();
  private int currentMsg;
  private long startTimestamp;
  private long endTimestamp;
  private boolean reachedEndOfTopic;
  private int numberOfMessages;

  public FakePulsarReader(String topic, int numberOfMessages) {
    this.numberOfMessages = numberOfMessages;
    this.setMock(topic, numberOfMessages);
  }

  public void setReachedEndOfTopic(boolean hasReachedEnd) {
    this.reachedEndOfTopic = hasReachedEnd;
  }

  public void setMock(String topic, int numberOfMessages) {
    this.topic = topic;
    for (int i = 0; i < numberOfMessages; i++) {
      long timestamp = Instant.now().plus(Duration.standardSeconds(i)).getMillis();
      if (i == 0) {
        startTimestamp = timestamp;
      } else if (i == 99) {
        endTimestamp = timestamp;
      }
      fakeMessages.add(new FakeMessage(topic, timestamp, Long.valueOf(i), Long.valueOf(i), i));
    }
    currentMsg = 0;
  }

  public void reset() {
    this.reachedEndOfTopic = false;
    this.currentMsg = 0;
    emptyMockRecords();
    setMock(topic, numberOfMessages);
  }

  public void emptyMockRecords() {
    this.fakeMessages.clear();
  }

  public long getStartTimestamp() {
    return this.startTimestamp;
  }

  public long getEndTimestamp() {
    return this.endTimestamp;
  }

  @Override
  public String getTopic() {
    return this.topic;
  }

  @Override
  public Message<byte[]> readNext() throws PulsarClientException {
    if (currentMsg == 0 && fakeMessages.isEmpty()) {
      return null;
    }

    Message<byte[]> msg = fakeMessages.get(currentMsg);
    if (currentMsg <= fakeMessages.size() - 1) {
      currentMsg++;
    }
    return msg;
  }

  @Override
  public Message<byte[]> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
    return null;
  }

  @Override
  public CompletableFuture<Message<byte[]>> readNextAsync() {
    return null;
  }

  @Override
  public CompletableFuture<Void> closeAsync() {
    return null;
  }

  @Override
  public boolean hasReachedEndOfTopic() {
    return this.reachedEndOfTopic;
  }

  @Override
  public boolean hasMessageAvailable() throws PulsarClientException {
    return false;
  }

  @Override
  public CompletableFuture<Boolean> hasMessageAvailableAsync() {
    return null;
  }

  @Override
  public boolean isConnected() {
    return false;
  }

  @Override
  public void seek(MessageId messageId) throws PulsarClientException {}

  @Override
  public void seek(long timestamp) throws PulsarClientException {
    for (int i = 0; i < fakeMessages.size(); i++) {
      if (timestamp == fakeMessages.get(i).getPublishTime()) {
        currentMsg = i;
        break;
      }
    }
  }

  @Override
  public CompletableFuture<Void> seekAsync(MessageId messageId) {
    return null;
  }

  @Override
  public CompletableFuture<Void> seekAsync(long timestamp) {
    return null;
  }

  @Override
  public CompletableFuture<Void> seekAsync(Function<String, Object> function) {
    return null;
  }

  @Override
  public void seek(Function<String, Object> function) throws PulsarClientException {}

  @Override
  public void close() throws IOException {}
}
