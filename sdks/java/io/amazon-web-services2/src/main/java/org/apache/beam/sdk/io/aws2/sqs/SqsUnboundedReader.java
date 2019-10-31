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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

class SqsUnboundedReader extends UnboundedSource.UnboundedReader<Message> implements Serializable {

  public static final int MAX_NUMBER_OF_MESSAGES = 10;
  private final SqsUnboundedSource source;
  private Message current;
  private final Queue<Message> messagesNotYetRead;
  private List<Message> messagesToDelete;
  private Instant oldestPendingTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;

  public SqsUnboundedReader(SqsUnboundedSource source, SqsCheckpointMark sqsCheckpointMark) {
    this.source = source;
    this.current = null;

    this.messagesNotYetRead = new ArrayDeque<>();
    this.messagesToDelete = new ArrayList<>();

    if (sqsCheckpointMark != null) {
      this.messagesToDelete.addAll(sqsCheckpointMark.getMessagesToDelete());
    }
  }

  @Override
  public Instant getWatermark() {
    return oldestPendingTimestamp;
  }

  @Override
  public Message getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  @Override
  public Instant getCurrentTimestamp() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }

    return getTimestamp(current);
  }

  @Override
  public byte[] getCurrentRecordId() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current.messageId().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public CheckpointMark getCheckpointMark() {
    return new SqsCheckpointMark(this, messagesToDelete);
  }

  @Override
  public SqsUnboundedSource getCurrentSource() {
    return source;
  }

  @Override
  public boolean start() {
    return advance();
  }

  @Override
  public boolean advance() {
    if (messagesNotYetRead.isEmpty()) {
      pull();
    }

    current = messagesNotYetRead.poll();
    if (current == null) {
      return false;
    }

    messagesToDelete.add(current);

    Instant currentMessageTimestamp = getCurrentTimestamp();
    if (getCurrentTimestamp().isBefore(oldestPendingTimestamp)) {
      oldestPendingTimestamp = currentMessageTimestamp;
    }

    return true;
  }

  @Override
  public void close() {}

  void delete(final Collection<Message> messages) {
    for (Message message : messages) {
      if (messagesToDelete.contains(message)) {
        DeleteMessageRequest deleteMessageRequest =
            DeleteMessageRequest.builder()
                .queueUrl(source.getRead().queueUrl())
                .receiptHandle(message.receiptHandle())
                .build();

        source.getSqs().deleteMessage(deleteMessageRequest);
        Instant currentMessageTimestamp = getTimestamp(message);
        if (currentMessageTimestamp.isAfter(oldestPendingTimestamp)) {
          oldestPendingTimestamp = currentMessageTimestamp;
        }
      }
    }
  }

  private void pull() {
    final ReceiveMessageRequest receiveMessageRequest =
        ReceiveMessageRequest.builder()
            .maxNumberOfMessages(MAX_NUMBER_OF_MESSAGES)
            .attributeNames(Arrays.asList(QueueAttributeName.CREATED_TIMESTAMP))
            .queueUrl(source.getRead().queueUrl())
            .build();

    final ReceiveMessageResponse receiveMessageResponse =
        source.getSqs().receiveMessage(receiveMessageRequest);

    final List<Message> messages = receiveMessageResponse.messages();

    if (messages == null || messages.isEmpty()) {
      return;
    }

    for (Message message : messages) {
      messagesNotYetRead.add(message);
    }
  }

  private Instant getTimestamp(final Message message) {
    String timeStamp = message.messageAttributes().get(QueueAttributeName.CREATED_TIMESTAMP.toString()).stringValue();
    return new Instant(
        Long.parseLong(timeStamp));
  }
}
