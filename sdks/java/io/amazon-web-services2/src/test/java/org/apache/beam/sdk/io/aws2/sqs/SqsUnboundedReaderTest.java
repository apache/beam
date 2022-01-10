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

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/** Tests on {@link SqsUnboundedReader}. */
@RunWith(JUnit4.class)
public class SqsUnboundedReaderTest {
  private static final String DATA = "testData";

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule public EmbeddedSqsServer embeddedSqsRestServer = new EmbeddedSqsServer();

  private SqsUnboundedSource source;

  private void setupOneMessage() {
    final SqsClient client = embeddedSqsRestServer.getClient();
    final String queueUrl = embeddedSqsRestServer.getQueueUrl();
    client.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(DATA).build());
    source =
        new SqsUnboundedSource(
            SqsIO.read()
                .withQueueUrl(queueUrl)
                .withSqsClientProvider(SqsClientProviderMock.of(client))
                .withMaxNumRecords(1));
  }

  private void setupMessages(List<String> messages) {
    final SqsClient client = embeddedSqsRestServer.getClient();
    final String queueUrl = embeddedSqsRestServer.getQueueUrl();
    for (String message : messages) {
      client.sendMessage(
          SendMessageRequest.builder().queueUrl(queueUrl).messageBody(message).build());
    }
    source =
        new SqsUnboundedSource(
            SqsIO.read()
                .withQueueUrl(queueUrl)
                .withSqsClientProvider(SqsClientProviderMock.of(client))
                .withMaxNumRecords(messages.size()));
  }

  @Test
  public void testReadOneMessage() throws IOException {
    setupOneMessage();
    UnboundedSource.UnboundedReader<SqsMessage> reader =
        source.createReader(pipeline.getOptions(), null);
    // Read one message.
    assertTrue(reader.start());
    assertEquals(DATA, reader.getCurrent().getBody());
    assertFalse(reader.advance());
    // ACK the message.
    UnboundedSource.CheckpointMark checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  @Test
  public void testTimeoutAckAndRereadOneMessage() throws IOException {
    setupOneMessage();
    UnboundedSource.UnboundedReader<SqsMessage> reader =
        source.createReader(pipeline.getOptions(), null);
    SqsClient sqsClient = embeddedSqsRestServer.getClient();
    assertTrue(reader.start());
    assertEquals(DATA, reader.getCurrent().getBody());
    String receiptHandle = reader.getCurrent().getReceiptHandle();
    // Set the message to timeout.
    sqsClient.changeMessageVisibility(
        ChangeMessageVisibilityRequest.builder()
            .queueUrl(source.getRead().queueUrl())
            .receiptHandle(receiptHandle)
            .visibilityTimeout(0)
            .build());
    // We'll now receive the same message again.
    assertTrue(reader.advance());
    assertEquals(DATA, reader.getCurrent().getBody());
    assertFalse(reader.advance());
    // Now ACK the message.
    UnboundedSource.CheckpointMark checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  @Test
  public void testMultipleReaders() throws IOException {
    List<String> incoming = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      incoming.add(String.format("data_%d", i));
    }
    setupMessages(incoming);
    UnboundedSource.UnboundedReader<SqsMessage> reader =
        source.createReader(pipeline.getOptions(), null);
    // Consume two messages, only read one.
    assertTrue(reader.start());
    assertEquals("data_0", reader.getCurrent().getBody());

    // Grab checkpoint.
    SqsCheckpointMark checkpoint = (SqsCheckpointMark) reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    assertEquals(1, checkpoint.notYetReadReceipts.size());

    // Read second message.
    assertTrue(reader.advance());
    assertEquals("data_1", reader.getCurrent().getBody());

    // Restore from checkpoint.
    byte[] checkpointBytes =
        CoderUtils.encodeToByteArray(source.getCheckpointMarkCoder(), checkpoint);
    checkpoint = CoderUtils.decodeFromByteArray(source.getCheckpointMarkCoder(), checkpointBytes);
    assertEquals(1, checkpoint.notYetReadReceipts.size());

    // Re-read second message.
    reader = source.createReader(pipeline.getOptions(), checkpoint);
    assertTrue(reader.start());
    assertEquals("data_1", reader.getCurrent().getBody());

    // We are done.
    assertFalse(reader.advance());

    // ACK final message.
    checkpoint = (SqsCheckpointMark) reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  @Test
  public void testReadMany() throws IOException {

    HashSet<String> messages = new HashSet<>();
    List<String> incoming = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String content = String.format("data_%d", i);
      messages.add(content);
      incoming.add(String.format("data_%d", i));
    }
    setupMessages(incoming);

    SqsUnboundedReader reader =
        (SqsUnboundedReader) source.createReader(pipeline.getOptions(), null);

    for (int i = 0; i < 100; i++) {
      if (i == 0) {
        assertTrue(reader.start());
      } else {
        assertTrue(reader.advance());
      }
      String data = reader.getCurrent().getBody();
      boolean messageNum = messages.remove(data);
      // No duplicate messages.
      assertTrue(messageNum);
    }
    // We are done.
    assertFalse(reader.advance());
    // We saw each message exactly once.
    assertTrue(messages.isEmpty());
    reader.close();
  }

  /** Tests that checkpoints finalized after the reader is closed succeed. */
  @Test
  public void testCloseWithActiveCheckpoints() throws Exception {
    setupOneMessage();
    UnboundedSource.UnboundedReader<SqsMessage> reader =
        source.createReader(pipeline.getOptions(), null);
    reader.start();
    UnboundedSource.CheckpointMark checkpoint = reader.getCheckpointMark();
    reader.close();
    checkpoint.finalizeCheckpoint();
  }
}
