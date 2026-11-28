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

import static java.lang.System.currentTimeMillis;
import static java.util.stream.IntStream.range;
import static junit.framework.TestCase.assertFalse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.io.aws2.sqs.EmbeddedSqsServer.TestCaseEnv;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/** Tests on {@link SqsUnboundedReader}. */
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class SqsUnboundedReaderTest {
  private static final String DATA = "testData";

  @ClassRule public static EmbeddedSqsServer sqsServer = new EmbeddedSqsServer();

  @Rule public TestCaseEnv testCase = new TestCaseEnv(sqsServer);

  @Mock(answer = RETURNS_DEEP_STUBS)
  public SqsUnboundedSource mockSource;

  private AwsOptions options = PipelineOptionsFactory.create().as(AwsOptions.class);

  private void setupMessages(String... messages) {
    final SqsClient client = testCase.getClient();
    final String queueUrl = testCase.getQueueUrl();
    for (String message : messages) {
      client.sendMessage(b -> b.queueUrl(queueUrl).messageBody(message));
    }

    MockClientBuilderFactory.set(options, SqsClientBuilder.class, client);
    when(mockSource.getRead().queueUrl()).thenReturn(queueUrl);
  }

  @Test
  public void testReadOneMessage() throws IOException {
    setupMessages(DATA);
    SqsUnboundedReader reader = new SqsUnboundedReader(mockSource, null, options);

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
  public void testAckDeletedMessage() throws IOException {
    setupMessages(DATA);
    SqsUnboundedReader reader = new SqsUnboundedReader(mockSource, null, options);

    // Read one message
    assertTrue(reader.start());
    assertEquals(DATA, reader.getCurrent().getBody());
    String receiptHandle = reader.getCurrent().getReceiptHandle();
    assertFalse(reader.advance());

    // Simulate already ACKed message after re-delivery to different reader
    testCase
        .getClient()
        .deleteMessage(b -> b.queueUrl(testCase.getQueueUrl()).receiptHandle(receiptHandle));

    // Now ACK the message.
    UnboundedSource.CheckpointMark checkpoint = reader.getCheckpointMark();
    // Checkpoint can be finalized without failing due to invalid receipt handle
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  @Test
  public void testExtendDeletedMessage() throws IOException {
    setupMessages(DATA);
    Clock clock = mock(Clock.class);
    when(clock.millis()).thenReturn(currentTimeMillis());

    SqsUnboundedReader reader = new SqsUnboundedReader(mockSource, null, options, clock);

    // Read one message
    assertTrue(reader.start());
    assertEquals(DATA, reader.getCurrent().getBody());

    // Simulate already ACKed message after re-delivery to different reader
    String receiptHandle = reader.getCurrent().getReceiptHandle();
    testCase
        .getClient()
        .deleteMessage(b -> b.queueUrl(testCase.getQueueUrl()).receiptHandle(receiptHandle));

    // Forward time to force extension of visibility
    when(clock.millis()).thenReturn(currentTimeMillis() + reader.getVisibilityTimeoutMs() * 8 / 10);

    // Advancing the reader will attempt extending the visibility of the only message received and
    // succeeds despite the invalid receipt handle, but there's no further message.
    assertFalse(reader.advance());
    reader.close();
  }

  @Test
  public void testRereadExpiredMessage() throws IOException {
    setupMessages(DATA);
    SqsUnboundedReader reader = new SqsUnboundedReader(mockSource, null, options);

    // Read one message
    assertTrue(reader.start());
    assertEquals(DATA, reader.getCurrent().getBody());
    String receiptHandle = reader.getCurrent().getReceiptHandle();

    // Expire the message to simulate some delay in processing
    testCase
        .getClient()
        .changeMessageVisibility(
            b ->
                b.queueUrl(testCase.getQueueUrl())
                    .receiptHandle(receiptHandle)
                    .visibilityTimeout(0));

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
  public void testRestoreReaderFromCheckpoint() throws IOException {
    setupMessages("data_0", "data_1");
    SqsUnboundedReader reader = new SqsUnboundedReader(mockSource, null, options);
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
    SerializableCoder<SqsCheckpointMark> coder = SerializableCoder.of(SqsCheckpointMark.class);
    byte[] checkpointBytes = CoderUtils.encodeToByteArray(coder, checkpoint);
    checkpoint = CoderUtils.decodeFromByteArray(coder, checkpointBytes);
    assertEquals(1, checkpoint.notYetReadReceipts.size());

    // Re-read second message.
    reader = new SqsUnboundedReader(mockSource, checkpoint, options);
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
  public void testReadManyMessages() throws IOException {
    List<String> receivedMessages = new ArrayList<>();
    setupMessages(range(0, 100).mapToObj(Integer::toString).toArray(String[]::new));

    SqsUnboundedReader reader = new SqsUnboundedReader(mockSource, null, options);

    assertTrue(reader.start());
    do {
      receivedMessages.add(reader.getCurrent().getBody());
    } while (reader.advance());

    assertThat(receivedMessages).hasSize(100);
    assertThat(receivedMessages).doesNotHaveDuplicates();

    // ACK all messages
    UnboundedSource.CheckpointMark checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  /** Tests that checkpoints finalized after the reader is closed succeed. */
  @Test
  public void testCloseWithActiveCheckpoints() throws Exception {
    setupMessages(DATA);
    SqsUnboundedReader reader = new SqsUnboundedReader(mockSource, null, options);
    reader.start();
    UnboundedSource.CheckpointMark checkpoint = reader.getCheckpointMark();
    reader.close();
    checkpoint.finalizeCheckpoint();
    assertEquals(0, reader.numInFlightCheckpoints.get());
  }

  @Test
  public void testRestoredCheckpointDeletesMessagesAndCatchesUpEarlierCheckpoints()
      throws Exception {
    setupMessages("msg_0", "msg_1", "msg_2");
    SqsUnboundedReader reader = new SqsUnboundedReader(mockSource, null, options);
    SerializableCoder<SqsCheckpointMark> coder = SerializableCoder.of(SqsCheckpointMark.class);

    assertTrue(reader.start());
    assertEquals("msg_0", reader.getCurrent().getBody());
    // Take checkpoint 1 and leave it unfinalized.
    SqsCheckpointMark cp1 = (SqsCheckpointMark) reader.getCheckpointMark();

    assertTrue(reader.advance());
    assertEquals("msg_1", reader.getCurrent().getBody());
    SqsCheckpointMark cp2 = (SqsCheckpointMark) reader.getCheckpointMark();

    assertEquals(2, reader.numInFlightCheckpoints.get());

    // Finalize a restored copy of cp2 without finalizing cp1.
    SqsCheckpointMark restoredCp2 = CoderUtils.clone(coder, cp2);
    restoredCp2.finalizeCheckpoint();

    // Both cp1 and cp2 should be accounted for; only msg_2 remains in flight (not visible).
    assertEquals(0, reader.numInFlightCheckpoints.get());
    assertEquals(
        "1",
        testCase
            .getClient()
            .getQueueAttributes(
                b ->
                    b.queueUrl(testCase.getQueueUrl())
                        .attributeNames(
                            QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE))
            .attributes()
            .get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));

    // Read msg_2 and resume from a restored checkpoint via SqsUnboundedSource.createReader.
    assertTrue(reader.advance());
    assertEquals("msg_2", reader.getCurrent().getBody());
    SqsCheckpointMark cp3 = (SqsCheckpointMark) reader.getCheckpointMark();
    SqsCheckpointMark restoredCp3 = CoderUtils.clone(coder, cp3);

    SqsUnboundedSource source = new SqsUnboundedSource(mockSource.getRead());
    UnboundedSource.UnboundedReader<SqsMessage> resumed = source.createReader(options, restoredCp3);
    assertEquals(0, reader.numInFlightCheckpoints.get());

    // Subsequent finalize on already-caught-up cp1 is a no-op.
    cp1.finalizeCheckpoint();
    assertEquals(0, reader.numInFlightCheckpoints.get());

    assertEquals(
        "0",
        testCase
            .getClient()
            .getQueueAttributes(
                b ->
                    b.queueUrl(testCase.getQueueUrl())
                        .attributeNames(
                            QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE))
            .attributes()
            .get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));

    resumed.close();
    reader.close();
  }
}
