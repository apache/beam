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
package org.apache.beam.sdk.io.aws.sqs;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.CoderUtils;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests on {@link SqsIO}. */
@RunWith(JUnit4.class)
public class SqsIOTest {
  private static final String DATA = "testData";

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule public EmbeddedSqsServer embeddedSqsRestServer = new EmbeddedSqsServer();

  private SqsUnboundedSource source;

  private void setupOneMessage() {
    final AmazonSQS client = embeddedSqsRestServer.getClient();
    final String queueUrl = embeddedSqsRestServer.getQueueUrl();
    client.sendMessage(queueUrl, DATA);
    source =
        new SqsUnboundedSource(
            SqsIO.read().withQueueUrl(queueUrl).withMaxNumRecords(1),
            new SqsConfiguration(pipeline.getOptions().as(AwsOptions.class)));
  }

  private void setupMessages(List<String> messages) {
    final AmazonSQS client = embeddedSqsRestServer.getClient();
    final String queueUrl = embeddedSqsRestServer.getQueueUrl();
    for (String message : messages) {
      client.sendMessage(queueUrl, message);
    }
    source =
        new SqsUnboundedSource(
            SqsIO.read().withQueueUrl(queueUrl).withMaxNumRecords(1),
            new SqsConfiguration(pipeline.getOptions().as(AwsOptions.class)));
  }

  @Test
  public void testCheckpointCoderIsSane() {
    setupOneMessage();
    CoderProperties.coderSerializable(source.getCheckpointMarkCoder());
    // Since we only serialize/deserialize the 'notYetReadIds', and we don't want to make
    // equals on checkpoints ignore those fields, we'll test serialization and deserialization
    // of checkpoints in multipleReaders below.
  }

  @Test
  public void testReadOneMessage() throws IOException {
    setupOneMessage();
    UnboundedReader<Message> reader = source.createReader(pipeline.getOptions(), null);
    // Read one message.
    assertTrue(reader.start());
    assertEquals(DATA, reader.getCurrent().getBody());
    assertFalse(reader.advance());
    // ACK the message.
    CheckpointMark checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader.close();
  }

  @Test
  public void testTimeoutAckAndRereadOneMessage() throws IOException {
    setupOneMessage();
    UnboundedReader<Message> reader = source.createReader(pipeline.getOptions(), null);
    AmazonSQS sqsClient = source.getSqs();
    assertTrue(reader.start());
    assertEquals(DATA, reader.getCurrent().getBody());
    String receiptHandle = reader.getCurrent().getReceiptHandle();
    // Set the message to timeout.
    sqsClient.changeMessageVisibility(source.getRead().queueUrl(), receiptHandle, 0);
    // We'll now receive the same message again.
    assertTrue(reader.advance());
    assertEquals(DATA, reader.getCurrent().getBody());
    assertFalse(reader.advance());
    // Now ACK the message.
    CheckpointMark checkpoint = reader.getCheckpointMark();
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
    UnboundedReader<Message> reader = source.createReader(pipeline.getOptions(), null);
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

  @Test
  public void testWrite() {
    final AmazonSQS client = embeddedSqsRestServer.getClient();
    final String queueUrl = embeddedSqsRestServer.getQueueUrl();

    List<SendMessageRequest> messages = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      final SendMessageRequest request = new SendMessageRequest(queueUrl, "This is a test " + i);
      messages.add(request);
    }
    pipeline.apply(Create.of(messages)).apply(SqsIO.write());
    pipeline.run().waitUntilFinish();

    List<String> received = new ArrayList<>();
    while (received.size() < 100) {
      final ReceiveMessageResult receiveMessageResult = client.receiveMessage(queueUrl);

      if (receiveMessageResult.getMessages() != null) {
        for (Message message : receiveMessageResult.getMessages()) {
          received.add(message.getBody());
        }
      }
    }
    assertEquals(100, received.size());
    for (int i = 0; i < 100; i++) {
      received.contains("This is a test " + i);
    }
  }

  /** Tests that checkpoints finalized after the reader is closed succeed. */
  @Test
  public void testCloseWithActiveCheckpoints() throws Exception {
    setupOneMessage();
    UnboundedReader<Message> reader = source.createReader(pipeline.getOptions(), null);
    reader.start();
    CheckpointMark checkpoint = reader.getCheckpointMark();
    reader.close();
    checkpoint.finalizeCheckpoint();
  }

  private static class EmbeddedSqsServer extends ExternalResource {

    private SQSRestServer sqsRestServer;
    private AmazonSQS client;
    private String queueUrl;

    @Override
    protected void before() {
      sqsRestServer = SQSRestServerBuilder.start();

      String endpoint = "http://localhost:9324";
      String region = "elasticmq";
      String accessKey = "x";
      String secretKey = "x";

      client =
          AmazonSQSClientBuilder.standard()
              .withCredentials(
                  new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
              .withEndpointConfiguration(
                  new AwsClientBuilder.EndpointConfiguration(endpoint, region))
              .build();
      final CreateQueueResult queue = client.createQueue("test");
      queueUrl = queue.getQueueUrl();
    }

    @Override
    protected void after() {
      sqsRestServer.stopAndWait();
    }

    public AmazonSQS getClient() {
      return client;
    }

    public String getQueueUrl() {
      return queueUrl;
    }
  }
}
