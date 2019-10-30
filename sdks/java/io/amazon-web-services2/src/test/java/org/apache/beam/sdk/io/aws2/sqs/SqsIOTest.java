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

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/** Tests on {@link SqsIO}. */
@RunWith(JUnit4.class)
public class SqsIOTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule public EmbeddedSqsServer embeddedSqsRestServer = new EmbeddedSqsServer();

  @Test
  public void testRead() {
    final SqsClient client = embeddedSqsRestServer.getClient();
    final String queueUrl = embeddedSqsRestServer.getQueueUrl();

    final PCollection<Message> output =
        pipeline.apply(SqsIO.read().withQueueUrl(queueUrl).withMaxNumRecords(100));

    PAssert.thatSingleton(output.apply(Count.globally())).isEqualTo(100L);

    for (int i = 0; i < 100; i++) {
      SendMessageRequest sendMessageRequest =
          SendMessageRequest.builder().queueUrl(queueUrl).messageBody("This is a test").build();
      client.sendMessage(sendMessageRequest);
    }
    pipeline.run();
  }

  @Test
  public void testWrite() {
    final SqsClient client = embeddedSqsRestServer.getClient();
    final String queueUrl = embeddedSqsRestServer.getQueueUrl();

    List<SendMessageRequest> messages = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      final SendMessageRequest request =
          SendMessageRequest.builder()
              .queueUrl(queueUrl)
              .messageBody("This is a test " + i)
              .build();
      messages.add(request);
    }
    pipeline.apply(Create.of(messages)).apply(SqsIO.write());
    pipeline.run().waitUntilFinish();

    List<String> received = new ArrayList<>();
    while (received.size() < 100) {
      ReceiveMessageRequest receiveMessageRequest =
          ReceiveMessageRequest.builder().queueUrl(queueUrl).build();
      final ReceiveMessageResponse receiveMessageResponse =
          client.receiveMessage(receiveMessageRequest);

      if (receiveMessageResponse != null) {
        for (Message message : receiveMessageResponse.messages()) {
          received.add(message.body());
        }
      }
    }
    assertEquals(100, received.size());
    for (int i = 0; i < 100; i++) {
      received.contains("This is a test " + i);
    }
  }

  private static class EmbeddedSqsServer extends ExternalResource {

    private SQSRestServer sqsRestServer;
    private SqsClient client;
    private String queueUrl;

    @Override
    protected void before() {
      sqsRestServer = SQSRestServerBuilder.start();

      String endpoint = "http://localhost:9324";
      String region = "elasticmq";
      String accessKey = "x";
      String secretKey = "x";


      client =
          SqsClient.builder()
              .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
              .endpointOverride(URI.create(endpoint))
              .region(Region.of(region))
              .overrideConfiguration(ClientOverrideConfiguration.builder().build())
              .build();

      CreateQueueRequest createQueueRequest =
          CreateQueueRequest.builder().queueName("test").build();
      final CreateQueueResponse queue = client.createQueue(createQueueRequest);
      queueUrl = queue.queueUrl();
    }

    @Override
    protected void after() {
      sqsRestServer.stopAndWait();
    }

    public SqsClient getClient() {
      return client;
    }

    public String getQueueUrl() {
      return queueUrl;
    }
  }
}
