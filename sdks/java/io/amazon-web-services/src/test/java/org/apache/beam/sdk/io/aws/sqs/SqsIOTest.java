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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests on {@link SqsIO}. */
@RunWith(JUnit4.class)
public class SqsIOTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule public EmbeddedSqsServer embeddedSqsRestServer = new EmbeddedSqsServer();

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
      assertTrue(received.contains("This is a test " + i));
    }
  }
}
