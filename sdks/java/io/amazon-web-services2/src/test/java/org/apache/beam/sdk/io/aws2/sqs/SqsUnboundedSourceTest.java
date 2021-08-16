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

import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/** Tests on {@link SqsUnboundedSource}. */
@RunWith(JUnit4.class)
public class SqsUnboundedSourceTest {

  private static final String DATA = "testData";

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule public EmbeddedSqsServer embeddedSqsRestServer = new EmbeddedSqsServer();

  @Test
  public void testCheckpointCoderIsSane() {
    final SqsClient client = embeddedSqsRestServer.getClient();
    final String queueUrl = embeddedSqsRestServer.getQueueUrl();
    client.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(DATA).build());
    SqsUnboundedSource source =
        new SqsUnboundedSource(
            SqsIO.read()
                .withQueueUrl(queueUrl)
                .withSqsClientProvider(SqsClientProviderMock.of(client))
                .withMaxNumRecords(1));
    CoderProperties.coderSerializable(source.getCheckpointMarkCoder());
  }
}
