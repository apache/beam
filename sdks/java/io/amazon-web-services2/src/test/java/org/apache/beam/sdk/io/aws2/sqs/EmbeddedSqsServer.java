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

import java.net.URI;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.rules.ExternalResource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;

class EmbeddedSqsServer extends ExternalResource {
  private static SQSRestServer sqsRestServer;
  private static SqsClient client;
  private static String queueUrl;
  private static String queueName = "test";

  @Override
  protected void before() {
    sqsRestServer = SQSRestServerBuilder.withDynamicPort().start();
    int port = sqsRestServer.waitUntilStarted().localAddress().getPort();
    client =
        SqsClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
            .endpointOverride(URI.create(String.format("http://localhost:%d", port)))
            .region(Region.US_WEST_2)
            .build();

    CreateQueueRequest createQueueRequest =
        CreateQueueRequest.builder().queueName(queueName).build();
    final CreateQueueResponse queue = client.createQueue(createQueueRequest);
    queueUrl = queue.queueUrl();
  }

  public SqsClient getClient() {
    return client;
  }

  public String getQueueUrl() {
    return queueUrl;
  }

  @Override
  protected void after() {
    sqsRestServer.stopAndWait();
    client.close();
  }
}
