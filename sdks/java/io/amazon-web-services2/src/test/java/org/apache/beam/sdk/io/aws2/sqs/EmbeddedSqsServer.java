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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;

class EmbeddedSqsServer {
  private static SQSRestServer sqsRestServer;
  private static SqsClient client;
  private static String queueUrl;
  private static int port = 9234;
  private static String endPoint = String.format("http://localhost:%d", port);
  private static String queueName = "test";

  static void start() {
    sqsRestServer = SQSRestServerBuilder.withPort(port).start();

    client =
        SqsClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
            .endpointOverride(URI.create(endPoint))
            .region(Region.US_WEST_2)
            .build();

    CreateQueueRequest createQueueRequest =
        CreateQueueRequest.builder().queueName(queueName).build();
    final CreateQueueResponse queue = client.createQueue(createQueueRequest);
    queueUrl = queue.queueUrl();
  }

  static SqsClient getClient() {
    return client;
  }

  static String getQueueUrl() {
    return queueUrl;
  }

  static void stop() {
    sqsRestServer.stopAndWait();
  }
}
