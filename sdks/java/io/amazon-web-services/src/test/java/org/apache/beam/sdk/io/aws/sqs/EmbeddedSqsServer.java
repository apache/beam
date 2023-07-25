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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.rules.ExternalResource;

class EmbeddedSqsServer extends ExternalResource {

  private SQSRestServer sqsRestServer;
  private AmazonSQS client;
  private String queueUrl;

  @Override
  protected void before() {
    sqsRestServer = SQSRestServerBuilder.withDynamicPort().start();
    int port = sqsRestServer.waitUntilStarted().localAddress().getPort();

    String endpoint = String.format("http://localhost:%d", port);
    String region = "elasticmq";
    String accessKey = "x";
    String secretKey = "x";

    client =
        AmazonSQSClientBuilder.standard()
            .withCredentials(
                new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
            .build();
    final CreateQueueResult queue = client.createQueue("test");
    queueUrl = queue.getQueueUrl();
  }

  @Override
  protected void after() {
    sqsRestServer.stopAndWait();
    client.shutdown();
  }

  public AmazonSQS getClient() {
    return client;
  }

  public String getQueueUrl() {
    return queueUrl;
  }
}
