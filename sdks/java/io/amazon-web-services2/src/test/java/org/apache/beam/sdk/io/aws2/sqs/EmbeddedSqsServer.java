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

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

import java.net.URI;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.rules.ExternalResource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

class EmbeddedSqsServer extends ExternalResource {
  private SQSRestServer sqsRestServer;
  private URI endpoint;

  @Override
  protected void before() {
    sqsRestServer = SQSRestServerBuilder.withDynamicPort().start();
    int port = sqsRestServer.waitUntilStarted().localAddress().getPort();
    endpoint = URI.create(String.format("http://localhost:%d", port));
  }

  @Override
  protected void after() {
    sqsRestServer.stopAndWait();
  }

  /** Isolated environment (queue, client) per test case. */
  public static class TestCaseEnv extends ExternalResource {
    private final EmbeddedSqsServer sqsServer;
    private SqsClient client;
    private String queueUrl;

    public TestCaseEnv(EmbeddedSqsServer sqsServer) {
      this.sqsServer = sqsServer;
    }

    @Override
    protected void before() throws Throwable {
      client =
          SqsClient.builder()
              .credentialsProvider(
                  StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
              .endpointOverride(sqsServer.endpoint)
              .region(Region.US_WEST_2)
              .build();

      queueUrl = client.createQueue(b -> b.queueName(randomAlphanumeric(5))).queueUrl();
    }

    @Override
    protected void after() {
      client.close();
    }

    public SqsClient getClient() {
      return client;
    }

    public String getQueueUrl() {
      return queueUrl;
    }
  }
}
