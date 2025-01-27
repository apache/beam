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
package org.apache.beam.sdk.io.solace.broker;

import com.google.api.client.http.HttpRequestFactory;
import com.solacesystems.jcsmp.JCSMPFactory;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that manages REST calls to the Solace Element Management Protocol (SEMP) using basic
 * authentication.
 *
 * <p>This class provides methods to check necessary information, such as if the queue is
 * non-exclusive, remaining backlog bytes of a queue. It can also create and execute calls to create
 * queue for a topic.
 */
@Internal
public class BasicAuthSempClient implements SempClient {
  private static final Logger LOG = LoggerFactory.getLogger(BasicAuthSempClient.class);

  private final SempBasicAuthClientExecutor sempBasicAuthClientExecutor;

  public BasicAuthSempClient(
      String host,
      String username,
      String password,
      String vpnName,
      SerializableSupplier<HttpRequestFactory> httpRequestFactorySupplier) {
    sempBasicAuthClientExecutor =
        new SempBasicAuthClientExecutor(
            host, username, password, vpnName, httpRequestFactorySupplier.get());
  }

  @Override
  public boolean isQueueNonExclusive(String queueName) throws IOException {
    boolean queueNonExclusive = sempBasicAuthClientExecutor.isQueueNonExclusive(queueName);
    LOG.info(
        "SolaceIO.Read: SempOperations: queried SEMP if queue {} is non-exclusive: {}",
        queueName,
        queueNonExclusive);
    return queueNonExclusive;
  }

  @Override
  public com.solacesystems.jcsmp.Queue createQueueForTopic(String queueName, String topicName)
      throws IOException {
    createQueue(queueName);
    createSubscription(queueName, topicName);
    return JCSMPFactory.onlyInstance().createQueue(queueName);
  }

  @Override
  public long getBacklogBytes(String queueName) throws IOException {
    return sempBasicAuthClientExecutor.getBacklogBytes(queueName);
  }

  private void createQueue(String queueName) throws IOException {
    LOG.info("SolaceIO.Read: Creating new queue {}.", queueName);
    sempBasicAuthClientExecutor.createQueueResponse(queueName);
  }

  private void createSubscription(String queueName, String topicName) throws IOException {
    LOG.info("SolaceIO.Read: Creating new subscription {} for topic {}.", queueName, topicName);
    sempBasicAuthClientExecutor.createSubscriptionResponse(queueName, topicName);
  }
}
