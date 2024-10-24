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
package org.apache.beam.sdk.io.solace.it;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;
import org.testcontainers.utility.DockerImageName;

public class SolaceContainerManager {

  public static final String VPN_NAME = "default";
  public static final String PASSWORD = "password";
  public static final String USERNAME = "username";
  public static final String TOPIC_NAME = "test_topic";
  private static final Logger LOG = LoggerFactory.getLogger(SolaceContainerManager.class);
  private final SolaceContainer container;
  int jcsmpPortMapped = findAvailablePort();
  int sempPortMapped = findAvailablePort();

  public SolaceContainerManager() throws IOException {
    this.container =
        new SolaceContainer(DockerImageName.parse("solace/solace-pubsub-standard:10.7")) {
          {
            addFixedExposedPort(jcsmpPortMapped, 55555);
            addFixedExposedPort(sempPortMapped, 8080);
          }
        }.withVpn(VPN_NAME)
            .withCredentials(USERNAME, PASSWORD)
            .withTopic(TOPIC_NAME, Service.SMF)
            .withLogConsumer(new Slf4jLogConsumer(LOG));
  }

  public void start() {
    container.start();
  }

  void createQueueWithSubscriptionTopic(String queueName) {
    executeCommand(
        "curl",
        "http://localhost:8080/SEMP/v2/config/msgVpns/" + VPN_NAME + "/topicEndpoints",
        "-X",
        "GET",
        "-u",
        "admin:admin");
    executeCommand(
        "curl",
        "http://localhost:8080/SEMP/v2/config/msgVpns/" + VPN_NAME + "/topicEndpoints",
        "-X",
        "POST",
        "-u",
        "admin:admin",
        "-H",
        "Content-Type:application/json",
        "-d",
        "{\"topicEndpointName\":\""
            + TOPIC_NAME
            + "\",\"accessType\":\"exclusive\",\"permission\":\"modify-topic\",\"ingressEnabled\":true,\"egressEnabled\":true}");
    executeCommand(
        "curl",
        "http://localhost:8080/SEMP/v2/config/msgVpns/" + VPN_NAME + "/queues",
        "-X",
        "POST",
        "-u",
        "admin:admin",
        "-H",
        "Content-Type:application/json",
        "-d",
        "{\"queueName\":\""
            + queueName
            + "\",\"accessType\":\"non-exclusive\",\"maxMsgSpoolUsage\":200,\"permission\":\"consume\",\"ingressEnabled\":true,\"egressEnabled\":true}");
    executeCommand(
        "curl",
        "http://localhost:8080/SEMP/v2/config/msgVpns/"
            + VPN_NAME
            + "/queues/"
            + queueName
            + "/subscriptions",
        "-X",
        "POST",
        "-u",
        "admin:admin",
        "-H",
        "Content-Type:application/json",
        "-d",
        "{\"subscriptionTopic\":\"" + TOPIC_NAME + "\"}");
  }

  private void executeCommand(String... command) {
    try {
      ExecResult execResult = container.execInContainer(command);
      if (execResult.getExitCode() != 0) {
        logCommandError(execResult.getStderr(), command);
      } else {
        LOG.info(execResult.getStdout());
      }
    } catch (IOException | InterruptedException e) {
      logCommandError(e.getMessage(), command);
    }
  }

  private void logCommandError(String error, String... command) {
    LOG.error("Could not execute command {}: {}", command, error);
  }

  public void stop() {
    if (container != null) {
      container.stop();
    }
  }

  public void getQueueDetails(String queueName) {
    executeCommand(
        "curl",
        "http://localhost:8080/SEMP/v2/monitor/msgVpns/"
            + VPN_NAME
            + "/queues/"
            + queueName
            + "/msgs",
        "-X",
        "GET",
        "-u",
        "admin:admin");
  }

  public void sendToTopic(String payload, List<String> additionalHeaders) {
    // https://docs.solace.com/API/RESTMessagingPrtl/Solace-REST-Message-Encoding.htm

    List<String> command =
        new ArrayList<>(
            Arrays.asList(
                "curl",
                "http://localhost:9000/TOPIC/" + TOPIC_NAME,
                "-X",
                "POST",
                "-u",
                USERNAME + ":" + PASSWORD,
                "--header",
                "Content-Type:application/json",
                "-d",
                payload));

    for (String additionalHeader : additionalHeaders) {
      command.add("--header");
      command.add(additionalHeader);
    }

    executeCommand(command.toArray(new String[0]));
  }

  private static int findAvailablePort() throws IOException {
    ServerSocket s = new ServerSocket(0);
    try {
      return s.getLocalPort();
    } finally {
      s.close();
      try {
        // Some systems don't free the port for future use immediately.
        Thread.sleep(100);
      } catch (InterruptedException exn) {
        // ignore
      }
    }
  }
}
