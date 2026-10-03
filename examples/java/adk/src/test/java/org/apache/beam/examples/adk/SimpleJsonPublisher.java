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
package org.apache.beam.examples.adk; /*
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

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SimpleJsonPublisher {

  public static void main(String[] args) throws Exception {
    // Replace with your GCP Project ID and Pub/Sub Topic ID
    String projectId = "radoslaws-playground-pso";
    String topicId = "txn";

    TopicName topicName = TopicName.of(projectId, topicId);
    Publisher publisher = null;

    try {

      System.setProperty("google.cloud.project", "radoslaws-playground-pso");
      System.setProperty("otel.exporter.otlp.endpoint", "https://telemetry.googleapis.com");
      System.setProperty("otel.traces.exporter", "otlp");
      System.setProperty("otel.java.global-autoconfigure.enabled", "true");
      System.setProperty("otel.traces.sampler.arg", "1.00");
      System.setProperty("otel.service.name", "TRANSACTION_PRODUCER");
      // Creates a Cloud Trace exporter.

      OpenTelemetry ignored = GlobalOpenTelemetry.get();
      // Create a publisher instance bound to the topic
      publisher =
          Publisher.newBuilder(topicName)
              .setOpenTelemetry(ignored)
              .setEnableOpenTelemetryTracing(true)
              .build();

      // Sample JSON payload matching the Spanner AML transaction format
      String jsonPayload =
          "{\n"
              + "  \"TransactionId\": \""
              + UUID.randomUUID().toString()
              + "\",\n"
              + "  \"SenderId\": \"usr_charlie\",\n"
              + "  \"ReceiverId\": \"usr_alice\",\n"
              + "  \"Amount\": 9500.00,\n"
              + "  \"Timestamp\": \"2026-07-31T12:00:00Z\"\n"
              + "}";

      // Convert JSON string to Pub/Sub ByteString message
      ByteString data = ByteString.copyFrom(jsonPayload, StandardCharsets.UTF_8);
      PubsubMessage pubsubMessage =
          PubsubMessage.newBuilder()
              .setData(data)
              .putAttributes("contentType", "application/json") // Optional metadata
              .build();

      // Publish the message and wait for the message ID callback
      String messageId = publisher.publish(pubsubMessage).get();
      System.out.println("Published JSON message successfully with ID: " + messageId);

    } finally {
      if (publisher != null) {
        // Shut down the publisher and release resources
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
      }
    }
    Thread.sleep(10000);
  }
}
