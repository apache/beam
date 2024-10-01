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
package org.apache.beam.playground;

import io.github.embeddedkafka.EmbeddedK;
import io.github.embeddedkafka.EmbeddedKafka$;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import io.github.embeddedkafka.EmbeddedKafkaConfig$;
import scala.collection.immutable.Map;

public class KafkaEmulator {
  public static void main(java.lang.String[] args) {
    EmbeddedKafkaConfig config =
        new EmbeddedKafkaConfig() {
          @Override
          public int numberOfThreads() {
            return 1;
          }

          @Override
          public Map<String, String> customProducerProperties() {
            return EmbeddedKafkaConfig$.MODULE$.defaultConfig().customProducerProperties();
          }

          @Override
          public int zooKeeperPort() {
            return 0;
          }

          @Override
          public Map<String, String> customBrokerProperties() {
            return EmbeddedKafkaConfig$.MODULE$.defaultConfig().customBrokerProperties();
          }

          @Override
          public int kafkaPort() {
            return 0;
          }

          @Override
          public Map<String, String> customConsumerProperties() {
            return EmbeddedKafkaConfig$.MODULE$.defaultConfig().customConsumerProperties();
          }
        };

    EmbeddedK embeddedK = EmbeddedKafka$.MODULE$.start(config);
    System.out.println("Port: " + EmbeddedKafka$.MODULE$.kafkaPort(embeddedK.broker()));

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.out.println("Shutting down...");
                  EmbeddedKafka$.MODULE$.stop();
                }));
  }
}
