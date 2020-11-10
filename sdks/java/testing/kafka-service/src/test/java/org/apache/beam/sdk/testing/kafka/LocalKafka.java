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
package org.apache.beam.sdk.testing.kafka;

import java.nio.file.Files;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class LocalKafka {
  private final KafkaServerStartable server;

  LocalKafka(int kafkaPort, int zookeeperPort) throws Exception {
    Properties kafkaProperties = new Properties();
    kafkaProperties.setProperty("port", String.valueOf(kafkaPort));
    kafkaProperties.setProperty("zookeeper.connect", String.format("localhost:%s", zookeeperPort));
    kafkaProperties.setProperty("offsets.topic.replication.factor", "1");
    kafkaProperties.setProperty("log.dir", Files.createTempDirectory("kafka-log-").toString());
    server = new KafkaServerStartable(KafkaConfig.fromProps(kafkaProperties));
  }

  public void start() {
    server.startup();
  }

  public void stop() {
    server.shutdown();
  }

  public void awaitTermination() {
    server.awaitShutdown();
  }

  public static void main(String[] args) throws Exception {
    int kafkaPort = Integer.parseInt(args[0]);
    int zookeeperPort = Integer.parseInt(args[1]);
    LocalZookeeper zookeeper = new LocalZookeeper(zookeeperPort);
    LocalKafka kafka = new LocalKafka(kafkaPort, zookeeperPort);
    zookeeper.start();
    Thread.sleep(5000);
    kafka.start();
    kafka.awaitTermination();
  }
}
