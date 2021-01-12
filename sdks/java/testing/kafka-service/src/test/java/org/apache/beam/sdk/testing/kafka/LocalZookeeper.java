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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalZookeeper {
  private static final Logger LOG = LoggerFactory.getLogger(LocalZookeeper.class);
  private final ServerConfig serverConfig;
  private final Executor executor;

  LocalZookeeper(int port) throws Exception {
    Properties localProperties = new Properties();
    localProperties.setProperty("clientPort", String.valueOf(port));
    localProperties.setProperty("dataDir", Files.createTempDirectory("zookeeper-").toString());
    QuorumPeerConfig config = new QuorumPeerConfig();
    config.parseProperties(localProperties);
    serverConfig = new ServerConfig();
    serverConfig.readFrom(config);
    executor = Executors.newSingleThreadExecutor();
  }

  public void start() {
    executor.execute(
        () -> {
          try {
            new ZooKeeperServerMain().runFromConfig(serverConfig);
          } catch (Exception e) {
            LOG.error("local zookeeper failure.", e);
          }
        });
  }
}
