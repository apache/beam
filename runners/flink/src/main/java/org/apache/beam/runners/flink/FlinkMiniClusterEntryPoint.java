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
package org.apache.beam.runners.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point for starting an embedded Flink cluster. */
public class FlinkMiniClusterEntryPoint {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniClusterEntryPoint.class);

  static class MiniClusterArgs {
    @Option(name = "--rest-port")
    int restPort = 0;

    @Option(name = "--rest-bind-address")
    String restBindAddress = "";

    @Option(name = "--num-task-managers")
    int numTaskManagers = 1;

    @Option(name = "--num-task-slots-per-taskmanager")
    int numSlotsPerTaskManager = 1;
  }

  public static void main(String[] args) throws Exception {
    MiniClusterArgs miniClusterArgs = parseArgs(args);

    Configuration flinkConfig = new Configuration();
    flinkConfig.setInteger(RestOptions.PORT, miniClusterArgs.restPort);
    if (!miniClusterArgs.restBindAddress.isEmpty()) {
      flinkConfig.setString(RestOptions.BIND_ADDRESS, miniClusterArgs.restBindAddress);
    }

    MiniClusterConfiguration clusterConfig =
        new MiniClusterConfiguration.Builder()
            .setConfiguration(flinkConfig)
            .setNumTaskManagers(miniClusterArgs.numTaskManagers)
            .setNumSlotsPerTaskManager(miniClusterArgs.numSlotsPerTaskManager)
            .build();

    try (MiniCluster miniCluster = new MiniCluster(clusterConfig)) {
      miniCluster.start();
      System.out.println(
          String.format(
              "Started Flink mini cluster (%s TaskManagers with %s task slots) with Rest API at %s",
              miniClusterArgs.numTaskManagers,
              miniClusterArgs.numSlotsPerTaskManager,
              miniCluster.getRestAddress()));
      Thread.sleep(Long.MAX_VALUE);
    }
  }

  private static MiniClusterArgs parseArgs(String[] args) {
    MiniClusterArgs configuration = new MiniClusterArgs();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      LOG.error("Unable to parse command line arguments.", e);
      printUsage(parser);
      throw new IllegalArgumentException("Unable to parse command line arguments.", e);
    }
    return configuration;
  }

  private static void printUsage(CmdLineParser parser) {
    System.err.println(
        String.format(
            "Usage: java %s arguments...", FlinkMiniClusterEntryPoint.class.getSimpleName()));
    parser.printUsage(System.err);
    System.err.println();
  }
}
