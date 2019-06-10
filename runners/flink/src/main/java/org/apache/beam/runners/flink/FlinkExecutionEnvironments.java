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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for Flink execution environments. */
public class FlinkExecutionEnvironments {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutionEnvironments.class);

  /**
   * If the submitted job is a batch processing job, this method creates the adequate Flink {@link
   * org.apache.flink.api.java.ExecutionEnvironment} depending on the user-specified options.
   */
  public static ExecutionEnvironment createBatchExecutionEnvironment(
      FlinkPipelineOptions options, List<String> filesToStage) {
    return createBatchExecutionEnvironment(options, filesToStage, null);
  }

  @VisibleForTesting
  static ExecutionEnvironment createBatchExecutionEnvironment(
      FlinkPipelineOptions options, List<String> filesToStage, @Nullable String confDir) {

    LOG.info("Creating a Batch Execution Environment.");

    String masterUrl = options.getFlinkMaster();
    ExecutionEnvironment flinkBatchEnv;

    // depending on the master, create the right environment.
    if ("[local]".equals(masterUrl)) {
      flinkBatchEnv = ExecutionEnvironment.createLocalEnvironment();
    } else if ("[collection]".equals(masterUrl)) {
      flinkBatchEnv = new CollectionEnvironment();
    } else if ("[auto]".equals(masterUrl)) {
      flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
    } else if (masterUrl.matches(".*:\\d*")) {
      List<String> parts = Splitter.on(':').splitToList(masterUrl);
      flinkBatchEnv =
          ExecutionEnvironment.createRemoteEnvironment(
              parts.get(0),
              Integer.parseInt(parts.get(1)),
              filesToStage.toArray(new String[filesToStage.size()]));
    } else {
      LOG.warn("Unrecognized Flink Master URL {}. Defaulting to [auto].", masterUrl);
      flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
    }

    // set the correct parallelism.
    if (options.getParallelism() != -1 && !(flinkBatchEnv instanceof CollectionEnvironment)) {
      flinkBatchEnv.setParallelism(options.getParallelism());
    }
    // Set the correct parallelism, required by UnboundedSourceWrapper to generate consistent splits.
    final int parallelism;
    if (flinkBatchEnv instanceof CollectionEnvironment) {
      parallelism = 1;
    } else {
      parallelism =
          determineParallelism(options.getParallelism(), flinkBatchEnv.getParallelism(), confDir);
    }

    flinkBatchEnv.setParallelism(parallelism);
    // set parallelism in the options (required by some execution code)
    options.setParallelism(parallelism);

    if (options.getObjectReuse()) {
      flinkBatchEnv.getConfig().enableObjectReuse();
    } else {
      flinkBatchEnv.getConfig().disableObjectReuse();
    }

    applyLatencyTrackingInterval(flinkBatchEnv.getConfig(), options);

    return flinkBatchEnv;
  }

  /**
   * If the submitted job is a stream processing job, this method creates the adequate Flink {@link
   * org.apache.flink.streaming.api.environment.StreamExecutionEnvironment} depending on the
   * user-specified options.
   */
  public static StreamExecutionEnvironment createStreamExecutionEnvironment(
      FlinkPipelineOptions options, List<String> filesToStage) {
    return createStreamExecutionEnvironment(options, filesToStage, null);
  }

  @VisibleForTesting
  static StreamExecutionEnvironment createStreamExecutionEnvironment(
      FlinkPipelineOptions options, List<String> filesToStage, @Nullable String flinkConfigDir) {

    LOG.info("Creating a Streaming Environment.");

    String masterUrl = options.getFlinkMaster();
    StreamExecutionEnvironment flinkStreamEnv = null;

    // depending on the master, create the right environment.
    if ("[local]".equals(masterUrl)) {
      flinkStreamEnv = StreamExecutionEnvironment.createLocalEnvironment();
    } else if ("[auto]".equals(masterUrl)) {
      flinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    } else if (masterUrl.matches(".*:\\d*")) {
      List<String> parts = Splitter.on(':').splitToList(masterUrl);
      Configuration clientConfig = new Configuration();
      clientConfig.setInteger(RestOptions.PORT, Integer.parseInt(parts.get(1)));
      flinkStreamEnv =
          StreamExecutionEnvironment.createRemoteEnvironment(
              parts.get(0),
              Integer.parseInt(parts.get(1)),
              clientConfig,
              filesToStage.toArray(new String[filesToStage.size()]));
    } else {
      LOG.warn("Unrecognized Flink Master URL {}. Defaulting to [auto].", masterUrl);
      flinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    // Set the parallelism, required by UnboundedSourceWrapper to generate consistent splits.
    final int parallelism =
        determineParallelism(
            options.getParallelism(), flinkStreamEnv.getParallelism(), flinkConfigDir);
    flinkStreamEnv.setParallelism(parallelism);
    // set parallelism in the options (required by some execution code)
    options.setParallelism(parallelism);

    if (options.getObjectReuse()) {
      flinkStreamEnv.getConfig().enableObjectReuse();
    } else {
      flinkStreamEnv.getConfig().disableObjectReuse();
    }

    // default to event time
    flinkStreamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // for the following 2 parameters, a value of -1 means that Flink will use
    // the default values as specified in the configuration.
    int numRetries = options.getNumberOfExecutionRetries();
    if (numRetries != -1) {
      flinkStreamEnv.setNumberOfExecutionRetries(numRetries);
    }
    long retryDelay = options.getExecutionRetryDelay();
    if (retryDelay != -1) {
      flinkStreamEnv.getConfig().setExecutionRetryDelay(retryDelay);
    }

    // A value of -1 corresponds to disabled checkpointing (see CheckpointConfig in Flink).
    // If the value is not -1, then the validity checks are applied.
    // By default, checkpointing is disabled.
    long checkpointInterval = options.getCheckpointingInterval();
    if (checkpointInterval != -1) {
      if (checkpointInterval < 1) {
        throw new IllegalArgumentException("The checkpoint interval must be positive");
      }
      flinkStreamEnv.enableCheckpointing(checkpointInterval, options.getCheckpointingMode());
      if (options.getCheckpointTimeoutMillis() != -1) {
        flinkStreamEnv
            .getCheckpointConfig()
            .setCheckpointTimeout(options.getCheckpointTimeoutMillis());
      }
      boolean externalizedCheckpoint = options.isExternalizedCheckpointsEnabled();
      boolean retainOnCancellation = options.getRetainExternalizedCheckpointsOnCancellation();
      if (externalizedCheckpoint) {
        flinkStreamEnv
            .getCheckpointConfig()
            .enableExternalizedCheckpoints(
                retainOnCancellation
                    ? ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
                    : ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
      }
    }

    applyLatencyTrackingInterval(flinkStreamEnv.getConfig(), options);

    // State backend
    final AbstractStateBackend stateBackend = options.getStateBackend();
    if (stateBackend != null) {
      flinkStreamEnv.setStateBackend(stateBackend);
    }

    return flinkStreamEnv;
  }

  private static int determineParallelism(
      final int pipelineOptionsParallelism,
      final int envParallelism,
      @Nullable String flinkConfDir) {
    if (pipelineOptionsParallelism > 0) {
      return pipelineOptionsParallelism;
    }
    if (envParallelism > 0) {
      // If the user supplies a parallelism on the command-line, this is set on the execution environment during creation
      return envParallelism;
    }

    final Configuration configuration;
    if (flinkConfDir == null) {
      configuration = GlobalConfiguration.loadConfiguration();
    } else {
      configuration = GlobalConfiguration.loadConfiguration(flinkConfDir);
    }
    final int flinkConfigParallelism =
        configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM.key(), -1);
    if (flinkConfigParallelism > 0) {
      return flinkConfigParallelism;
    }
    LOG.warn(
        "No default parallelism could be found. Defaulting to parallelism 1. "
            + "Please set an explicit parallelism with --parallelism");
    return 1;
  }

  private static void applyLatencyTrackingInterval(
      ExecutionConfig config, FlinkPipelineOptions options) {
    long latencyTrackingInterval = options.getLatencyTrackingInterval();
    config.setLatencyTrackingInterval(latencyTrackingInterval);
  }
}
