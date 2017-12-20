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

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FlinkPipelineExecutor} that executes a {@link Pipeline} using a Flink
 * {@link StreamExecutionEnvironment}.
 */
class FlinkStreamingPipelineExecutor implements FlinkPipelineExecutor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlinkStreamingPipelineExecutor.class);

  @Override
  public PipelineResult executePipeline(
      FlinkRunner runner, Pipeline pipeline, FlinkPipelineOptions options) throws Exception {

    StreamExecutionEnvironment env = createStreamExecutionEnvironment(options);

    FlinkStreamingPipelineTranslator translator =
        new FlinkStreamingPipelineTranslator(runner, env, options);
    translator.translate(pipeline);

    return runPipeline(options, env);
  }

  private StreamExecutionEnvironment createStreamExecutionEnvironment(
      FlinkPipelineOptions options) {

    String masterUrl = options.getFlinkMaster();
    StreamExecutionEnvironment flinkStreamEnv = null;

    // depending on the master, create the right environment.
    if (masterUrl.equals("[local]")) {
      flinkStreamEnv = StreamExecutionEnvironment.createLocalEnvironment();
    } else if (masterUrl.equals("[auto]")) {
      flinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    } else if (masterUrl.matches(".*:\\d*")) {
      String[] parts = masterUrl.split(":");
      List<String> stagingFiles = options.getFilesToStage();
      flinkStreamEnv = StreamExecutionEnvironment.createRemoteEnvironment(parts[0],
          Integer.parseInt(parts[1]), stagingFiles.toArray(new String[stagingFiles.size()]));
    } else {
      LOG.warn("Unrecognized Flink Master URL {}. Defaulting to [auto].", masterUrl);
      flinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    // set the correct parallelism.
    if (options.getParallelism() != -1) {
      flinkStreamEnv.setParallelism(options.getParallelism());
    }

    // set parallelism in the options (required by some execution code)
    options.setParallelism(flinkStreamEnv.getParallelism());

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
      flinkStreamEnv.getCheckpointConfig().setCheckpointTimeout(
          options.getCheckpointTimeoutMillis());
      boolean externalizedCheckpoint = options.isExternalizedCheckpointsEnabled();
      boolean retainOnCancellation = options.getRetainExternalizedCheckpointsOnCancellation();
      if (externalizedCheckpoint) {
        flinkStreamEnv.getCheckpointConfig().enableExternalizedCheckpoints(
            retainOnCancellation ? ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
                : ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
      }
    }

    // State backend
    final AbstractStateBackend stateBackend = options.getStateBackend();
    if (stateBackend != null) {
      flinkStreamEnv.setStateBackend(stateBackend);
    }

    return flinkStreamEnv;
  }

  /**
   * This will use blocking submission so the job-control features of {@link PipelineResult} don't
   * work.
   */
  private static PipelineResult runPipeline(
      FlinkPipelineOptions options, StreamExecutionEnvironment env) throws Exception {

    JobExecutionResult jobResult = env.execute(options.getJobName());

    return FlinkRunnerResultUtil.wrapFlinkRunnerResult(LOG, jobResult);
  }
}
