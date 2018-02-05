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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class that instantiates and manages the execution of a given job.
 * Depending on if the job is a Streaming or Batch processing one, it creates
 * the adequate execution environment ({@link ExecutionEnvironment}
 * or {@link StreamExecutionEnvironment}), the necessary {@link FlinkPipelineTranslator}
 * ({@link FlinkBatchPipelineTranslator} or {@link FlinkStreamingPipelineTranslator}) to
 * transform the Beam job into a Flink one, and executes the (translated) job.
 */
class FlinkPipelineExecutionEnvironment {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlinkPipelineExecutionEnvironment.class);

  private final FlinkPipelineOptions options;

  /**
   * The Flink Batch execution environment. This is instantiated to either a
   * {@link org.apache.flink.api.java.CollectionEnvironment},
   * a {@link org.apache.flink.api.java.LocalEnvironment} or
   * a {@link org.apache.flink.api.java.RemoteEnvironment}, depending on the configuration
   * options.
   */
  private ExecutionEnvironment flinkBatchEnv;

  /**
   * The Flink Streaming execution environment. This is instantiated to either a
   * {@link org.apache.flink.streaming.api.environment.LocalStreamEnvironment} or
   * a {@link org.apache.flink.streaming.api.environment.RemoteStreamEnvironment}, depending
   * on the configuration options, and more specifically, the url of the master.
   */
  private StreamExecutionEnvironment flinkStreamEnv;

  /**
   * Creates a {@link FlinkPipelineExecutionEnvironment} with the user-specified parameters in the
   * provided {@link FlinkPipelineOptions}.
   *
   * @param options the user-defined pipeline options.
   * */
  FlinkPipelineExecutionEnvironment(FlinkPipelineOptions options) {
    this.options = checkNotNull(options);
  }

  /**
   * Depending on if the job is a Streaming or a Batch one, this method creates
   * the necessary execution environment and pipeline translator, and translates
   * the {@link org.apache.beam.sdk.values.PCollection} program into
   * a {@link org.apache.flink.api.java.DataSet}
   * or {@link org.apache.flink.streaming.api.datastream.DataStream} one.
   * */
  public void translate(FlinkRunner flinkRunner, Pipeline pipeline) {
    this.flinkBatchEnv = null;
    this.flinkStreamEnv = null;

    // Serialize and rehydrate pipeline to make sure we only depend serialized transforms.
    try {
      pipeline = PipelineTranslation.fromProto(PipelineTranslation.toProto(pipeline));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    PipelineTranslationOptimizer optimizer =
        new PipelineTranslationOptimizer(TranslationMode.BATCH, options);

    optimizer.translate(pipeline);
    TranslationMode translationMode = optimizer.getTranslationMode();

    pipeline.replaceAll(FlinkTransformOverrides.getDefaultOverrides(
        translationMode == TranslationMode.STREAMING));

    FlinkPipelineTranslator translator;
    if (translationMode == TranslationMode.STREAMING) {
      this.flinkStreamEnv = createStreamExecutionEnvironment();
      translator = new FlinkStreamingPipelineTranslator(flinkRunner, flinkStreamEnv, options);
    } else {
      this.flinkBatchEnv = createBatchExecutionEnvironment();
      translator = new FlinkBatchPipelineTranslator(flinkBatchEnv, options);
    }

    translator.translate(pipeline);
  }

  /**
   * Launches the program execution.
   * */
  public JobExecutionResult executePipeline() throws Exception {
    final String jobName = options.getJobName();

    if (flinkBatchEnv != null) {
      return flinkBatchEnv.execute(jobName);
    } else if (flinkStreamEnv != null) {
      return flinkStreamEnv.execute(jobName);
    } else {
      throw new IllegalStateException("The Pipeline has not yet been translated.");
    }
  }

  /**
   * If the submitted job is a batch processing job, this method creates the adequate
   * Flink {@link org.apache.flink.api.java.ExecutionEnvironment} depending
   * on the user-specified options.
   */
  private ExecutionEnvironment createBatchExecutionEnvironment() {

    LOG.info("Creating the required Batch Execution Environment.");

    String masterUrl = options.getFlinkMaster();
    ExecutionEnvironment flinkBatchEnv;

    // depending on the master, create the right environment.
    if (masterUrl.equals("[local]")) {
      flinkBatchEnv = ExecutionEnvironment.createLocalEnvironment();
    } else if (masterUrl.equals("[collection]")) {
      flinkBatchEnv = new CollectionEnvironment();
    } else if (masterUrl.equals("[auto]")) {
      flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
    } else if (masterUrl.matches(".*:\\d*")) {
      String[] parts = masterUrl.split(":");
      List<String> stagingFiles = options.getFilesToStage();
      flinkBatchEnv = ExecutionEnvironment.createRemoteEnvironment(parts[0],
          Integer.parseInt(parts[1]),
          stagingFiles.toArray(new String[stagingFiles.size()]));
    } else {
      LOG.warn("Unrecognized Flink Master URL {}. Defaulting to [auto].", masterUrl);
      flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
    }

    // set the correct parallelism.
    if (options.getParallelism() != -1 && !(flinkBatchEnv instanceof CollectionEnvironment)) {
      flinkBatchEnv.setParallelism(options.getParallelism());
    }

    // set parallelism in the options (required by some execution code)
    options.setParallelism(flinkBatchEnv.getParallelism());

    if (options.getObjectReuse()) {
      flinkBatchEnv.getConfig().enableObjectReuse();
    } else {
      flinkBatchEnv.getConfig().disableObjectReuse();
    }

    return flinkBatchEnv;
  }

  /**
   * If the submitted job is a stream processing job, this method creates the adequate
   * Flink {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment} depending
   * on the user-specified options.
   */
  private StreamExecutionEnvironment createStreamExecutionEnvironment() {

    LOG.info("Creating the required Streaming Environment.");

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

}
