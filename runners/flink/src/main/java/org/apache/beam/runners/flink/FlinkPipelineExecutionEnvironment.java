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

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.runners.core.construction.resources.PipelineResources;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class that instantiates and manages the execution of a given job. Depending on if the job is
 * a Streaming or Batch processing one, it creates the adequate execution environment ({@link
 * ExecutionEnvironment} or {@link StreamExecutionEnvironment}), the necessary {@link
 * FlinkPipelineTranslator} ({@link FlinkBatchPipelineTranslator} or {@link
 * FlinkStreamingPipelineTranslator}) to transform the Beam job into a Flink one, and executes the
 * (translated) job.
 */
class FlinkPipelineExecutionEnvironment {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlinkPipelineExecutionEnvironment.class);

  private final FlinkPipelineOptions options;

  /**
   * The Flink Batch execution environment. This is instantiated to either a {@link
   * org.apache.flink.api.java.CollectionEnvironment}, a {@link
   * org.apache.flink.api.java.LocalEnvironment} or a {@link
   * org.apache.flink.api.java.RemoteEnvironment}, depending on the configuration options.
   */
  private ExecutionEnvironment flinkBatchEnv;

  /**
   * The Flink Streaming execution environment. This is instantiated to either a {@link
   * org.apache.flink.streaming.api.environment.LocalStreamEnvironment} or a {@link
   * org.apache.flink.streaming.api.environment.RemoteStreamEnvironment}, depending on the
   * configuration options, and more specifically, the url of the master.
   */
  private StreamExecutionEnvironment flinkStreamEnv;

  /**
   * Creates a {@link FlinkPipelineExecutionEnvironment} with the user-specified parameters in the
   * provided {@link FlinkPipelineOptions}.
   *
   * @param options the user-defined pipeline options.
   */
  FlinkPipelineExecutionEnvironment(FlinkPipelineOptions options) {
    this.options = checkNotNull(options);
  }

  /**
   * Depending on if the job is a Streaming or a Batch one, this method creates the necessary
   * execution environment and pipeline translator, and translates the {@link
   * org.apache.beam.sdk.values.PCollection} program into a {@link
   * org.apache.flink.api.java.DataSet} or {@link
   * org.apache.flink.streaming.api.datastream.DataStream} one.
   */
  public void translate(Pipeline pipeline) {
    this.flinkBatchEnv = null;
    this.flinkStreamEnv = null;

    final boolean hasUnboundedOutput =
        PipelineTranslationModeOptimizer.hasUnboundedOutput(pipeline);
    if (hasUnboundedOutput) {
      LOG.info("Found unbounded PCollection. Switching to streaming execution.");
      options.setStreaming(true);
    }

    // Staged files need to be set before initializing the execution environments
    prepareFilesToStageForRemoteClusterExecution(options);

    FlinkPipelineTranslator translator;
    if (options.isStreaming()) {
      this.flinkStreamEnv =
          FlinkExecutionEnvironments.createStreamExecutionEnvironment(
              options, options.getFilesToStage());
      if (hasUnboundedOutput && !flinkStreamEnv.getCheckpointConfig().isCheckpointingEnabled()) {
        LOG.warn(
            "UnboundedSources present which rely on checkpointing, but checkpointing is disabled.");
      }
      translator = new FlinkStreamingPipelineTranslator(flinkStreamEnv, options);
    } else {
      this.flinkBatchEnv =
          FlinkExecutionEnvironments.createBatchExecutionEnvironment(
              options, options.getFilesToStage());
      translator = new FlinkBatchPipelineTranslator(flinkBatchEnv, options);
    }

    // Transform replacements need to receive the finalized PipelineOptions
    // including execution mode (batch/streaming) and parallelism.
    pipeline.replaceAll(FlinkTransformOverrides.getDefaultOverrides(options));

    translator.translate(pipeline);
  }

  /**
   * Local configurations work in the same JVM and have no problems with improperly formatted files
   * on classpath (eg. directories with .class files or empty directories). Prepare files for
   * staging only when using remote cluster (passing the master address explicitly).
   */
  private static void prepareFilesToStageForRemoteClusterExecution(FlinkPipelineOptions options) {
    if (!options.getFlinkMaster().matches("\\[auto\\]|\\[collection\\]|\\[local\\]")) {
      if (options.getFilesToStage() == null) {
        options.setFilesToStage(
            detectClassPathResourcesToStage(FlinkRunner.class.getClassLoader(), options));
        LOG.info(
            "PipelineOptions.filesToStage was not specified. "
                + "Defaulting to files from the classpath: will stage {} files. "
                + "Enable logging at DEBUG level to see which files will be staged.",
            options.getFilesToStage().size());
        LOG.debug("Classpath elements: {}", options.getFilesToStage());
      }
      options.setFilesToStage(
          PipelineResources.prepareFilesForStaging(
              options.getFilesToStage(),
              MoreObjects.firstNonNull(
                  options.getTempLocation(), System.getProperty("java.io.tmpdir"))));
    }
  }

  /** Launches the program execution. */
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
   * Retrieves the generated JobGraph which can be submitted against the cluster. For testing
   * purposes.
   */
  @VisibleForTesting
  JobGraph getJobGraph(Pipeline p) {
    translate(p);
    return flinkStreamEnv.getStreamGraph().getJobGraph();
  }

  @VisibleForTesting
  ExecutionEnvironment getBatchExecutionEnvironment() {
    return flinkBatchEnv;
  }

  @VisibleForTesting
  StreamExecutionEnvironment getStreamExecutionEnvironment() {
    return flinkStreamEnv;
  }
}
