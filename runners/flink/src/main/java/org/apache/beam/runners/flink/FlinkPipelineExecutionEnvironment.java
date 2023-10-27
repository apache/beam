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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import org.apache.beam.runners.core.construction.resources.PipelineResources;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
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
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
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
    if (options.isStreaming() || options.getUseDataStreamForBatch()) {
      this.flinkStreamEnv = FlinkExecutionEnvironments.createStreamExecutionEnvironment(options);
      if (hasUnboundedOutput && !flinkStreamEnv.getCheckpointConfig().isCheckpointingEnabled()) {
        LOG.warn(
            "UnboundedSources present which rely on checkpointing, but checkpointing is disabled.");
      }
      translator =
          new FlinkStreamingPipelineTranslator(flinkStreamEnv, options, options.isStreaming());
      if (!options.isStreaming()) {
        flinkStreamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
      }
    } else {
      this.flinkBatchEnv = FlinkExecutionEnvironments.createBatchExecutionEnvironment(options);
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
      PipelineResources.prepareFilesForStaging(options);
    }
  }

  /** Launches the program execution. */
  public PipelineResult executePipeline() throws Exception {
    final String jobName = options.getJobName();

    if (flinkBatchEnv != null) {
      if (options.getAttachedMode()) {
        JobExecutionResult jobExecutionResult = flinkBatchEnv.execute(jobName);
        return createAttachedPipelineResult(jobExecutionResult);
      } else {
        JobClient jobClient = flinkBatchEnv.executeAsync(jobName);
        return createDetachedPipelineResult(jobClient, options);
      }
    } else if (flinkStreamEnv != null) {
      if (options.getAttachedMode()) {
        JobExecutionResult jobExecutionResult = flinkStreamEnv.execute(jobName);
        return createAttachedPipelineResult(jobExecutionResult);
      } else {
        JobClient jobClient = flinkStreamEnv.executeAsync(jobName);
        return createDetachedPipelineResult(jobClient, options);
      }
    } else {
      throw new IllegalStateException("The Pipeline has not yet been translated.");
    }
  }

  private FlinkDetachedRunnerResult createDetachedPipelineResult(
      JobClient jobClient, FlinkPipelineOptions options) {
    LOG.info("Pipeline submitted in detached mode");
    return new FlinkDetachedRunnerResult(jobClient, options.getJobCheckIntervalInSecs());
  }

  private FlinkRunnerResult createAttachedPipelineResult(JobExecutionResult result) {
    LOG.info("Execution finished in {} msecs", result.getNetRuntime());
    Map<String, Object> accumulators = result.getAllAccumulatorResults();
    if (accumulators != null && !accumulators.isEmpty()) {
      LOG.info("Final accumulator values:");
      for (Map.Entry<String, Object> entry : result.getAllAccumulatorResults().entrySet()) {
        LOG.info("{} : {}", entry.getKey(), entry.getValue());
      }
    }
    FlinkRunnerResult flinkRunnerResult =
        new FlinkRunnerResult(accumulators, result.getNetRuntime());
    MetricsPusher metricsPusher =
        new MetricsPusher(
            flinkRunnerResult.getMetricsContainerStepMap(),
            options.as(MetricsOptions.class),
            flinkRunnerResult);
    metricsPusher.start();
    return flinkRunnerResult;
  }

  /**
   * Retrieves the generated JobGraph which can be submitted against the cluster. For testing
   * purposes.
   */
  @VisibleForTesting
  JobGraph getJobGraph(Pipeline p) {
    translate(p);
    StreamGraph streamGraph = flinkStreamEnv.getStreamGraph();
    // Normally the job name is set when we execute the job, and JobGraph is immutable, so we need
    // to set the job name here.
    streamGraph.setJobName(p.getOptions().getJobName());
    return streamGraph.getJobGraph();
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
