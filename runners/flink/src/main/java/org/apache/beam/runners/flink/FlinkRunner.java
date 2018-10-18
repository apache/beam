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

import static org.apache.beam.runners.core.construction.PipelineResources.detectClassPathResourcesToStage;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.program.DetachedEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that executes the operations in the pipeline by first translating them
 * to a Flink Plan and then executing them either locally or on a Flink cluster, depending on the
 * configuration.
 */
public class FlinkRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkRunner.class);

  /** Provided options. */
  private final FlinkPipelineOptions options;

  /**
   * Construct a runner from the provided options.
   *
   * @param options Properties which configure the runner.
   * @return The newly created runner.
   */
  public static FlinkRunner fromOptions(PipelineOptions options) {
    FlinkPipelineOptions flinkOptions =
        PipelineOptionsValidator.validate(FlinkPipelineOptions.class, options);
    ArrayList<String> missing = new ArrayList<>();

    if (flinkOptions.getAppName() == null) {
      missing.add("appName");
    }
    if (missing.size() > 0) {
      throw new IllegalArgumentException(
          "Missing required values: " + Joiner.on(',').join(missing));
    }

    if (flinkOptions.getFilesToStage() == null) {
      flinkOptions.setFilesToStage(
          detectClassPathResourcesToStage(FlinkRunner.class.getClassLoader()));
      LOG.info(
          "PipelineOptions.filesToStage was not specified. "
              + "Defaulting to files from the classpath: will stage {} files. "
              + "Enable logging at DEBUG level to see which files will be staged.",
          flinkOptions.getFilesToStage().size());
      LOG.debug("Classpath elements: {}", flinkOptions.getFilesToStage());
    }

    return new FlinkRunner(flinkOptions);
  }

  private FlinkRunner(FlinkPipelineOptions options) {
    this.options = options;
    this.ptransformViewsWithNonDeterministicKeyCoders = new HashSet<>();
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    logWarningIfPCollectionViewHasNonDeterministicKeyCoder(pipeline);

    MetricsEnvironment.setMetricsSupported(true);

    LOG.info("Executing pipeline using FlinkRunner.");

    FlinkPipelineExecutionEnvironment env = new FlinkPipelineExecutionEnvironment(options);

    LOG.info("Translating pipeline to Flink program.");
    env.translate(pipeline);

    JobExecutionResult result;
    try {
      LOG.info("Starting execution of Flink program.");
      result = env.executePipeline();
    } catch (Exception e) {
      LOG.error("Pipeline execution failed", e);
      throw new RuntimeException("Pipeline execution failed", e);
    }
    return createPipelineResult(result, options);
  }

  static PipelineResult createPipelineResult(JobExecutionResult result, PipelineOptions options) {
    if (result instanceof DetachedEnvironment.DetachedJobExecutionResult) {
      LOG.info("Pipeline submitted in Detached mode");
      // no metricsPusher because metrics are not supported in detached mode
      return new FlinkDetachedRunnerResult();
    } else {
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
              flinkRunnerResult.getMetricsContainerStepMap(), options, flinkRunnerResult);
      metricsPusher.start();
      return flinkRunnerResult;
    }
  }

  /** For testing. */
  public FlinkPipelineOptions getPipelineOptions() {
    return options;
  }

  @Override
  public String toString() {
    return "FlinkRunner#" + hashCode();
  }

  /** A set of {@link View}s with non-deterministic key coders. */
  Set<PTransform<?, ?>> ptransformViewsWithNonDeterministicKeyCoders;

  /** Records that the {@link PTransform} requires a deterministic key coder. */
  void recordViewUsesNonDeterministicKeyCoder(PTransform<?, ?> ptransform) {
    ptransformViewsWithNonDeterministicKeyCoders.add(ptransform);
  }

  /** Outputs a warning about PCollection views without deterministic key coders. */
  private void logWarningIfPCollectionViewHasNonDeterministicKeyCoder(Pipeline pipeline) {
    // We need to wait till this point to determine the names of the transforms since only
    // at this time do we know the hierarchy of the transforms otherwise we could
    // have just recorded the full names during apply time.
    if (!ptransformViewsWithNonDeterministicKeyCoders.isEmpty()) {
      final SortedSet<String> ptransformViewNamesWithNonDeterministicKeyCoders = new TreeSet<>();
      pipeline.traverseTopologically(
          new Pipeline.PipelineVisitor.Defaults() {

            @Override
            public void visitPrimitiveTransform(TransformHierarchy.Node node) {
              if (ptransformViewsWithNonDeterministicKeyCoders.contains(node.getTransform())) {
                ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
              }
            }

            @Override
            public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
              if (ptransformViewsWithNonDeterministicKeyCoders.contains(node.getTransform())) {
                ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
              }
              return CompositeBehavior.ENTER_TRANSFORM;
            }
          });

      LOG.warn(
          "Unable to use indexed implementation for View.AsMap and View.AsMultimap for {} "
              + "because the key coder is not deterministic. Falling back to singleton "
              + "implementation which may cause memory and/or performance problems. Future major "
              + "versions of the Flink runner will require deterministic key coders.",
          ptransformViewNamesWithNonDeterministicKeyCoders);
    }
  }
}
