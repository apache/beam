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
package org.apache.beam.runners.spark;

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.hasUnboundedPCollections;
import static org.apache.beam.runners.spark.SparkPipelineOptions.prepareFilesToStage;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides;
import org.apache.beam.runners.core.construction.graph.SplittableParDoExpander;
import org.apache.beam.runners.core.construction.graph.TrivialNativeTransformExpander;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.PortablePipelineJarUtils;
import org.apache.beam.runners.jobsubmission.PortablePipelineResult;
import org.apache.beam.runners.jobsubmission.PortablePipelineRunner;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.translation.SparkBatchPortablePipelineTranslator;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPortablePipelineTranslator;
import org.apache.beam.runners.spark.translation.SparkStreamingPortablePipelineTranslator;
import org.apache.beam.runners.spark.translation.SparkStreamingTranslationContext;
import org.apache.beam.runners.spark.translation.SparkTranslationContext;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerWrapper;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs a portable pipeline on Apache Spark. */
public class SparkPipelineRunner implements PortablePipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPipelineRunner.class);

  private final SparkPipelineOptions pipelineOptions;

  public SparkPipelineRunner(SparkPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public PortablePipelineResult run(RunnerApi.Pipeline pipeline, JobInfo jobInfo) {
    SparkPortablePipelineTranslator translator;
    boolean isStreaming = pipelineOptions.isStreaming() || hasUnboundedPCollections(pipeline);
    if (isStreaming) {
      translator = new SparkStreamingPortablePipelineTranslator();
    } else {
      translator = new SparkBatchPortablePipelineTranslator();
    }

    // Expand any splittable DoFns within the graph to enable sizing and splitting of bundles.
    Pipeline pipelineWithSdfExpanded =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            pipeline,
            SplittableParDoExpander.createSizedReplacement());

    // Don't let the fuser fuse any subcomponents of native transforms.
    Pipeline trimmedPipeline =
        TrivialNativeTransformExpander.forKnownUrns(
            pipelineWithSdfExpanded, translator.knownUrns());

    // Fused pipeline proto.
    // TODO: Consider supporting partially-fused graphs.
    RunnerApi.Pipeline fusedPipeline =
        trimmedPipeline.getComponents().getTransformsMap().values().stream()
                .anyMatch(proto -> ExecutableStage.URN.equals(proto.getSpec().getUrn()))
            ? trimmedPipeline
            : GreedyPipelineFuser.fuse(trimmedPipeline).toPipeline();

    // File staging.
    if (pipelineOptions.getFilesToStage() == null) {
      pipelineOptions.setFilesToStage(
          detectClassPathResourcesToStage(
              SparkPipelineRunner.class.getClassLoader(), pipelineOptions));
      LOG.info(
          "PipelineOptions.filesToStage was not specified. Defaulting to files from the classpath");
    }
    prepareFilesToStage(pipelineOptions);
    LOG.info(
        "Will stage {} files. (Enable logging at DEBUG level to see which files will be staged.)",
        pipelineOptions.getFilesToStage().size());
    LOG.debug("Staging files: {}", pipelineOptions.getFilesToStage());

    PortablePipelineResult result;
    final JavaSparkContext jsc = SparkContextFactory.getSparkContext(pipelineOptions);

    LOG.info(String.format("Running job %s on Spark master %s", jobInfo.jobId(), jsc.master()));

    // Initialize accumulators.
    AggregatorsAccumulator.init(pipelineOptions, jsc);
    MetricsEnvironment.setMetricsSupported(true);
    MetricsAccumulator.init(pipelineOptions, jsc);

    final SparkTranslationContext context =
        translator.createTranslationContext(jsc, pipelineOptions, jobInfo);
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    LOG.info(String.format("Running job %s on Spark master %s", jobInfo.jobId(), jsc.master()));

    if (isStreaming) {
      final JavaStreamingContext jssc =
          ((SparkStreamingTranslationContext) context).getStreamingContext();

      jssc.addStreamingListener(
          new JavaStreamingListenerWrapper(
              new AggregatorsAccumulator.AccumulatorCheckpointingSparkListener()));
      jssc.addStreamingListener(
          new JavaStreamingListenerWrapper(
              new MetricsAccumulator.AccumulatorCheckpointingSparkListener()));

      // Register user-defined listeners.
      for (JavaStreamingListener listener :
          pipelineOptions.as(SparkContextOptions.class).getListeners()) {
        LOG.info("Registered listener {}." + listener.getClass().getSimpleName());
        jssc.addStreamingListener(new JavaStreamingListenerWrapper(listener));
      }

      // Register Watermarks listener to broadcast the advanced WMs.
      jssc.addStreamingListener(
          new JavaStreamingListenerWrapper(
              new GlobalWatermarkHolder.WatermarkAdvancingStreamingListener()));

      jssc.checkpoint(pipelineOptions.getCheckpointDir());

      // Obtain timeout from options.
      Long timeout =
          pipelineOptions.as(SparkPortableStreamingPipelineOptions.class).getStreamingTimeoutMs();

      final Future<?> submissionFuture =
          executorService.submit(
              () -> {
                translator.translate(fusedPipeline, context);
                LOG.info(
                    String.format(
                        "Job %s: Pipeline translated successfully. Computing outputs",
                        jobInfo.jobId()));
                context.computeOutputs();

                jssc.start();
                try {
                  jssc.awaitTerminationOrTimeout(timeout);
                } catch (InterruptedException e) {
                  LOG.warn("Streaming context interrupted, shutting down.", e);
                }
                jssc.stop();
                LOG.info(String.format("Job %s finished.", jobInfo.jobId()));
              });
      result = new SparkPipelineResult.PortableStreamingMode(submissionFuture, jssc);
    } else {
      final Future<?> submissionFuture =
          executorService.submit(
              () -> {
                translator.translate(fusedPipeline, context);
                LOG.info(
                    String.format(
                        "Job %s: Pipeline translated successfully. Computing outputs",
                        jobInfo.jobId()));
                context.computeOutputs();
                LOG.info(String.format("Job %s finished.", jobInfo.jobId()));
              });
      result = new SparkPipelineResult.PortableBatchMode(submissionFuture, jsc);
    }
    executorService.shutdown();
    result.waitUntilFinish();

    MetricsPusher metricsPusher =
        new MetricsPusher(
            MetricsAccumulator.getInstance().value(),
            pipelineOptions.as(MetricsOptions.class),
            result);
    metricsPusher.start();

    return result;
  }

  /**
   * Main method to be called only as the entry point to an executable jar with structure as defined
   * in {@link PortablePipelineJarUtils}.
   */
  public static void main(String[] args) throws Exception {
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());

    SparkPipelineRunnerConfiguration configuration = parseArgs(args);
    String baseJobName =
        configuration.baseJobName == null
            ? PortablePipelineJarUtils.getDefaultJobName()
            : configuration.baseJobName;
    Preconditions.checkArgument(
        baseJobName != null,
        "No default job name found. Job name must be set using --base-job-name.");
    Pipeline pipeline = PortablePipelineJarUtils.getPipelineFromClasspath(baseJobName);
    Struct originalOptions = PortablePipelineJarUtils.getPipelineOptionsFromClasspath(baseJobName);

    // The retrieval token is only required by the legacy artifact service, which the Spark runner
    // no longer uses.
    String retrievalToken =
        ArtifactApi.CommitManifestResponse.Constants.NO_ARTIFACTS_STAGED_TOKEN
            .getValueDescriptor()
            .getOptions()
            .getExtension(RunnerApi.beamConstant);

    SparkPipelineOptions sparkOptions =
        PipelineOptionsTranslation.fromProto(originalOptions).as(SparkPipelineOptions.class);
    String invocationId =
        String.format("%s_%s", sparkOptions.getJobName(), UUID.randomUUID().toString());
    if (sparkOptions.getAppName() == null) {
      LOG.debug("App name was null. Using invocationId {}", invocationId);
      sparkOptions.setAppName(invocationId);
    }

    SparkPipelineRunner runner = new SparkPipelineRunner(sparkOptions);
    JobInfo jobInfo =
        JobInfo.create(
            invocationId,
            sparkOptions.getJobName(),
            retrievalToken,
            PipelineOptionsTranslation.toProto(sparkOptions));
    try {
      runner.run(pipeline, jobInfo);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Job %s failed.", invocationId), e);
    }
    LOG.info("Job {} finished successfully.", invocationId);
  }

  private static class SparkPipelineRunnerConfiguration {
    @Option(
        name = "--base-job-name",
        usage =
            "The job to run. This must correspond to a subdirectory of the jar's BEAM-PIPELINE "
                + "directory. *Only needs to be specified if the jar contains multiple pipelines.*")
    private String baseJobName = null;
  }

  private static SparkPipelineRunnerConfiguration parseArgs(String[] args) {
    SparkPipelineRunnerConfiguration configuration = new SparkPipelineRunnerConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      LOG.error("Unable to parse command line arguments.", e);
      parser.printUsage(System.err);
      throw new IllegalArgumentException("Unable to parse command line arguments.", e);
    }
    return configuration;
  }
}
