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
import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.hasUnboundedPCollections;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.PipelineTrimmer;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.runners.fnexecution.jobsubmission.PortablePipelineJarUtils;
import org.apache.beam.runners.fnexecution.jobsubmission.PortablePipelineResult;
import org.apache.beam.runners.fnexecution.jobsubmission.PortablePipelineRunner;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.program.DetachedEnvironment;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs a Pipeline on Flink via {@link FlinkRunner}. */
public class FlinkPipelineRunner implements PortablePipelineRunner {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkPipelineRunner.class);

  private final FlinkPipelineOptions pipelineOptions;
  private final String confDir;
  private final List<String> filesToStage;

  public FlinkPipelineRunner(
      FlinkPipelineOptions pipelineOptions, @Nullable String confDir, List<String> filesToStage) {
    this.pipelineOptions = pipelineOptions;
    this.confDir = confDir;
    this.filesToStage = filesToStage;
  }

  @Override
  public PortablePipelineResult run(final Pipeline pipeline, JobInfo jobInfo) throws Exception {
    MetricsEnvironment.setMetricsSupported(false);

    FlinkPortablePipelineTranslator<?> translator;
    if (!pipelineOptions.isStreaming() && !hasUnboundedPCollections(pipeline)) {
      // TODO: Do we need to inspect for unbounded sources before fusing?
      translator = FlinkBatchPortablePipelineTranslator.createTranslator();
    } else {
      translator = new FlinkStreamingPortablePipelineTranslator();
    }
    return runPipelineWithTranslator(pipeline, jobInfo, translator);
  }

  private <T extends FlinkPortablePipelineTranslator.TranslationContext>
      PortablePipelineResult runPipelineWithTranslator(
          final Pipeline pipeline, JobInfo jobInfo, FlinkPortablePipelineTranslator<T> translator)
          throws Exception {
    LOG.info("Translating pipeline to Flink program.");

    // Don't let the fuser fuse any subcomponents of native transforms.
    Pipeline trimmedPipeline = PipelineTrimmer.trim(pipeline, translator.knownUrns());

    // Fused pipeline proto.
    // TODO: Consider supporting partially-fused graphs.
    RunnerApi.Pipeline fusedPipeline =
        trimmedPipeline.getComponents().getTransformsMap().values().stream()
                .anyMatch(proto -> ExecutableStage.URN.equals(proto.getSpec().getUrn()))
            ? trimmedPipeline
            : GreedyPipelineFuser.fuse(trimmedPipeline).toPipeline();

    FlinkPortablePipelineTranslator.Executor executor =
        translator.translate(
            translator.createTranslationContext(jobInfo, pipelineOptions, confDir, filesToStage),
            fusedPipeline);
    final JobExecutionResult result = executor.execute(pipelineOptions.getJobName());

    return createPortablePipelineResult(result, pipelineOptions);
  }

  private PortablePipelineResult createPortablePipelineResult(
      JobExecutionResult result, PipelineOptions options) {
    if (result instanceof DetachedEnvironment.DetachedJobExecutionResult) {
      LOG.info("Pipeline submitted in Detached mode");
      // no metricsPusher because metrics are not supported in detached mode
      return new FlinkPortableRunnerResult.Detached();
    } else {
      LOG.info("Execution finished in {} msecs", result.getNetRuntime());
      Map<String, Object> accumulators = result.getAllAccumulatorResults();
      if (accumulators != null && !accumulators.isEmpty()) {
        LOG.info("Final accumulator values:");
        for (Map.Entry<String, Object> entry : result.getAllAccumulatorResults().entrySet()) {
          LOG.info("{} : {}", entry.getKey(), entry.getValue());
        }
      }
      FlinkPortableRunnerResult flinkRunnerResult =
          new FlinkPortableRunnerResult(accumulators, result.getNetRuntime());
      MetricsPusher metricsPusher =
          new MetricsPusher(
              flinkRunnerResult.getMetricsContainerStepMap(),
              options.as(MetricsOptions.class),
              flinkRunnerResult);
      metricsPusher.start();
      return flinkRunnerResult;
    }
  }

  /**
   * Main method to be called only as the entry point to an executable jar with structure as defined
   * in {@link PortablePipelineJarUtils}.
   */
  public static void main(String[] args) throws Exception {
    // Register standard file systems.
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());

    FlinkPipelineRunnerConfiguration configuration = parseArgs(args);
    Pipeline pipeline = PortablePipelineJarUtils.getPipelineFromClasspath();
    Struct options = PortablePipelineJarUtils.getPipelineOptionsFromClasspath();
    FlinkPipelineOptions flinkOptions =
        PipelineOptionsTranslation.fromProto(options).as(FlinkPipelineOptions.class);
    String invocationId =
        String.format("%s_%s", flinkOptions.getJobName(), UUID.randomUUID().toString());
    ProxyManifest proxyManifest = PortablePipelineJarUtils.getArtifactManifestFromClassPath();
    String retrievalToken =
        PortablePipelineJarUtils.stageArtifacts(
            proxyManifest, flinkOptions, invocationId, configuration.artifactStagingPath);

    FlinkPipelineRunner runner =
        new FlinkPipelineRunner(
            flinkOptions,
            configuration.flinkConfDir,
            detectClassPathResourcesToStage(FlinkPipelineRunner.class.getClassLoader()));
    JobInfo jobInfo =
        JobInfo.create(invocationId, flinkOptions.getJobName(), retrievalToken, options);
    try {
      runner.run(pipeline, jobInfo);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Job %s failed.", invocationId), e);
    }
    LOG.info("Job {} finished successfully.", invocationId);
  }

  private static class FlinkPipelineRunnerConfiguration {
    @Option(name = "--artifacts-dir", usage = "The location to store staged artifact files")
    private String artifactStagingPath =
        Paths.get(System.getProperty("java.io.tmpdir"), "beam-artifact-staging").toString();

    @Option(
        name = "--flink-conf-dir",
        usage =
            "Directory containing Flink YAML configuration files. "
                + "These properties will be set to all jobs submitted to Flink and take precedence "
                + "over configurations in FLINK_CONF_DIR.")
    private String flinkConfDir = null;
  }

  private static FlinkPipelineRunnerConfiguration parseArgs(String[] args) {
    FlinkPipelineRunnerConfiguration configuration = new FlinkPipelineRunnerConfiguration();
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
