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

import static org.apache.beam.runners.fnexecution.translation.PipelineTranslatorUtils.hasUnboundedPCollections;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.PipelineTrimmer;
import org.apache.beam.runners.fnexecution.jobsubmission.PortablePipelineRunner;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.flink.api.common.JobExecutionResult;
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
  public PipelineResult run(final Pipeline pipeline, JobInfo jobInfo) throws Exception {
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
      PipelineResult runPipelineWithTranslator(
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

    return FlinkRunner.createPipelineResult(result, pipelineOptions);
  }
}
