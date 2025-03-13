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
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.util.construction.graph.ProtoOverrides;
import org.apache.beam.sdk.util.construction.graph.SplittableParDoExpander;
import org.apache.beam.sdk.util.construction.graph.TrivialNativeTransformExpander;
import org.apache.flink.api.common.JobExecutionResult;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Interface for portable Flink translators. This allows for a uniform invocation pattern for
 * pipeline translation between streaming and portable runners.
 *
 * <p>Pipeline translators will generally provide a mechanism to produce the translation contexts
 * that they use for pipeline translation. Post translation, the translation context should contain
 * a pipeline plan that has not yet been executed.
 */
public interface FlinkPortablePipelineTranslator<
    T extends FlinkPortablePipelineTranslator.TranslationContext> {

  /** The context used for pipeline translation. */
  interface TranslationContext {
    JobInfo getJobInfo();

    FlinkPipelineOptions getPipelineOptions();
  }

  /** A handle used to execute a translated pipeline. */
  interface Executor {
    JobExecutionResult execute(String jobName) throws Exception;
  }

  T createTranslationContext(
      JobInfo jobInfo,
      FlinkPipelineOptions pipelineOptions,
      @Nullable String confDir,
      List<String> filesToStage);

  Set<String> knownUrns();

  default RunnerApi.Pipeline prepareForTranslation(RunnerApi.Pipeline pipeline) {
    // Expand any splittable ParDos within the graph to enable sizing and splitting of bundles.
    RunnerApi.Pipeline pipelineWithSdfExpanded =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            pipeline,
            SplittableParDoExpander.createSizedReplacement());

    // Don't let the fuser fuse any subcomponents of native transforms.
    RunnerApi.Pipeline trimmedPipeline =
        TrivialNativeTransformExpander.forKnownUrns(pipelineWithSdfExpanded, knownUrns());

    // Fused pipeline proto.
    // TODO: Consider supporting partially-fused graphs.
    RunnerApi.Pipeline fusedPipeline =
        trimmedPipeline.getComponents().getTransformsMap().values().stream()
                .anyMatch(proto -> ExecutableStage.URN.equals(proto.getSpec().getUrn()))
            ? trimmedPipeline
            : GreedyPipelineFuser.fuse(trimmedPipeline).toPipeline();

    return fusedPipeline;
  }

  /** Translates the given pipeline. */
  Executor translate(T context, RunnerApi.Pipeline pipeline);
}
