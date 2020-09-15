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
package org.apache.beam.runners.samza;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides;
import org.apache.beam.runners.core.construction.graph.SplittableParDoExpander;
import org.apache.beam.runners.core.construction.renderer.PipelineDotRenderer;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.jobsubmission.PortablePipelineResult;
import org.apache.beam.runners.jobsubmission.PortablePipelineRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs a Samza job via {@link SamzaRunner}. */
public class SamzaPipelineRunner implements PortablePipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaPipelineRunner.class);

  private final SamzaPipelineOptions options;

  @Override
  public PortablePipelineResult run(final Pipeline pipeline, JobInfo jobInfo) {
    // Expand any splittable DoFns within the graph to enable sizing and splitting of bundles.
    Pipeline pipelineWithSdfExpanded =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            pipeline,
            SplittableParDoExpander.createSizedReplacement());

    // Fused pipeline proto.
    final RunnerApi.Pipeline fusedPipeline =
        GreedyPipelineFuser.fuse(pipelineWithSdfExpanded).toPipeline();
    LOG.info("Portable pipeline to run:");
    LOG.info(PipelineDotRenderer.toDotString(fusedPipeline));
    // the pipeline option coming from sdk will set the sdk specific runner which will break
    // serialization
    // so we need to reset the runner here to a valid Java runner
    options.setRunner(SamzaRunner.class);
    try {
      final SamzaRunner runner = SamzaRunner.fromOptions(options);
      return runner.runPortablePipeline(fusedPipeline);
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke samza job", e);
    }
  }

  public SamzaPipelineRunner(SamzaPipelineOptions options) {
    this.options = options;
  }
}
