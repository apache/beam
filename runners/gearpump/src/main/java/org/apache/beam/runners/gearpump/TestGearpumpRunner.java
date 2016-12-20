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

package org.apache.beam.runners.gearpump;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import org.apache.gearpump.cluster.embedded.EmbeddedCluster;

/**
 * Gearpump {@link PipelineRunner} for tests, which uses {@link EmbeddedCluster}.
 */
public class TestGearpumpRunner extends PipelineRunner<GearpumpPipelineResult> {

  private final GearpumpRunner delegate;
  private final EmbeddedCluster cluster;

  private TestGearpumpRunner(GearpumpPipelineOptions options) {
    cluster = EmbeddedCluster.apply();
    cluster.start();
    options.setEmbeddedCluster(cluster);
    delegate = GearpumpRunner.fromOptions(options);
  }

  public static TestGearpumpRunner fromOptions(PipelineOptions options) {
    GearpumpPipelineOptions pipelineOptions =
        PipelineOptionsValidator.validate(GearpumpPipelineOptions.class, options);
    return new TestGearpumpRunner(pipelineOptions);
  }

  @Override
  public GearpumpPipelineResult run(Pipeline pipeline) {
    GearpumpPipelineResult result = delegate.run(pipeline);
    PipelineResult.State state = result.waitUntilFinish();
    cluster.stop();
    assert(state == PipelineResult.State.DONE);

    return result;
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput>
  OutputT apply(PTransform<InputT, OutputT> transform, InputT input) {
    return delegate.apply(transform, input);
  }
}
