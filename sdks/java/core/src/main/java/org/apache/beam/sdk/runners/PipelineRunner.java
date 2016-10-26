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
package org.apache.beam.sdk.runners;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * A {@link PipelineRunner} can execute, translate, or otherwise process a
 * {@link Pipeline}.
 *
 * @param <ResultT> the type of the result of {@link #run}.
 */
public abstract class PipelineRunner<ResultT extends PipelineResult> {

  /**
   * Constructs a runner from the provided options.
   *
   * @return The newly created runner.
   */
  public static PipelineRunner<? extends PipelineResult> fromOptions(PipelineOptions options) {
    GcsOptions gcsOptions = PipelineOptionsValidator.validate(GcsOptions.class, options);
    checkNotNull(options);

    // (Re-)register standard IO factories. Clobbers any prior credentials.
    IOChannelUtils.registerStandardIOFactories(gcsOptions);

    @SuppressWarnings("unchecked")
    PipelineRunner<? extends PipelineResult> result =
        InstanceBuilder.ofType(PipelineRunner.class)
        .fromClass(options.getRunner())
        .fromFactoryMethod("fromOptions")
        .withArg(PipelineOptions.class, options)
        .build();
    return result;
  }

  /**
   * Processes the given Pipeline, returning the results.
   */
  public abstract ResultT run(Pipeline pipeline);

  /**
   * Applies a transform to the given input, returning the output.
   *
   * <p>The default implementation calls PTransform.apply(input), but can be overridden
   * to customize behavior for a particular runner.
   */
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    return transform.apply(input);
  }
}
