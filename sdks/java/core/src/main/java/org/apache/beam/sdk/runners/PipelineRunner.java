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
package com.google.cloud.dataflow.sdk.runners;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.common.base.Preconditions;

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
    Preconditions.checkNotNull(options);

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
