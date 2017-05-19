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
package org.apache.beam.sdk;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.util.InstanceBuilder;

/**
 * A {@link PipelineRunner} runs a {@link Pipeline}.
 *
 * @param <ResultT> the type of the result of {@link #run}, often a handle to a running job.
 */
public abstract class PipelineRunner<ResultT extends PipelineResult> {

  /**
   * Constructs a runner from the provided {@link PipelineOptions}.
   *
   * @return The newly created runner.
   */
  public static PipelineRunner<? extends PipelineResult> fromOptions(PipelineOptions options) {
    checkNotNull(options);
    PipelineOptionsValidator.validate(PipelineOptions.class, options);

    // (Re-)register standard FileSystems. Clobbers any prior credentials.
    FileSystems.setDefaultPipelineOptions(options);

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
   * Processes the given {@link Pipeline}, potentially asynchronously, returning a runner-specific
   * type of result.
   */
  public abstract ResultT run(Pipeline pipeline);
}
