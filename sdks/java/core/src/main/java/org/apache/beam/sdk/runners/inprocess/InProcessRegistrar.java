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
package org.apache.beam.sdk.runners.inprocess;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

/**
 * Contains the {@link PipelineRunnerRegistrar} and {@link PipelineOptionsRegistrar} for the
 * {@link InProcessPipelineRunner}.
 */
public class InProcessRegistrar {
  private InProcessRegistrar() {}
  /**
   * Registers the {@link InProcessPipelineRunner}.
   */
  @AutoService(PipelineRunnerRegistrar.class)
  public static class InProcessRunner implements PipelineRunnerRegistrar {
    @Override
    public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
      return ImmutableList.<Class<? extends PipelineRunner<?>>>of(InProcessPipelineRunner.class);
    }
  }

  /**
   * Registers the {@link InProcessPipelineOptions}.
   */
  @AutoService(PipelineOptionsRegistrar.class)
  public static class InProcessOptions implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.<Class<? extends PipelineOptions>>of(InProcessPipelineOptions.class);
    }
  }
}
