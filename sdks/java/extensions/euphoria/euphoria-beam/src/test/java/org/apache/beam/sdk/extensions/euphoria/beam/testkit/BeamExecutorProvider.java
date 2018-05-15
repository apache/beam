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
package org.apache.beam.sdk.extensions.euphoria.beam.testkit;

import org.apache.beam.sdk.extensions.euphoria.beam.BeamExecutor;
import org.apache.beam.sdk.extensions.euphoria.core.executor.Executor;
import org.apache.beam.sdk.extensions.euphoria.operator.test.junit.ExecutorEnvironment;
import org.apache.beam.sdk.extensions.euphoria.operator.test.junit.ExecutorProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.time.Duration;

/**
 * Local {@link ExecutorProvider}.
 */
public interface BeamExecutorProvider extends ExecutorProvider {

  default ExecutorEnvironment newExecutorEnvironment() throws Exception {
    final String[] args = {"--runner=DirectRunner"};
    final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    final Executor executor = new BeamExecutor(options).withAllowedLateness(Duration.ofHours(1));

    return new ExecutorEnvironment() {
      @Override
      public Executor getExecutor() {
        return executor;
      }

      @Override
      public void shutdown() throws Exception {
        executor.shutdown();
      }
    };
  }
}
