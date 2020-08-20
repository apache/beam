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

import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;

/**
 * Compatibility layer for {@link PackagedProgram} and {@link OptimizerPlanEnvironment} breaking
 * changes.
 */
public abstract class FlinkRunnerTestCompat {
  public PackagedProgram getPackagedProgram() throws ProgramInvocationException {
    return PackagedProgram.newBuilder().setEntryPointClassName(getClass().getName()).build();
  }

  public OptimizerPlanEnvironment getOptimizerPlanEnvironment() {
    int parallelism = Runtime.getRuntime().availableProcessors();
    return new OptimizerPlanEnvironment(
        new Configuration(), getClass().getClassLoader(), parallelism);
  }

  public void getPipeline(OptimizerPlanEnvironment env, PackagedProgram packagedProgram)
      throws ProgramInvocationException {
    int parallelism = Runtime.getRuntime().availableProcessors();
    PackagedProgramUtils.getPipelineFromProgram(
        packagedProgram, env.getConfiguration(), parallelism, true);
  }
}
