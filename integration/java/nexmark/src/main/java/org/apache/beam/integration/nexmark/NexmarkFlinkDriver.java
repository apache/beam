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
package org.apache.beam.integration.nexmark;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Run NexMark queries using the Flink runner.
 */
public class NexmarkFlinkDriver extends NexmarkDriver<NexmarkFlinkDriver.NexmarkFlinkOptions> {
  /**
   * Command line flags.
   */
  public interface NexmarkFlinkOptions extends Options, FlinkPipelineOptions {
  }

  /**
   * Entry point.
   */
  public static void main(String[] args) {
    // Gather command line args, baseline, configurations, etc.
    NexmarkFlinkOptions options = PipelineOptionsFactory.fromArgs(args)
                                                        .withValidation()
                                                        .as(NexmarkFlinkOptions.class);
    options.setRunner(FlinkRunner.class);
    NexmarkFlinkRunner runner = new NexmarkFlinkRunner(options);
    new NexmarkFlinkDriver().runAll(options, runner);
  }
}


