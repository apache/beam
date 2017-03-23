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
package org.apache.beam.integration.nexmark.drivers;

import org.apache.beam.integration.nexmark.NexmarkDriver;
import org.apache.beam.integration.nexmark.NexmarkOptions;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Run NexMark queries using the Apex runner.
 */
public class NexmarkApexDriver extends NexmarkDriver<NexmarkApexDriver.NexmarkApexOptions> {
  /**
   * Command line flags.
   */
  public interface NexmarkApexOptions extends NexmarkOptions, ApexPipelineOptions {
  }

  /**
   * Entry point.
   */
  public static void main(String[] args) {
    // Gather command line args, baseline, configurations, etc.
    NexmarkApexOptions options = PipelineOptionsFactory.fromArgs(args)
                                                        .withValidation()
                                                        .as(NexmarkApexOptions.class);
    options.setRunner(ApexRunner.class);
    NexmarkApexRunner runner = new NexmarkApexRunner(options);
    new NexmarkApexDriver().runAll(options, runner);
  }
}


