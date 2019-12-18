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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.samza.config.TaskConfig.MAX_CONCURRENCY;

import java.util.HashMap;
import java.util.Map;

/** Validates that the {@link SamzaPipelineOptions} conforms to all the criteria. */
public class SamzaPipelineOptionsValidator {
  public static void validate(SamzaPipelineOptions opts) {
    checkArgument(opts.getMaxSourceParallelism() >= 1);
    validateBundlingRelatedOptions(opts);
  }

  /*
   * Perform some bundling related validation for pipeline option .
   */
  private static void validateBundlingRelatedOptions(SamzaPipelineOptions pipelineOptions) {
    if (pipelineOptions.getMaxBundleSize() > 1) {
      // TODO: remove this check and implement bundling for side input, timer, etc in DoFnOp.java
      checkState(
          isPortable(pipelineOptions),
          "Bundling is not supported in non portable mode. Please disable by setting maxBundleSize to 1.");

      String taskConcurrencyConfig = MAX_CONCURRENCY;
      Map<String, String> configs =
          pipelineOptions.getConfigOverride() == null
              ? new HashMap<>()
              : pipelineOptions.getConfigOverride();
      long taskConcurrency = Long.parseLong(configs.getOrDefault(taskConcurrencyConfig, "1"));
      checkState(
          taskConcurrency == 1,
          "Bundling is not supported if "
              + taskConcurrencyConfig
              + " is greater than 1. Please disable bundling by setting maxBundleSize to 1. Or disable task concurrency.");
    }
  }

  private static boolean isPortable(SamzaPipelineOptions options) {
    return options instanceof SamzaPortablePipelineOptions;
  }
}
