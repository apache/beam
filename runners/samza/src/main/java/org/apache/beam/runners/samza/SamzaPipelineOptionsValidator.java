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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.samza.config.JobConfig.JOB_CONTAINER_THREAD_POOL_SIZE;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;

/** Validates that the {@link SamzaPipelineOptions} conforms to all the criteria. */
public class SamzaPipelineOptionsValidator {
  public static void validate(SamzaPipelineOptions opts) {
    checkArgument(opts.getMaxSourceParallelism() >= 1);
    validateBundlingRelatedOptions(opts);
  }

  /*
   * Perform some bundling related validation for pipeline option.
   * Visible for testing.
   */
  static void validateBundlingRelatedOptions(SamzaPipelineOptions pipelineOptions) {
    if (pipelineOptions.getMaxBundleSize() > 1) {
      final Map<String, String> configs =
          pipelineOptions.getConfigOverride() == null
              ? new HashMap<>()
              : pipelineOptions.getConfigOverride();
      final JobConfig jobConfig = new JobConfig(new MapConfig(configs));

      // Validate that the threadPoolSize is not override in the code
      checkArgument(
          jobConfig.getThreadPoolSize() <= 1,
          JOB_CONTAINER_THREAD_POOL_SIZE
              + " config should be replaced with SamzaPipelineOptions.numThreadsForProcessElement");
    }
  }
}
