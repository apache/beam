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

import java.util.Map;
import org.apache.beam.sdk.PipelineResult;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.program.DetachedEnvironment;
import org.slf4j.Logger;

/**
 * Static utility methods for wrapping a Flink job result as a
 * {@link org.apache.beam.sdk.PipelineResult}.
 */
class FlinkRunnerResultUtil {

  static PipelineResult wrapFlinkRunnerResult(Logger log, JobExecutionResult jobResult) {
    if (jobResult instanceof DetachedEnvironment.DetachedJobExecutionResult) {
      log.info("Pipeline submitted in Detached mode");
      return new FlinkDetachedRunnerResult();
    } else {
      log.info("Execution finished in {} msecs", jobResult.getNetRuntime());
      Map<String, Object> accumulators = jobResult.getAllAccumulatorResults();
      if (accumulators != null && !accumulators.isEmpty()) {
        log.info("Final accumulator values:");
        for (Map.Entry<String, Object> entry : accumulators.entrySet()) {
          log.info("{} : {}", entry.getKey(), entry.getValue());
        }
      }

      return new FlinkRunnerResult(accumulators, jobResult.getNetRuntime());
    }
  }
}
