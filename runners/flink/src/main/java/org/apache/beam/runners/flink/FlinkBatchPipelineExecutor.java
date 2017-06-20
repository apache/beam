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

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.DetachedEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FlinkPipelineExecutor} that executes a {@link Pipeline} using a Flink
 * {@link ExecutionEnvironment}.
 */
class FlinkBatchPipelineExecutor implements FlinkPipelineExecutor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlinkBatchPipelineExecutor.class);

  @Override
  public PipelineResult executePipeline(
      FlinkRunner runner, Pipeline pipeline, FlinkPipelineOptions options) throws Exception{

    ExecutionEnvironment env = createBatchExecutionEnvironment(options);
    FlinkBatchPipelineTranslator translator = new FlinkBatchPipelineTranslator(env, options);
    translator.translate(pipeline);

    JobExecutionResult result = env.execute(options.getJobName());

    if (result instanceof DetachedEnvironment.DetachedJobExecutionResult) {
      LOG.info("Pipeline submitted in Detached mode");
      return new FlinkDetachedRunnerResult();
    } else {
      LOG.info("Execution finished in {} msecs", result.getNetRuntime());
      Map<String, Object> accumulators = result.getAllAccumulatorResults();
      if (accumulators != null && !accumulators.isEmpty()) {
        LOG.info("Final accumulator values:");
        for (Map.Entry<String, Object> entry : result.getAllAccumulatorResults().entrySet()) {
          LOG.info("{} : {}", entry.getKey(), entry.getValue());
        }
      }

      return new FlinkRunnerResult(accumulators, result.getNetRuntime());
    }
  }

  private ExecutionEnvironment createBatchExecutionEnvironment(FlinkPipelineOptions options) {

    String masterUrl = options.getFlinkMaster();
    ExecutionEnvironment flinkBatchEnv;

    // depending on the master, create the right environment.
    if (masterUrl.equals("[local]")) {
      flinkBatchEnv = ExecutionEnvironment.createLocalEnvironment();
    } else if (masterUrl.equals("[collection]")) {
      flinkBatchEnv = new CollectionEnvironment();
    } else if (masterUrl.equals("[auto]")) {
      flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
    } else if (masterUrl.matches(".*:\\d*")) {
      String[] parts = masterUrl.split(":");
      List<String> stagingFiles = options.getFilesToStage();
      flinkBatchEnv = ExecutionEnvironment.createRemoteEnvironment(parts[0],
          Integer.parseInt(parts[1]),
          stagingFiles.toArray(new String[stagingFiles.size()]));
    } else {
      LOG.warn("Unrecognized Flink Master URL {}. Defaulting to [auto].", masterUrl);
      flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
    }

    // set the correct parallelism.
    if (options.getParallelism() != -1 && !(flinkBatchEnv instanceof CollectionEnvironment)) {
      flinkBatchEnv.setParallelism(options.getParallelism());
    }

    // set parallelism in the options (required by some execution code)
    options.setParallelism(flinkBatchEnv.getParallelism());

    if (options.getObjectReuse()) {
      flinkBatchEnv.getConfig().enableObjectReuse();
    } else {
      flinkBatchEnv.getConfig().disableObjectReuse();
    }

    return flinkBatchEnv;
  }
}
