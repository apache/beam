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
package org.apache.beam.runners.flink.translation.functions;

import java.io.IOException;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.DockerJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of a {@link FlinkExecutableStageContext} for batch jobs. */
class FlinkBatchExecutableStageContext implements FlinkExecutableStageContext, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkBatchExecutableStageContext.class);

  private final JobBundleFactory jobBundleFactory;

  private static FlinkBatchExecutableStageContext create(JobInfo jobInfo) throws Exception {
    JobBundleFactory jobBundleFactory = DockerJobBundleFactory.create(jobInfo);
    return new FlinkBatchExecutableStageContext(jobBundleFactory);
  }

  private FlinkBatchExecutableStageContext(JobBundleFactory jobBundleFactory) {
    this.jobBundleFactory = jobBundleFactory;
  }

  @Override
  public StageBundleFactory getStageBundleFactory(ExecutableStage executableStage) {
    return jobBundleFactory.forStage(executableStage);
  }

  public static StateRequestHandler getStateRequestHandler(
      ExecutableStage executableStage, RuntimeContext runtimeContext) {
    SideInputHandlerFactory sideInputHandlerFactory =
        FlinkBatchSideInputHandlerFactory.forStage(executableStage, runtimeContext);
    try {
      return StateRequestHandlers.forSideInputHandlerFactory(
          ProcessBundleDescriptors.getSideInputs(executableStage), sideInputHandlerFactory);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    jobBundleFactory.close();
  }

  enum BatchFactory implements Factory {
    REFERENCE_COUNTING;

    private static final ReferenceCountingFlinkExecutableStageContextFactory actualFactory =
        ReferenceCountingFlinkExecutableStageContextFactory.create(
            FlinkBatchExecutableStageContext::create);

    @Override
    public FlinkExecutableStageContext get(JobInfo jobInfo) {
      return actualFactory.get(jobInfo);
    }
  }
}
