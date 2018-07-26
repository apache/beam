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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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

// TODO: Rename this to FlinkBatchExecutableStageContext for consistency.
/** Implementation of a {@link FlinkExecutableStageContext} for batch jobs. */
class BatchFlinkExecutableStageContext implements FlinkExecutableStageContext {
  private static final Logger LOG = LoggerFactory.getLogger(BatchFlinkExecutableStageContext.class);

  private final JobBundleFactory jobBundleFactory;

  private static BatchFlinkExecutableStageContext create(JobInfo jobInfo) throws Exception {
    JobBundleFactory jobBundleFactory = DockerJobBundleFactory.create(jobInfo);
    return new BatchFlinkExecutableStageContext(jobBundleFactory);
  }

  private BatchFlinkExecutableStageContext(JobBundleFactory jobBundleFactory) {
    this.jobBundleFactory = jobBundleFactory;
  }

  @Override
  public StageBundleFactory getStageBundleFactory(ExecutableStage executableStage) {
    return jobBundleFactory.forStage(executableStage);
  }

  @Override
  public StateRequestHandler getStateRequestHandler(
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
  protected void finalize() throws Exception {
    jobBundleFactory.close();
  }

  enum BatchFactory implements Factory {
    INSTANCE;

    @SuppressWarnings("Immutable") // observably immutable
    private final LoadingCache<JobInfo, BatchFlinkExecutableStageContext> cachedContexts;

    BatchFactory() {
      cachedContexts =
          CacheBuilder.newBuilder()
              .weakValues()
              .build(
                  new CacheLoader<JobInfo, BatchFlinkExecutableStageContext>() {
                    @Override
                    public BatchFlinkExecutableStageContext load(JobInfo jobInfo) throws Exception {
                      return create(jobInfo);
                    }
                  });
    }

    @Override
    public FlinkExecutableStageContext get(JobInfo jobInfo) {
      return cachedContexts.getUnchecked(jobInfo);
    }
  }
}
