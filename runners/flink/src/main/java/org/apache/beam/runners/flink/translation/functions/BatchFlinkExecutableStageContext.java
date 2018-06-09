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
import com.google.common.cache.RemovalNotification;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.flink.ArtifactSourcePool;
import org.apache.beam.runners.fnexecution.control.DockerJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of a {@link FlinkExecutableStageContext} for batch jobs. */
class BatchFlinkExecutableStageContext implements FlinkExecutableStageContext {
  private static final Logger LOG = LoggerFactory.getLogger(BatchFlinkExecutableStageContext.class);

  private final JobBundleFactory jobBundleFactory;
  private final ArtifactSourcePool artifactSourcePool;

  private static BatchFlinkExecutableStageContext create(JobInfo jobInfo) throws Exception {
    ArtifactSourcePool artifactSourcePool = ArtifactSourcePool.create();
    JobBundleFactory jobBundleFactory = DockerJobBundleFactory.create(jobInfo, artifactSourcePool);
    return new BatchFlinkExecutableStageContext(jobBundleFactory, artifactSourcePool);
  }

  private BatchFlinkExecutableStageContext(
      JobBundleFactory jobBundleFactory, ArtifactSourcePool artifactSourcePool) {
    this.jobBundleFactory = jobBundleFactory;
    this.artifactSourcePool = artifactSourcePool;
  }

  @Override
  public <InputT> StageBundleFactory getStageBundleFactory(ExecutableStage executableStage) {
    return jobBundleFactory.<InputT>forStage(executableStage);
  }

  @Override
  public StateRequestHandler getStateRequestHandler(
      ExecutableStage executableStage, RuntimeContext runtimeContext) {
    return FlinkBatchStateRequestHandler.forStage(executableStage, runtimeContext);
  }

  @Override
  public ArtifactSourcePool getArtifactSourcePool() {
    return artifactSourcePool;
  }

  private void cleanUp() throws Exception {
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
              .removalListener(
                  (RemovalNotification<JobInfo, BatchFlinkExecutableStageContext> removal) -> {
                    try {
                      removal.getValue().cleanUp();
                    } catch (Exception e) {
                      LOG.warn(
                          "Error cleaning up bundle factory for job " + removal.getKey().jobId(),
                          e);
                    }
                  })
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
