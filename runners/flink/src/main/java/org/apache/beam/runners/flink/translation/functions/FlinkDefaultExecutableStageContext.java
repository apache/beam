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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** Implementation of a {@link FlinkExecutableStageContext}. */
class FlinkDefaultExecutableStageContext implements FlinkExecutableStageContext, AutoCloseable {
  private final JobBundleFactory jobBundleFactory;

  private static FlinkDefaultExecutableStageContext create(JobInfo jobInfo) {
    JobBundleFactory jobBundleFactory = DefaultJobBundleFactory.create(jobInfo);
    return new FlinkDefaultExecutableStageContext(jobBundleFactory);
  }

  private FlinkDefaultExecutableStageContext(JobBundleFactory jobBundleFactory) {
    this.jobBundleFactory = jobBundleFactory;
  }

  @Override
  public StageBundleFactory getStageBundleFactory(ExecutableStage executableStage) {
    return jobBundleFactory.forStage(executableStage);
  }

  @Override
  public void close() throws Exception {
    jobBundleFactory.close();
  }

  private static class JobFactoryState {
    private int index = 0;
    private final List<ReferenceCountingFlinkExecutableStageContextFactory> factories =
        new ArrayList<>();
    private final int maxFactories;

    private JobFactoryState(int maxFactories) {
      Preconditions.checkArgument(maxFactories >= 0, "sdk_worker_parallelism must be >= 0");

      if (maxFactories == 0) {
        // if this is 0, use the auto behavior of num_cores - 1 so that we leave some resources
        // available for the java process
        this.maxFactories = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
      } else {
        this.maxFactories = maxFactories;
      }
    }

    private synchronized FlinkExecutableStageContext.Factory getFactory() {
      ReferenceCountingFlinkExecutableStageContextFactory factory;
      // If we haven't yet created maxFactories factories, create a new one. Otherwise use an
      // existing one from factories.
      if (factories.size() < maxFactories) {
        factory =
            ReferenceCountingFlinkExecutableStageContextFactory.create(
                FlinkDefaultExecutableStageContext::create);
        factories.add(factory);
      } else {
        factory = factories.get(index);
      }

      index = (index + 1) % maxFactories;

      return factory;
    }
  }

  enum MultiInstanceFactory implements Factory {
    MULTI_INSTANCE;

    // This map should only ever have a single element, as each job will have its own
    // classloader and therefore its own instance of MultiInstanceFactory.INSTANCE. This
    // code supports multiple JobInfos in order to provide a sensible implementation of
    // Factory.get(JobInfo), which in theory could be called with different JobInfos.
    private static final ConcurrentMap<String, JobFactoryState> jobFactories =
        new ConcurrentHashMap<>();

    @Override
    public FlinkExecutableStageContext get(JobInfo jobInfo) {
      JobFactoryState state =
          jobFactories.computeIfAbsent(
              jobInfo.jobId(),
              k -> {
                PortablePipelineOptions portableOptions =
                    PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions())
                        .as(PortablePipelineOptions.class);

                return new JobFactoryState(
                    MoreObjects.firstNonNull(portableOptions.getSdkWorkerParallelism(), 1L)
                        .intValue());
              });

      return state.getFactory().get(jobInfo);
    }
  }
}
