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

import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.DockerJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;

/** Implementation of a {@link FlinkExecutableStageContext}. */
class FlinkDefaultExecutableStageContext implements FlinkExecutableStageContext, AutoCloseable {
  private final JobBundleFactory jobBundleFactory;

  private static FlinkDefaultExecutableStageContext create(JobInfo jobInfo) throws Exception {
    JobBundleFactory jobBundleFactory = DockerJobBundleFactory.FACTORY.get().create(jobInfo);
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

  enum ReferenceCountingFactory implements Factory {
    REFERENCE_COUNTING;

    private static final ReferenceCountingFlinkExecutableStageContextFactory actualFactory =
        ReferenceCountingFlinkExecutableStageContextFactory.create(
            FlinkDefaultExecutableStageContext::create);

    @Override
    public FlinkExecutableStageContext get(JobInfo jobInfo) {
      return actualFactory.get(jobInfo);
    }
  }
}
