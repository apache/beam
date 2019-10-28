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
package org.apache.beam.runners.fnexecution.control;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** Implementation of a {@link ExecutableStageContext}. */
public class DefaultExecutableStageContext implements ExecutableStageContext, AutoCloseable {
  private final JobBundleFactory jobBundleFactory;

  private static DefaultExecutableStageContext create(JobInfo jobInfo) {
    JobBundleFactory jobBundleFactory = DefaultJobBundleFactory.create(jobInfo);
    return new DefaultExecutableStageContext(jobBundleFactory);
  }

  private DefaultExecutableStageContext(JobBundleFactory jobBundleFactory) {
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

  /**
   * {@link ExecutableStageContext.Factory} that creates and round-robins between a number of child
   * {@link ExecutableStageContext.Factory} instances.
   */
  public static class MultiInstanceFactory implements ExecutableStageContext.Factory {

    private int index = 0;
    private final List<ReferenceCountingExecutableStageContextFactory> factories =
        new ArrayList<>();
    private final int maxFactories;
    private final SerializableFunction<Object, Boolean> isReleaseSynchronous;

    public MultiInstanceFactory(
        int maxFactories, SerializableFunction<Object, Boolean> isReleaseSynchronous) {
      this.isReleaseSynchronous = isReleaseSynchronous;
      Preconditions.checkArgument(maxFactories >= 0, "sdk_worker_parallelism must be >= 0");

      if (maxFactories == 0) {
        // if this is 0, use the auto behavior of num_cores - 1 so that we leave some resources
        // available for the java process
        this.maxFactories = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
      } else {
        this.maxFactories = maxFactories;
      }
    }

    private synchronized ExecutableStageContext.Factory getFactory() {
      ReferenceCountingExecutableStageContextFactory factory;
      // If we haven't yet created maxFactories factories, create a new one. Otherwise use an
      // existing one from factories.
      if (factories.size() < maxFactories) {
        factory =
            ReferenceCountingExecutableStageContextFactory.create(
                DefaultExecutableStageContext::create, isReleaseSynchronous);
        factories.add(factory);
      } else {
        factory = factories.get(index);
      }

      index = (index + 1) % maxFactories;

      return factory;
    }

    @Override
    public ExecutableStageContext get(JobInfo jobInfo) {
      return getFactory().get(jobInfo);
    }
  }
}
