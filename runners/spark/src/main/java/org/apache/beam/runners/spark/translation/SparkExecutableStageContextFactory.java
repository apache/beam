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
package org.apache.beam.runners.spark.translation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.runners.fnexecution.control.DefaultExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.ReferenceCountingExecutableStageContextFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;

/**
 * Singleton class that contains one {@link ExecutableStageContext.Factory} per job. Assumes it is
 * safe to release the backing environment asynchronously.
 */
public class SparkExecutableStageContextFactory implements ExecutableStageContext.Factory {

  private static final SparkExecutableStageContextFactory instance =
      new SparkExecutableStageContextFactory();
  // This map should only ever have a single element, as each job will have its own
  // classloader and therefore its own instance of SparkExecutableStageContextFactory. This
  // code supports multiple JobInfos in order to provide a sensible implementation of
  // Factory.get(JobInfo), which in theory could be called with different JobInfos.
  private static final ConcurrentMap<String, ExecutableStageContext.Factory> jobFactories =
      new ConcurrentHashMap<>();

  private SparkExecutableStageContextFactory() {}

  public static SparkExecutableStageContextFactory getInstance() {
    return instance;
  }

  @Override
  public ExecutableStageContext get(JobInfo jobInfo) {
    ExecutableStageContext.Factory jobFactory =
        jobFactories.computeIfAbsent(
            jobInfo.jobId(),
            k -> {
              return ReferenceCountingExecutableStageContextFactory.create(
                  DefaultExecutableStageContext::create,
                  // Always release environment asynchronously.
                  (caller) -> false);
            });
    return jobFactory.get(jobInfo);
  }
}
