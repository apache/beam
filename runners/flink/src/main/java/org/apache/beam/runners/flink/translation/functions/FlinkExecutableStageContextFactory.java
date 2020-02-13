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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.runners.fnexecution.control.DefaultExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.ReferenceCountingExecutableStageContextFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.flink.api.java.ExecutionEnvironment;

/** Singleton class that contains one {@link ExecutableStageContext.Factory} per job. */
public class FlinkExecutableStageContextFactory implements ExecutableStageContext.Factory {

  private static final FlinkExecutableStageContextFactory instance =
      new FlinkExecutableStageContextFactory();
  // This map should only ever have a single element, as each job will have its own
  // classloader and therefore its own instance of FlinkExecutableStageContextFactory. This
  // code supports multiple JobInfos in order to provide a sensible implementation of
  // Factory.get(JobInfo), which in theory could be called with different JobInfos.
  private static final ConcurrentMap<String, ExecutableStageContext.Factory> jobFactories =
      new ConcurrentHashMap<>();

  private FlinkExecutableStageContextFactory() {}

  public static FlinkExecutableStageContextFactory getInstance() {
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
                  // Clean up context immediately if its class is not loaded on Flink parent
                  // classloader.
                  (caller) ->
                      caller.getClass().getClassLoader()
                          != ExecutionEnvironment.class.getClassLoader());
            });

    return jobFactory.get(jobInfo);
  }
}
