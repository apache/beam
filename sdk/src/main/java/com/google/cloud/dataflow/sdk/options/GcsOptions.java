/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.options;

import com.google.cloud.dataflow.sdk.util.AppEngineEnvironment;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Options used to configure Google Cloud Storage.
 */
public interface GcsOptions extends
    ApplicationNameOptions, GcpOptions, PipelineOptions {
  /** Alternative GcsUtil instance. */
  @JsonIgnore
  @Default.InstanceFactory(GcsUtil.GcsUtilFactory.class)
  GcsUtil getGcsUtil();
  void setGcsUtil(GcsUtil value);

  ////////////////////////////////////////////////////////////////////////////
  // Allows the user to provide an alternative ExecutorService if their
  // environment does not support the default implementation.
  @JsonIgnore
  @Default.InstanceFactory(ExecutorServiceFactory.class)
  ExecutorService getExecutorService();
  void setExecutorService(ExecutorService value);

  /**
   * Returns the default {@link ExecutorService} to use within the Dataflow SDK. The
   * {@link ExecutorService} is compatible with AppEngine.
   */
  public static class ExecutorServiceFactory implements DefaultValueFactory<ExecutorService> {
    @Override
    public ExecutorService create(PipelineOptions options) {
      ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder();
      threadFactoryBuilder.setThreadFactory(MoreExecutors.platformThreadFactory());
      if (!AppEngineEnvironment.IS_APP_ENGINE) {
        // AppEngine doesn't allow modification of threads to be daemon threads.
        threadFactoryBuilder.setDaemon(true);
      }
      /* The SDK requires an unbounded thread pool because a step may create X writers
       * each requiring their own thread to perform the writes otherwise a writer may
       * block causing deadlock for the step because the writers buffer is full.
       * Also, the MapTaskExecutor launches the steps in reverse order and completes
       * them in forward order thus requiring enough threads so that each step's writers
       * can be active.
       */
      return new ThreadPoolExecutor(
          0, Integer.MAX_VALUE, // Allow an unlimited number of re-usable threads.
          Long.MAX_VALUE, TimeUnit.NANOSECONDS, // Keep non-core threads alive forever.
          new SynchronousQueue<Runnable>(),
          threadFactoryBuilder.build());
    }
  }
}
