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
package org.apache.beam.sdk.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.beam.sdk.util.UnboundedScheduledExecutorService;

/**
 * Options for configuring the {@link ScheduledExecutorService} used throughout the Java runtime.
 */
public interface ExecutorOptions extends PipelineOptions {

  /**
   * The {@link ScheduledExecutorService} instance to use to create threads, can be overridden to
   * specify a {@link ScheduledExecutorService} that is compatible with the user's environment. If
   * unset, the default is to create an {@link UnboundedScheduledExecutorService}.
   */
  @JsonIgnore
  @Description(
      "The ScheduledExecutorService instance to use to create threads, can be overridden to specify "
          + "a ScheduledExecutorService that is compatible with the user's environment. If unset, "
          + "the default is to create an UnboundedScheduledExecutorService.")
  @Default.InstanceFactory(ScheduledExecutorServiceFactory.class)
  @Hidden
  ScheduledExecutorService getScheduledExecutorService();

  void setScheduledExecutorService(ScheduledExecutorService value);

  /** Returns the default {@link ScheduledExecutorService} to use within the Apache Beam SDK. */
  class ScheduledExecutorServiceFactory implements DefaultValueFactory<ScheduledExecutorService> {
    @Override
    public ScheduledExecutorService create(PipelineOptions options) {
      /* The SDK requires an unbounded thread pool because a step may create X writers
       * each requiring their own thread to perform the writes otherwise a writer may
       * block causing deadlock for the step because the writers buffer is full.
       * Also, the MapTaskExecutor launches the steps in reverse order and completes
       * them in forward order thus requiring enough threads so that each step's writers
       * can be active.
       */
      return new UnboundedScheduledExecutorService();
    }
  }
}
