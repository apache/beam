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

public interface ExecutorOptions extends PipelineOptions {

  /**
   * The {@link ScheduledExecutorService} instance to use to create threads, can be overridden to
   * specify a {@link ScheduledExecutorService} that is compatible with the user's environment. If
   * unset, the default is to create an {@link ScheduledExecutorService} with a core number of
   * threads equal to {@code Math.max(4,Runtime.getRuntime().availableProcessors())}.
   */
  @JsonIgnore
  @Description(
      "The ScheduledExecutorService instance to use to create threads, can be overridden to specify "
          + "a ScheduledExecutorService that is compatible with the user's environment. If unset, "
          + "the default is to create a ScheduledExecutorService with a core number of threads "
          + "equal to Math.max(4, Runtime.getRuntime().availableProcessors()).")
  @Default.InstanceFactory(ScheduledExecutorServiceFactory.class)
  @Hidden
  ScheduledExecutorService getScheduledExecutorService();

  void setScheduledExecutorService(ScheduledExecutorService value);

  /** Returns the default {@link ScheduledExecutorService} to use within the Apache Beam SDK. */
  class ScheduledExecutorServiceFactory implements DefaultValueFactory<ScheduledExecutorService> {
    @Override
    public ScheduledExecutorService create(PipelineOptions options) {
      return new UnboundedScheduledExecutorService();
    }
  }
}
