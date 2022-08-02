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
package org.apache.beam.runners.samza;

import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.samza.context.ApplicationContainerContext;
import org.apache.samza.context.ApplicationContainerContextFactory;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.metrics.MetricsRegistryMap;

/** Runtime context for the Samza runner. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SamzaExecutionContext implements ApplicationContainerContext {

  private final SamzaPipelineOptions options;
  private SamzaMetricsContainer metricsContainer;

  public SamzaExecutionContext(SamzaPipelineOptions options) {
    this.options = options;
  }

  public SamzaPipelineOptions getPipelineOptions() {
    return options;
  }

  public SamzaMetricsContainer getMetricsContainer() {
    return this.metricsContainer;
  }

  void setMetricsContainer(SamzaMetricsContainer metricsContainer) {
    this.metricsContainer = metricsContainer;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  /** The factory to return this {@link SamzaExecutionContext}. */
  public class Factory implements ApplicationContainerContextFactory<SamzaExecutionContext> {

    @Override
    public SamzaExecutionContext create(
        ExternalContext externalContext, JobContext jobContext, ContainerContext containerContext) {

      final MetricsRegistryMap metricsRegistry =
          (MetricsRegistryMap) containerContext.getContainerMetricsRegistry();
      SamzaExecutionContext.this.setMetricsContainer(new SamzaMetricsContainer(metricsRegistry));
      return SamzaExecutionContext.this;
    }
  }
}
