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
package org.apache.beam.sdk.metrics;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;

/**
 * Implementation of {@link Distribution} that delegates to the instance for the current context.
 */
@Internal
public class DelegatingDistribution implements Metric, Distribution, Serializable {
  private final MetricName name;
  private final boolean processWideContainer;

  public DelegatingDistribution(MetricName name) {
    this(name, false);
  }

  public DelegatingDistribution(MetricName name, boolean processWideContainer) {
    this.name = name;
    this.processWideContainer = processWideContainer;
  }

  @Override
  public void update(long value) {
    MetricsContainer container =
        this.processWideContainer
            ? MetricsEnvironment.getProcessWideContainer()
            : MetricsEnvironment.getCurrentContainer();
    if (container != null) {
      container.getDistribution(name).update(value);
    }
  }

  @Override
  public void update(long sum, long count, long min, long max) {
    MetricsContainer container =
        this.processWideContainer
            ? MetricsEnvironment.getProcessWideContainer()
            : MetricsEnvironment.getCurrentContainer();
    if (container != null) {
      container.getDistribution(name).update(sum, count, min, max);
    }
  }

  @Override
  public MetricName getName() {
    return name;
  }
}
