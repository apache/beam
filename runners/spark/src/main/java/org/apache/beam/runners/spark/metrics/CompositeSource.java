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

package org.apache.beam.runners.spark.metrics;

import com.codahale.metrics.MetricRegistry;
import org.apache.spark.metrics.source.Source;


/**
 * Composite source made up of several {@link MetricRegistry} instances.
 */
public class CompositeSource implements Source {
  private final String name;
  private final MetricRegistry metricRegistry;

  public CompositeSource(final String name, MetricRegistry... metricRegistries) {
    this.name = name;
    this.metricRegistry = new MetricRegistry();
    for (MetricRegistry metricRegistry : metricRegistries) {
      this.metricRegistry.registerAll(metricRegistry);
    }
  }

  @Override
  public String sourceName() {
    return name;
  }

  @Override
  public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
