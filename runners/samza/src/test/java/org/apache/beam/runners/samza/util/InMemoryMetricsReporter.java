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
package org.apache.beam.runners.samza.util;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An in-memory {@link MetricsReporter} for testing. */
public class InMemoryMetricsReporter implements MetricsReporter {
  private Map<String, ReadableMetricsRegistry> registries;

  public InMemoryMetricsReporter() {
    registries = new HashMap<>();
  }

  @Override
  public void start() {}

  @Override
  public void register(String source, ReadableMetricsRegistry registry) {
    registries.put(source, registry);
  }

  @Override
  public void stop() {}

  public @Nullable ReadableMetricsRegistry getMetricsRegistry(@NonNull String source) {
    return registries.get(source);
  }
}
