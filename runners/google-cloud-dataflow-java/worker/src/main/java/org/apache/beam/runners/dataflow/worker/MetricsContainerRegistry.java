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
package org.apache.beam.runners.dataflow.worker;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;

/**
 * Manages the instances of {@link MetricsContainer} that have been created for a specific context.
 *
 * <p>This class is thread-safe. In Batch mode it there is a single thread creating containers and a
 * separate thread calling {@link #getContainers()} to report tentative results. In Streaming all
 * execution threads use the same registry. Thus it supports concurrent retrieval and creation of
 * containers, as well as the ability to iterate over the containers.
 */
public abstract class MetricsContainerRegistry<T extends MetricsContainer> {

  private final ConcurrentMap<String, T> containers = new ConcurrentSkipListMap<>();

  /** Retrieve (creating if necessary) the {@link MetricsContainer} to use with the given step. */
  public T getContainer(String stepName) {
    return containers.computeIfAbsent(stepName, unused -> createContainer(stepName));
  }

  /**
   * Returns all of the containers that have been created with this registry. It returns a weakly
   * consistent view of the containers that have been created in this registry. That is:
   *
   * <ul>
   *   <li>iteration may proceed concurrently with other operations
   *   <li>iteration will never throw ConcurrentModificationException
   *   <li>iteration is guaranteed to traverse elements as they existed upon construction exactly
   *       once, and may (but are not guaranteed to) reflect any modifications subsequent to
   *       construction.
   * </ul>
   */
  public FluentIterable<T> getContainers() {
    return FluentIterable.from(containers.values());
  }

  /** Create the container for a given step. */
  protected abstract T createContainer(String stepName);
}
