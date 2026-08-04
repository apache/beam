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
package org.apache.beam.sdk.lineage;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/**
 * A test implementation of {@link LineageBase} for integration testing with DirectRunner.
 *
 * <p>This implementation records all lineage FQNs in thread-safe static storage for test
 * assertions.
 */
public class TestLineage implements LineageBase {

  // Thread-safe storage for recorded lineage, keyed by direction
  private static final ConcurrentHashMap<Lineage.LineageDirection, List<String>> RECORDED_LINEAGE =
      new ConcurrentHashMap<>();

  private final Lineage.LineageDirection direction;

  /** Constructor used by reflection via {@code --lineageType} pipeline option. */
  public TestLineage(PipelineOptions options, Lineage.LineageDirection direction) {
    this(direction);
  }

  public TestLineage(Lineage.LineageDirection direction) {
    this.direction = direction;
  }

  @Override
  public void add(Iterable<String> rollupSegments) {
    // Record the FQN for test assertions
    String fqn = String.join("", rollupSegments);
    RECORDED_LINEAGE.computeIfAbsent(direction, k -> new CopyOnWriteArrayList<>()).add(fqn);
  }

  public Lineage.LineageDirection getDirection() {
    return direction;
  }

  /** Returns all recorded source lineage FQNs. */
  public static List<String> getRecordedSources() {
    return ImmutableList.copyOf(
        RECORDED_LINEAGE.getOrDefault(Lineage.LineageDirection.SOURCE, ImmutableList.of()));
  }

  /** Returns all recorded sink lineage FQNs. */
  public static List<String> getRecordedSinks() {
    return ImmutableList.copyOf(
        RECORDED_LINEAGE.getOrDefault(Lineage.LineageDirection.SINK, ImmutableList.of()));
  }

  /** Clears all recorded lineage. Should be called in @Before to ensure test isolation. */
  public static void clearRecorded() {
    RECORDED_LINEAGE.clear();
  }
}
