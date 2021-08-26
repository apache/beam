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
package org.apache.beam.runners.dataflow.worker.counters;

import com.google.auto.value.AutoValue;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The {@link NameContext} represents the various names associated with a specific instruction in
 * the workflow graph.
 */
@AutoValue
public abstract class NameContext {
  /**
   * Create a {@link NameContext} with a {@code stageName}, an {@code originalName}, a {@code
   * systemName} and a {@code userName}.
   */
  public static NameContext create(
      String stageName, String originalName, String systemName, String userName) {
    return new AutoValue_NameContext(stageName, originalName, systemName, userName);
  }

  /**
   * Create a {@link NameContext} with only a {@code stageName} for representing time spent outside
   * specific steps..
   */
  public static NameContext forStage(String stageName) {
    return new AutoValue_NameContext(stageName, null, null, null);
  }

  /** Returns the name of the stage this instruction is executing in. */
  public abstract @Nullable String stageName();

  /**
   * Returns the "original" name of this instruction. This name is a short name assigned by the SDK
   * to each instruction in the pipeline submitted to the runner.
   *
   * <p>The {@code originalName} should be used when reporting measurements associated with a
   * logical step in the pipeline submitted by the user.
   *
   * <p>This name is deliberately short. {@link #userName()} should be preferred for display
   * purposes, although {@link #originalName()} is useful for concisely annotating information that
   * will be mapped back to the "user" name prior to display (eg., structured counters, logs, etc.)
   *
   * <p>Examples: "s2", "s4", "s8"
   */
  public abstract @Nullable String originalName();

  /**
   * Returns the "system" name of this instruction. There may be multiple "system" names associated
   * with a specific "original" name. For instance, the expansion of GroupByKey and the unzipping of
   * Flattens may introduce new system instructions, or duplicate existing instructions.
   *
   * <p>The {@code systemName} should be used when a unique ID across the entire optimized graph is
   * necessary.
   *
   * <p>Examples: "s4", "s4-write-shuffle46", "s813-reify66", "partial-s89"
   */
  public abstract @Nullable String systemName();

  /**
   * Returns the name given to this instruction by the SDK that created the workflow graph.
   *
   * <p>For Dataflow, this name represents the user-readable full path identifying the specific
   * transform. Each user name should be assigned a single {@code originalName} and one or more
   * {@code systemName}s.
   *
   * <p>Examples: "MapElements/Map", "BigShuffle.GroupByFirstNBytes/GroupByKey/Reify"
   */
  public abstract @Nullable String userName();
}
