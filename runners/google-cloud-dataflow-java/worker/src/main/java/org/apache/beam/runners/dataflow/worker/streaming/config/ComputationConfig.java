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
package org.apache.beam.runners.dataflow.worker.streaming.config;

import com.google.api.services.dataflow.model.MapTask;
import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

@Internal
@AutoValue
public abstract class ComputationConfig {

  public static ComputationConfig create(
      MapTask mapTask,
      @Nullable Map<String, String> userTransformToStateFamilyName,
      Map<String, String> stateNameMap) {
    return new AutoValue_ComputationConfig(
        mapTask,
        Optional.ofNullable(userTransformToStateFamilyName)
            .map(ImmutableMap::copyOf)
            .orElseGet(ImmutableMap::of),
        ImmutableMap.copyOf(stateNameMap));
  }

  public abstract MapTask mapTask();

  public abstract ImmutableMap<String, String> userTransformToStateFamilyName();

  public abstract ImmutableMap<String, String> stateNameMap();

  /** Interface to fetch configurations for a specific computation. */
  public interface Fetcher {
    default void start() {}

    default void stop() {}

    Optional<ComputationConfig> fetchConfig(String computationId);

    StreamingGlobalConfigHandle getGlobalConfigHandle();
  }
}
