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

import com.google.api.services.dataflow.model.StreamingComputationConfig;
import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

/** Pipeline configuration for jobs running w/ Streaming Engine. */
@AutoValue
@Internal
public abstract class StreamingPipelineConfig {

  private static final long DEFAULT_MAX_WORK_ITEM_COMMIT_BYTES = 180 << 20;

  public static StreamingPipelineConfig.Builder builder() {
    return new AutoValue_StreamingPipelineConfig.Builder()
        .setMaxWorkItemCommitBytes(DEFAULT_MAX_WORK_ITEM_COMMIT_BYTES)
        .setComputationConfig(null)
        .setUserStepToStateFamilyNameMap(new HashMap<>())
        .setWindmillServiceEndpoints(ImmutableSet.of());
  }

  public static StreamingPipelineConfig forAppliance(
      Map<String, String> userStepToStateFamilyNameMap) {
    return builder()
        .setMaxWorkItemCommitBytes(Integer.MAX_VALUE)
        .setUserStepToStateFamilyNameMap(userStepToStateFamilyNameMap)
        .build();
  }

  public abstract long maxWorkItemCommitBytes();

  public abstract Map<String, String> userStepToStateFamilyNameMap();

  public abstract Optional<StreamingComputationConfig> computationConfig();

  public abstract ImmutableSet<HostAndPort> windmillServiceEndpoints();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setMaxWorkItemCommitBytes(long value);

    public abstract Builder setUserStepToStateFamilyNameMap(Map<String, String> value);

    public abstract Builder setComputationConfig(@Nullable StreamingComputationConfig value);

    public abstract Builder setWindmillServiceEndpoints(ImmutableSet<HostAndPort> value);

    public abstract StreamingPipelineConfig build();
  }
}
