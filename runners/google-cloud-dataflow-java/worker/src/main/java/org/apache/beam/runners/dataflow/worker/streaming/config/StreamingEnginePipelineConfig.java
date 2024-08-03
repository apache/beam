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

import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

/** Global pipeline config for pipelines running in Streaming Engine mode. */
@AutoValue
@Internal
public abstract class StreamingEnginePipelineConfig {

  private static final long DEFAULT_MAX_WORK_ITEM_COMMIT_BYTES = 180 << 20;

  public static StreamingEnginePipelineConfig.Builder builder() {
    return new AutoValue_StreamingEnginePipelineConfig.Builder()
        .setMaxWorkItemCommitBytes(DEFAULT_MAX_WORK_ITEM_COMMIT_BYTES)
        .setMaxOutputKeyBytes(Long.MAX_VALUE)
        .setMaxOutputValueBytes(Long.MAX_VALUE)
        .setUserStepToStateFamilyNameMap(new HashMap<>())
        .setWindmillServiceEndpoints(ImmutableSet.of());
  }

  public abstract long maxWorkItemCommitBytes();

  public abstract long maxOutputKeyBytes();

  public abstract long maxOutputValueBytes();

  public abstract Map<String, String> userStepToStateFamilyNameMap();

  public abstract ImmutableSet<HostAndPort> windmillServiceEndpoints();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setMaxWorkItemCommitBytes(long value);

    public abstract Builder setMaxOutputKeyBytes(long value);

    public abstract Builder setMaxOutputValueBytes(long value);

    public abstract Builder setUserStepToStateFamilyNameMap(Map<String, String> value);

    public abstract Builder setWindmillServiceEndpoints(ImmutableSet<HostAndPort> value);

    public abstract StreamingEnginePipelineConfig build();
  }
}
