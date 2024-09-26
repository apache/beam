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
import org.apache.beam.runners.dataflow.worker.OperationalLimits;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.UserWorkerRunnerV1Settings;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

/** Global pipeline config for pipelines running in Streaming Engine mode. */
@AutoValue
@Internal
public abstract class StreamingGlobalConfig {

  public static StreamingGlobalConfig.Builder builder() {
    return new AutoValue_StreamingGlobalConfig.Builder()
        .setWindmillServiceEndpoints(ImmutableSet.of())
        .setUserWorkerJobSettings(UserWorkerRunnerV1Settings.newBuilder().build())
        .setOperationalLimits(OperationalLimits.builder().build());
  }

  public abstract OperationalLimits operationalLimits();

  public abstract ImmutableSet<HostAndPort> windmillServiceEndpoints();

  public abstract UserWorkerRunnerV1Settings userWorkerJobSettings();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setWindmillServiceEndpoints(ImmutableSet<HostAndPort> value);

    public abstract Builder setOperationalLimits(OperationalLimits operationalLimits);

    public abstract Builder setUserWorkerJobSettings(UserWorkerRunnerV1Settings settings);

    public abstract StreamingGlobalConfig build();
  }
}
