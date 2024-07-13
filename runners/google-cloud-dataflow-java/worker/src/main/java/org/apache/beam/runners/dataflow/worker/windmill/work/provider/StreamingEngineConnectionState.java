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
<<<<<<<< HEAD:runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/streaming/harness/StreamingEngineConnectionState.java
package org.apache.beam.runners.dataflow.worker.streaming.harness;
========
package org.apache.beam.runners.dataflow.worker.windmill.work.provider;
>>>>>>>> 1479775a19e (change StreamingEngineClient to FanOutStreamingEngineWorkProvider and have it implement WorkProvider interface. Move class to windmill/work/provider directory, update visibility for dependent classes and move tests, add GetWorkBudgetOwnerInterface):runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/windmill/work/provider/StreamingEngineConnectionState.java

import com.google.auto.value.AutoValue;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints.Endpoint;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Represents the current state of connections to Streaming Engine. Connections are updated when
 * backend workers assigned to the key ranges being processed by this user worker change during
 * pipeline execution. For example, changes can happen via autoscaling, load-balancing, or other
 * backend updates.
 */
@AutoValue
abstract class StreamingEngineConnectionState {
  static final StreamingEngineConnectionState EMPTY = builder().build();

  static Builder builder() {
    return new AutoValue_StreamingEngineConnectionState.Builder()
        .setWindmillConnections(ImmutableMap.of())
        .setWindmillStreams(ImmutableMap.of())
        .setGlobalDataStreams(ImmutableMap.of());
  }

  abstract ImmutableMap<Endpoint, WindmillConnection> windmillConnections();

  abstract ImmutableMap<WindmillConnection, WindmillStreamSender> windmillStreams();

  /** Mapping of GlobalDataIds and the direct GetDataStreams used fetch them. */
  abstract ImmutableMap<String, Supplier<GetDataStream>> globalDataStreams();

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder setWindmillConnections(
        ImmutableMap<Endpoint, WindmillConnection> value);

    public abstract Builder setWindmillStreams(
        ImmutableMap<WindmillConnection, WindmillStreamSender> value);

    public abstract Builder setGlobalDataStreams(
        ImmutableMap<String, Supplier<GetDataStream>> value);

    public abstract StreamingEngineConnectionState build();
  }
}
