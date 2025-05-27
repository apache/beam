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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCache;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;

/** Dependencies needed to create {@link StreamingWorkerHarness} implementations. */
@AutoValue
public abstract class StreamingWorkerDependencies {

  public static Builder builder() {
    return new AutoValue_StreamingWorkerDependencies.Builder();
  }

  public abstract ComputationConfig.Fetcher configFetcher();

  public abstract ComputationStateCache computationStateCache();

  public abstract WindmillServerStub windmillServer();

  public abstract GrpcWindmillStreamFactory windmillStreamFactory();

  public abstract @Nullable ChannelCache channelCache();

  public abstract ChannelCachingStubFactory stubFactory();

  public abstract @Nullable GrpcDispatcherClient windmillDispatcherClient();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setConfigFetcher(ComputationConfig.Fetcher value);

    public abstract Builder setComputationStateCache(ComputationStateCache value);

    public abstract Builder setWindmillServer(WindmillServerStub value);

    public abstract Builder setWindmillStreamFactory(GrpcWindmillStreamFactory value);

    public abstract Builder setWindmillDispatcherClient(GrpcDispatcherClient value);

    public abstract GrpcDispatcherClient windmillDispatcherClient();

    public abstract Builder setChannelCache(ChannelCache channelCache);

    public abstract Builder setStubFactory(ChannelCachingStubFactory stubFactory);

    public abstract StreamingWorkerDependencies build();
  }
}
