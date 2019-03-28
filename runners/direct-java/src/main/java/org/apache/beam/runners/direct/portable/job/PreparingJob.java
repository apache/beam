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
package org.apache.beam.runners.direct.portable.job;

import com.google.auto.value.AutoValue;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactStagingService;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.Struct;

/** A Job with a {@code prepare} call but no corresponding {@code run} call. */
@AutoValue
abstract class PreparingJob implements AutoCloseable {
  public static Builder builder() {
    return new AutoValue_PreparingJob.Builder();
  }

  abstract Pipeline getPipeline();

  abstract Struct getOptions();

  abstract String getStagingSessionToken();

  abstract GrpcFnServer<BeamFileSystemArtifactStagingService> getArtifactStagingServer();

  @Override
  public void close() throws Exception {
    getArtifactStagingServer().close();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setPipeline(Pipeline pipeline);

    abstract Builder setOptions(Struct options);

    abstract Builder setStagingSessionToken(String stagingSessionToken);

    abstract Builder setArtifactStagingServer(
        GrpcFnServer<BeamFileSystemArtifactStagingService> server);

    abstract PreparingJob build();
  }
}
