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

import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetConfigResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

@Internal
@ThreadSafe
public final class StreamingApplianceConfigLoader
    implements StreamingConfigLoader<GetConfigResponse> {

  private final WindmillServerStub windmillServer;
  private final Consumer<GetConfigResponse> onConfigResponse;

  public StreamingApplianceConfigLoader(
      WindmillServerStub windmillServer, Consumer<GetConfigResponse> onConfigResponse) {
    this.windmillServer = windmillServer;
    this.onConfigResponse = onConfigResponse;
  }

  @Override
  public void start() {
    // no-op. Does not perform any asynchronous processing internally.
  }

  @Override
  public Optional<GetConfigResponse> getComputationConfig(String computationId) {
    Preconditions.checkArgument(
        !computationId.isEmpty(),
        "computationId is empty. Cannot fetch computation config without a computationId.");
    GetConfigResponse getConfigResponse =
        windmillServer.getConfig(
            Windmill.GetConfigRequest.newBuilder().addComputations(computationId).build());

    if (getConfigResponse != null) {
      onConfigResponse.accept(getConfigResponse);
    }

    return Optional.ofNullable(getConfigResponse);
  }

  @Override
  public void stop() {
    // no-op. Does not perform any asynchronous processing internally.
  }
}
