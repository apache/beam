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
package org.apache.beam.fn.harness.data;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;

/** A {@link BeamFnTimerClient} that uses gRPC for sending and receiving timers. */
public class BeamFnTimerGrpcClient implements BeamFnTimerClient {
  private final BeamFnDataClient beamFnDataClient;
  private final ApiServiceDescriptor timerApiServiceDescriptor;

  public BeamFnTimerGrpcClient(
      BeamFnDataClient beamFnDataClient, ApiServiceDescriptor timerApiServiceDescriptor) {
    this.beamFnDataClient = beamFnDataClient;
    this.timerApiServiceDescriptor = timerApiServiceDescriptor;
  }

  @Override
  public <K> CloseableFnDataReceiver<Timer<K>> register(
      LogicalEndpoint timerEndpoint,
      Coder<Timer<K>> coder,
      Consumer<Elements> responseEmbedElementsConsumer) {
    checkArgument(
        timerEndpoint.isTimer(),
        "Expected to receive timer endpoint but received %s",
        timerEndpoint);
    return beamFnDataClient.send(
        timerApiServiceDescriptor, timerEndpoint, coder, responseEmbedElementsConsumer);
  }
}
