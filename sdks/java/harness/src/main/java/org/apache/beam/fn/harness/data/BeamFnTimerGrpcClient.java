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

import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;

/**
 * A {@link BeamFnTimerClient} that uses gRPC for sending and receiving timers.
 *
 * <p>TODO: Handle closing clients that are currently not a consumer nor are being consumed.
 */
public class BeamFnTimerGrpcClient implements BeamFnTimerClient {
  private final BeamFnDataClient beamFnDataClient;
  private final ApiServiceDescriptor timerApiServiceDescriptor;

  public BeamFnTimerGrpcClient(
      BeamFnDataClient beamFnDataClient, ApiServiceDescriptor timerApiServiceDescriptor) {
    this.beamFnDataClient = beamFnDataClient;
    this.timerApiServiceDescriptor = timerApiServiceDescriptor;
  }

  @Override
  public <K> TimerHandler<K> register(
      LogicalEndpoint timerEndpoint, Coder<Timer<K>> coder, FnDataReceiver<Timer<K>> receiver) {
    checkArgument(
        timerEndpoint.isTimer(),
        "Expected to receive timer endpoint but received %s",
        timerEndpoint);
    InboundDataClient inbound =
        beamFnDataClient.receive(timerApiServiceDescriptor, timerEndpoint, coder, receiver);
    CloseableFnDataReceiver<Timer<K>> outbound =
        beamFnDataClient.send(timerApiServiceDescriptor, timerEndpoint, coder);

    return new TimerHandler<K>() {
      @Override
      public void flush() throws Exception {
        outbound.flush();
      }

      @Override
      public void close() throws Exception {
        outbound.close();
      }

      @Override
      public void accept(Timer<K> input) throws Exception {
        outbound.accept(input);
      }

      @Override
      public void awaitCompletion() throws InterruptedException, Exception {
        inbound.awaitCompletion();
      }

      @Override
      public boolean isDone() {
        return inbound.isDone();
      }

      @Override
      public void cancel() {
        inbound.cancel();
      }

      @Override
      public void complete() {
        inbound.complete();
      }

      @Override
      public void fail(Throwable t) {
        inbound.fail(t);
      }
    };
  }
}
