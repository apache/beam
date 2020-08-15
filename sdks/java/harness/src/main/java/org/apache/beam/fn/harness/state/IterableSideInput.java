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
package org.apache.beam.fn.harness.state;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.DataStreams;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;

/**
 * An implementation of a iterable side input that utilizes the Beam Fn State API to fetch values.
 *
 * <p>TODO: Support block level caching and prefetch.
 */
public class IterableSideInput<T> implements IterableView<T> {

  private final BeamFnStateClient beamFnStateClient;
  private final String instructionId;
  private final String ptransformId;
  private final String sideInputId;
  private final ByteString encodedWindow;
  private final Coder<T> valueCoder;

  public IterableSideInput(
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      String ptransformId,
      String sideInputId,
      ByteString encodedWindow,
      Coder<T> valueCoder) {
    this.beamFnStateClient = beamFnStateClient;
    this.instructionId = instructionId;
    this.ptransformId = ptransformId;
    this.sideInputId = sideInputId;
    this.encodedWindow = encodedWindow;
    this.valueCoder = valueCoder;
  }

  @Override
  public Iterable<T> get() {
    StateRequest.Builder requestBuilder = StateRequest.newBuilder();
    requestBuilder
        .setInstructionId(instructionId)
        .getStateKeyBuilder()
        .getIterableSideInputBuilder()
        .setTransformId(ptransformId)
        .setSideInputId(sideInputId)
        .setWindow(encodedWindow);

    return new LazyCachingIteratorToIterable<>(
        new DataStreams.DataStreamDecoder(
            valueCoder,
            DataStreams.inbound(
                StateFetchingIterators.readAllStartingFrom(
                    beamFnStateClient, requestBuilder.build()))));
  }
}
