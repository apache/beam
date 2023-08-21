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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;

/**
 * An implementation of a multimap side input that utilizes the Beam Fn State API to fetch values.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class MultimapSideInput<K, V> implements MultimapView<K, V> {

  private final Cache<?, ?> cache;
  private final BeamFnStateClient beamFnStateClient;
  private final StateRequest keysRequest;
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;

  public MultimapSideInput(
      Cache<?, ?> cache,
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      StateKey stateKey,
      Coder<K> keyCoder,
      Coder<V> valueCoder) {
    checkArgument(
        stateKey.hasMultimapKeysSideInput(),
        "Expected MultimapKeysSideInput StateKey but received %s.",
        stateKey);
    this.cache = cache;
    this.beamFnStateClient = beamFnStateClient;
    this.keysRequest =
        StateRequest.newBuilder().setInstructionId(instructionId).setStateKey(stateKey).build();
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public Iterable<K> get() {
    return StateFetchingIterators.readAllAndDecodeStartingFrom(
        cache, beamFnStateClient, keysRequest, keyCoder);
  }

  @Override
  public Iterable<V> get(K k) {
    ByteStringOutputStream output = new ByteStringOutputStream();
    try {
      keyCoder.encode(k, output);
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to encode key %s for side input id %s.",
              k, keysRequest.getStateKey().getMultimapKeysSideInput().getSideInputId()),
          e);
    }
    ByteString encodedKey = output.toByteString();
    StateKey stateKey =
        StateKey.newBuilder()
            .setMultimapSideInput(
                StateKey.MultimapSideInput.newBuilder()
                    .setTransformId(
                        keysRequest.getStateKey().getMultimapKeysSideInput().getTransformId())
                    .setSideInputId(
                        keysRequest.getStateKey().getMultimapKeysSideInput().getSideInputId())
                    .setWindow(keysRequest.getStateKey().getMultimapKeysSideInput().getWindow())
                    .setKey(encodedKey))
            .build();

    StateRequest request = keysRequest.toBuilder().setStateKey(stateKey).build();
    return StateFetchingIterators.readAllAndDecodeStartingFrom(
        Caches.subCache(cache, "ValuesForKey", encodedKey), beamFnStateClient, request, valueCoder);
  }
}
