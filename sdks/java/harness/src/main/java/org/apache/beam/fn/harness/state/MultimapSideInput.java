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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/**
 * An implementation of a multimap side input that utilizes the Beam Fn State API to fetch values.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class MultimapSideInput<K, V> implements MultimapView<K, V> {

  private static final int BULK_READ_SIZE = 100;

  private final Cache<?, ?> cache;
  private final BeamFnStateClient beamFnStateClient;
  private final StateRequest keysRequest;
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;
  private volatile Function<ByteString, Iterable<V>> bulkReadResult;
  private final boolean useBulkRead;

  public MultimapSideInput(
      Cache<?, ?> cache,
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      StateKey stateKey,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      boolean useBulkRead) {
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
    this.useBulkRead = useBulkRead;
  }

  @Override
  public Iterable<K> get() {
    return StateFetchingIterators.readAllAndDecodeStartingFrom(
        cache, beamFnStateClient, keysRequest, keyCoder);
  }

  @Override
  public Iterable<V> get(K k) {
    ByteString encodedKey = encodeKey(k);

    if (useBulkRead) {
      if (bulkReadResult == null) {
        synchronized (this) {
          if (bulkReadResult == null) {
            Map<ByteString, Iterable<V>> bulkRead = new HashMap<>();
            StateKey bulkReadStateKey =
                StateKey.newBuilder()
                    .setMultimapKeysValuesSideInput(
                        StateKey.MultimapKeysValuesSideInput.newBuilder()
                            .setTransformId(
                                keysRequest
                                    .getStateKey()
                                    .getMultimapKeysSideInput()
                                    .getTransformId())
                            .setSideInputId(
                                keysRequest
                                    .getStateKey()
                                    .getMultimapKeysSideInput()
                                    .getSideInputId())
                            .setWindow(
                                keysRequest.getStateKey().getMultimapKeysSideInput().getWindow()))
                    .build();

            StateRequest bulkReadRequest =
                keysRequest.toBuilder().setStateKey(bulkReadStateKey).build();
            try {
              Iterator<KV<K, Iterable<V>>> entries =
                  StateFetchingIterators.readAllAndDecodeStartingFrom(
                          Caches.subCache(cache, "ValuesForKey", encodedKey),
                          beamFnStateClient,
                          bulkReadRequest,
                          KvCoder.of(keyCoder, IterableCoder.of(valueCoder)))
                      .iterator();
              while (bulkRead.size() < BULK_READ_SIZE && entries.hasNext()) {
                KV<K, Iterable<V>> entry = entries.next();
                bulkRead.put(encodeKey(entry.getKey()), entry.getValue());
              }
              if (entries.hasNext()) {
                bulkReadResult = bulkRead::get;
              } else {
                bulkReadResult =
                    key -> {
                      Iterable<V> result = bulkRead.get(key);
                      if (result == null) {
                        // As we read the entire set of values, we don't have to do a lookup to know
                        // this key doesn't exist.
                        // Missing keys are treated as empty iterables in this multimap.
                        return Collections.emptyList();
                      } else {
                        return result;
                      }
                    };
              }
            } catch (Exception exn) {
              bulkReadResult = bulkRead::get;
            }
          }
        }
      }

      Iterable<V> bulkReadValues = bulkReadResult.apply(encodedKey);
      if (bulkReadValues != null) {
        return bulkReadValues;
      }
    }

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

  private ByteString encodeKey(K k) {
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
    return output.toByteString();
  }
}
