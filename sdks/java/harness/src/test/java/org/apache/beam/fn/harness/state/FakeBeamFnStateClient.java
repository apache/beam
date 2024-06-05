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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.RequestCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;

/** A fake implementation of a {@link BeamFnStateClient} to aid with testing. */
public class FakeBeamFnStateClient implements BeamFnStateClient {
  private static final int DEFAULT_CHUNK_SIZE = 6;
  private final Map<StateKey, List<ByteString>> data;
  private int currentId;

  public <V> FakeBeamFnStateClient(Coder<V> valueCoder, Map<StateKey, List<V>> initialData) {
    this(valueCoder, initialData, DEFAULT_CHUNK_SIZE);
  }

  public <V> FakeBeamFnStateClient(
      Coder<V> valueCoder, Map<StateKey, List<V>> initialData, int chunkSize) {
    this(Maps.transformValues(initialData, (value) -> KV.of(valueCoder, value)), chunkSize);
  }

  public FakeBeamFnStateClient(Map<StateKey, KV<Coder<?>, List<?>>> initialData) {
    this(initialData, DEFAULT_CHUNK_SIZE);
  }

  public FakeBeamFnStateClient(Map<StateKey, KV<Coder<?>, List<?>>> initialData, int chunkSize) {
    Map<StateKey, List<ByteString>> encodedData =
        new HashMap<>(
            Maps.transformValues(
                initialData,
                (KV<Coder<?>, List<?>> coderAndValues) -> {
                  List<ByteString> chunks = new ArrayList<>();
                  ByteStringOutputStream output = new ByteStringOutputStream();
                  for (Object value : coderAndValues.getValue()) {
                    try {
                      ((Coder<Object>) coderAndValues.getKey()).encode(value, output);
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                    if (output.size() >= chunkSize) {
                      ByteString chunk = output.toByteStringAndReset();
                      int i = 0;
                      for (; i + chunkSize <= chunk.size(); i += chunkSize) {
                        // We specifically use a copy of the bytes instead of a proper substring
                        // so that debugging is easier since we don't have to worry about the
                        // substring being a view over the original string.
                        chunks.add(
                            ByteString.copyFrom(chunk.substring(i, i + chunkSize).toByteArray()));
                      }
                      if (i < chunk.size()) {
                        chunks.add(
                            ByteString.copyFrom(chunk.substring(i, chunk.size()).toByteArray()));
                      }
                    }
                  }
                  // Add the last chunk
                  if (output.size() > 0) {
                    chunks.add(output.toByteString());
                  }
                  return chunks;
                }));
    this.data =
        new ConcurrentHashMap<>(
            Maps.filterValues(encodedData, byteStrings -> !byteStrings.isEmpty()));
  }

  public Map<StateKey, ByteString> getData() {
    return Maps.transformValues(
        data,
        bs -> {
          ByteString all = ByteString.EMPTY;
          for (ByteString b : bs) {
            all = all.concat(b);
          }
          return all;
        });
  }

  // Returns data reflecting the state api appended values opposed
  // to the logical concatenated value.
  public Map<StateKey, List<ByteString>> getRawData() {
    return data;
  }

  @Override
  public CompletableFuture<StateResponse> handle(StateRequest.Builder requestBuilder) {
    // The id should never be filled out
    assertEquals("", requestBuilder.getId());
    requestBuilder.setId(generateId());

    StateRequest request = requestBuilder.build();
    StateKey key = request.getStateKey();
    StateResponse.Builder response;

    assertNotEquals(RequestCase.REQUEST_NOT_SET, request.getRequestCase());
    assertNotEquals(TypeCase.TYPE_NOT_SET, key.getTypeCase());
    // multimap side input and runner based state keys only support get requests
    if (key.getTypeCase() == TypeCase.MULTIMAP_SIDE_INPUT || key.getTypeCase() == TypeCase.RUNNER) {
      assertEquals(RequestCase.GET, request.getRequestCase());
    }
    if (key.getTypeCase() == TypeCase.MULTIMAP_KEYS_VALUES_SIDE_INPUT && !data.containsKey(key)) {
      // Allow testing this not being supported rather than blindly returning the empty list.
      throw new UnsupportedOperationException("No multimap keys values states provided.");
    }

    switch (request.getRequestCase()) {
      case GET:
        List<ByteString> byteStrings =
            data.getOrDefault(request.getStateKey(), Collections.singletonList(ByteString.EMPTY));
        int block = 0;
        if (request.getGet().getContinuationToken().size() > 0) {
          block = Integer.parseInt(request.getGet().getContinuationToken().toStringUtf8());
        }
        ByteString returnBlock = byteStrings.get(block);
        ByteString continuationToken = ByteString.EMPTY;
        if (byteStrings.size() > block + 1) {
          continuationToken = ByteString.copyFromUtf8(Integer.toString(block + 1));
        }
        response =
            StateResponse.newBuilder()
                .setGet(
                    StateGetResponse.newBuilder()
                        .setData(returnBlock)
                        .setContinuationToken(continuationToken));
        break;

      case CLEAR:
        data.remove(request.getStateKey());
        response = StateResponse.newBuilder().setClear(StateClearResponse.getDefaultInstance());
        break;

      case APPEND:
        List<ByteString> previousValue =
            data.computeIfAbsent(request.getStateKey(), (unused) -> new ArrayList<>());
        previousValue.add(request.getAppend().getData());
        response = StateResponse.newBuilder().setAppend(StateAppendResponse.getDefaultInstance());
        break;

      default:
        throw new IllegalStateException(
            String.format("Unknown request type %s", request.getRequestCase()));
    }

    return CompletableFuture.completedFuture(response.setId(requestBuilder.getId()).build());
  }

  private String generateId() {
    return Integer.toString(++currentId);
  }

  public int getCallCount() {
    return currentId;
  }
}
