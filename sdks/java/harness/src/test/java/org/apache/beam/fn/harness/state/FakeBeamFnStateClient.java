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

import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/** A fake implementation of a {@link BeamFnStateClient} to aid with testing. */
public class FakeBeamFnStateClient implements BeamFnStateClient {
  private final Map<StateKey, List<ByteString>> data;
  private int currentId;

  public FakeBeamFnStateClient(Map<StateKey, ByteString> initialData) {
    this(initialData, 6);
  }

  public FakeBeamFnStateClient(Map<StateKey, ByteString> initialData, int chunkSize) {
    this.data =
        new ConcurrentHashMap<>(
            Maps.transformValues(
                initialData,
                (ByteString all) -> {
                  List<ByteString> chunks = new ArrayList<>();
                  for (int i = 0; i < Math.max(1, all.size()); i += chunkSize) {
                    chunks.add(all.substring(i, Math.min(all.size(), i + chunkSize)));
                  }
                  return chunks;
                }));
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

  @Override
  public void handle(
      StateRequest.Builder requestBuilder, CompletableFuture<StateResponse> responseFuture) {
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

    switch (request.getRequestCase()) {
      case GET:
        // Chunk gets into 6 byte return blocks
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
            data.getOrDefault(request.getStateKey(), Collections.singletonList(ByteString.EMPTY));
        List<ByteString> newValue = new ArrayList<>();
        newValue.addAll(previousValue);
        ByteString newData = request.getAppend().getData();
        if (newData.size() % 2 == 0) {
          newValue.remove(newValue.size() - 1);
          newValue.add(previousValue.get(previousValue.size() - 1).concat(newData));
        } else {
          newValue.add(newData);
        }
        data.put(request.getStateKey(), newValue);
        response = StateResponse.newBuilder().setAppend(StateAppendResponse.getDefaultInstance());
        break;

      default:
        throw new IllegalStateException(
            String.format("Unknown request type %s", request.getRequestCase()));
    }

    responseFuture.complete(response.setId(requestBuilder.getId()).build());
  }

  private String generateId() {
    return Integer.toString(++currentId);
  }
}
