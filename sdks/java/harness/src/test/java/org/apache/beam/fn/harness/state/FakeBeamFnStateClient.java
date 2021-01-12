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

import java.util.Collections;
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
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;

/** A fake implementation of a {@link BeamFnStateClient} to aid with testing. */
public class FakeBeamFnStateClient implements BeamFnStateClient {
  private final Map<StateKey, ByteString> data;
  private int currentId;

  public FakeBeamFnStateClient(Map<StateKey, ByteString> initialData) {
    this.data = new ConcurrentHashMap<>(initialData);
  }

  public Map<StateKey, ByteString> getData() {
    return Collections.unmodifiableMap(data);
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
        // Chunk gets into 5 byte return blocks
        ByteString byteString = data.getOrDefault(request.getStateKey(), ByteString.EMPTY);
        int block = 0;
        if (request.getGet().getContinuationToken().size() > 0) {
          block = Integer.parseInt(request.getGet().getContinuationToken().toStringUtf8());
        }
        ByteString returnBlock =
            byteString.substring(block * 5, Math.min(byteString.size(), (block + 1) * 5));
        ByteString continuationToken = ByteString.EMPTY;
        if (byteString.size() > (block + 1) * 5) {
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
        ByteString previousValue = data.getOrDefault(request.getStateKey(), ByteString.EMPTY);
        data.put(request.getStateKey(), previousValue.concat(request.getAppend().getData()));
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
