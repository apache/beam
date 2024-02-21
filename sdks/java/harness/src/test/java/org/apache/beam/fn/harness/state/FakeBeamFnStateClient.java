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

import static org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.RequestCase.GET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListEntry;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListRange;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListStateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListStateUpdateResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.RequestCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.joda.time.Instant;

/** A fake implementation of a {@link BeamFnStateClient} to aid with testing. */
public class FakeBeamFnStateClient implements BeamFnStateClient {
  private static final int DEFAULT_CHUNK_SIZE = 6;
  private final Map<StateKey, List<ByteString>> data;
  private int currentId;
  private final Map<StateKey, NavigableSet<Long>> orderedListKeys;

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

    Map<StateKey, Coder<Object>> orderedListInitialData =
        new HashMap<>(
            Maps.transformValues(
                Maps.filterKeys(
                    initialData, (k) -> k.getTypeCase() == TypeCase.ORDERED_LIST_USER_STATE),
                (v) -> {
                  // make sure the provided coder is a TimestampedValueCoder.
                  assert v.getKey() instanceof TimestampedValueCoder;
                  return ((TimestampedValueCoder<Object>) v.getKey()).getValueCoder();
                }));

    this.orderedListKeys = new HashMap<>();
    for (Map.Entry<StateKey, Coder<Object>> entry : orderedListInitialData.entrySet()) {
      long sortKey = entry.getKey().getOrderedListUserState().getSortKey();

      StateKey.Builder keyBuilder = entry.getKey().toBuilder();
      keyBuilder.getOrderedListUserStateBuilder().clearSortKey();

      this.orderedListKeys
          .computeIfAbsent(keyBuilder.build(), (unused) -> new TreeSet<>())
          .add(sortKey);
    }

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

  static class EncodeOnlyByteStringCoder extends AtomicCoder<ByteString> {
    public static EncodeOnlyByteStringCoder of() {
      return INSTANCE;
    }

    private static final EncodeOnlyByteStringCoder INSTANCE = new EncodeOnlyByteStringCoder();

    private EncodeOnlyByteStringCoder() {}

    @Override
    public void encode(ByteString value, OutputStream os) throws IOException {
      // Unlike normal ByteStringCoder, we don't write the length before the content.
      // The stream receiver should use a coder that can decode the data without knowing
      // the length.
      value.writeTo(os);
    }

    @Override
    public ByteString decode(InputStream is) throws IOException {
      throw new RuntimeException("decode is not supported in EncodeOnlyByteStringCoder");
    }
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
      assertEquals(GET, request.getRequestCase());
    }
    if (key.getTypeCase() == TypeCase.MULTIMAP_KEYS_VALUES_SIDE_INPUT && !data.containsKey(key)) {
      // Allow testing this not being supported rather than blindly returning the empty list.
      throw new UnsupportedOperationException("No multimap keys values states provided.");
    }

    switch (request.getRequestCase()) {
      case GET:
        {
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
        }
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

      case ORDERED_LIST_GET:
        {
          long start = request.getOrderedListGet().getRange().getStart();
          long end = request.getOrderedListGet().getRange().getEnd();

          KvCoder<Long, Integer> coder = KvCoder.of(VarLongCoder.of(), VarIntCoder.of());
          long sortKey = start;
          int index = 0;
          if (request.getOrderedListGet().getContinuationToken().size() > 0) {
            try {
              // The continuation format here is the sort key (long) followed by an index (int)
              KV<Long, Integer> cursor =
                  coder.decode(request.getOrderedListGet().getContinuationToken().newInput());
              sortKey = cursor.getKey();
              index = cursor.getValue();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          ByteString continuationToken;
          ByteString returnBlock = ByteString.EMPTY;
          try {
            if (sortKey < start || sortKey >= end) {
              throw new IndexOutOfBoundsException("sort key out of range");
            }

            NavigableSet<Long> subset =
                orderedListKeys
                    .getOrDefault(request.getStateKey(), new TreeSet<>())
                    .subSet(sortKey, true, end, false);

            // get the effective sort key currently, can throw NoSuchElementException
            Long nextSortKey = subset.first();

            StateKey.Builder keyBuilder = request.getStateKey().toBuilder();
            keyBuilder.getOrderedListUserStateBuilder().setSortKey(nextSortKey);
            List<ByteString> byteStrings =
                data.getOrDefault(keyBuilder.build(), Collections.singletonList(ByteString.EMPTY));

            // get the block specified in continuation token, can throw IndexOutOfBoundsException
            returnBlock = byteStrings.get(index);

            if (byteStrings.size() > index + 1) {
              // more blocks from this sort key
              index += 1;
            } else {
              // finish navigating the current sort key and need to find the next one,
              // can throw NoSuchElementException
              nextSortKey = subset.tailSet(nextSortKey, false).first();
              index = 0;
            }

            ByteStringOutputStream outputStream = new ByteStringOutputStream();
            try {
              KV<Long, Integer> cursor = KV.of(nextSortKey, index);
              coder.encode(cursor, outputStream);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            continuationToken = outputStream.toByteString();
          } catch (NoSuchElementException | IndexOutOfBoundsException e) {
            continuationToken = ByteString.EMPTY;
          }
          response =
              StateResponse.newBuilder()
                  .setOrderedListGet(
                      OrderedListStateGetResponse.newBuilder()
                          .setData(returnBlock)
                          .setContinuationToken(continuationToken));
        }
        break;

      case ORDERED_LIST_UPDATE:
        for (OrderedListRange r : request.getOrderedListUpdate().getDeletesList()) {
          List<Long> keysToRemove =
              new ArrayList<>(
                  orderedListKeys
                      .getOrDefault(request.getStateKey(), new TreeSet<>())
                      .subSet(r.getStart(), true, r.getEnd(), false));

          for (Long l : keysToRemove) {
            StateKey.Builder keyBuilder = request.getStateKey().toBuilder();
            keyBuilder.getOrderedListUserStateBuilder().setSortKey(l);
            data.remove(keyBuilder.build());
            orderedListKeys.get(request.getStateKey()).remove(l);
          }
        }

        for (OrderedListEntry e : request.getOrderedListUpdate().getInsertsList()) {
          StateKey.Builder keyBuilder = request.getStateKey().toBuilder();
          keyBuilder.getOrderedListUserStateBuilder().setSortKey(e.getSortKey());

          ByteStringOutputStream outStream = new ByteStringOutputStream();
          TimestampedValue<ByteString> tv =
              TimestampedValue.of(e.getData(), Instant.ofEpochMilli(e.getSortKey()));
          try {
            TimestampedValueCoder.of(EncodeOnlyByteStringCoder.of()).encode(tv, outStream);
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
          ByteString output = outStream.toByteString();

          List<ByteString> previousValues =
              data.computeIfAbsent(keyBuilder.build(), (unused) -> new ArrayList<>());
          previousValues.add(output);

          orderedListKeys
              .computeIfAbsent(request.getStateKey(), (unused) -> new TreeSet<>())
              .add(e.getSortKey());
        }
        response =
            StateResponse.newBuilder()
                .setOrderedListUpdate(OrderedListStateUpdateResponse.getDefaultInstance());
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
