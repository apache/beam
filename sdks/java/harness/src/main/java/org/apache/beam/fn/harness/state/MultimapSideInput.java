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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.stream.DataStreams.DataStreamDecoder;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.LongMath;

/**
 * An implementation of a multimap side input that utilizes the Beam Fn State API to fetch values.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class MultimapSideInput<K, V> implements MultimapView<K, V> {

  private static final int BULK_READ_SIZE = 100;
  private static final int WEIGH_SAMPLE_LIMIT = 64;

  private enum BulkReadCacheKey {
    INSTANCE
  }

  /**
   * Immutable, bounded snapshot of up to {@link #BULK_READ_SIZE} multimap entries shared across
   * bundles via the state cache.
   */
  private static final class CachedBulkRead<V> implements Weighted {
    private final Map<Object, Iterable<V>> entries;
    private final boolean complete;
    private final long weight;

    CachedBulkRead(Map<Object, Iterable<V>> entries, boolean complete, long weight) {
      this.entries = entries;
      this.complete = complete;
      this.weight = weight;
    }

    @Override
    public long getWeight() {
      return weight;
    }

    Iterable<V> get(Object structuralKey) {
      Iterable<V> result = entries.get(structuralKey);
      if (result == null && complete) {
        // As we read the entire set of values, we don't have to do a lookup to know
        // this key doesn't exist. Missing keys are treated as empty iterables in this multimap.
        return Collections.emptyList();
      }
      return result;
    }
  }

  private final Cache<?, ?> cache;
  private final BeamFnStateClient beamFnStateClient;
  private final StateRequest keysRequest;
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;
  private volatile CachedBulkRead<V> bulkReadResult;
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
    ByteString encodedKey = null;
    if (useBulkRead) {
      if (k == null) {
        // Validate nullability against keyCoder so non-nullable coders (e.g., ByteArrayCoder whose
        // structuralValue(null) does not throw) fail consistently with IllegalStateException
        // regardless of useBulkRead or BULK_READ_SIZE.
        encodedKey = encodeKey(null);
      }
      CachedBulkRead<V> reader = this.bulkReadResult;
      if (reader == null) {
        reader = loadOrGetBulkRead();
      }
      Iterable<V> bulkReadValues = reader.get(keyCoder.structuralValue(k));
      if (bulkReadValues != null) {
        return bulkReadValues;
      }
    }

    if (encodedKey == null) {
      encodedKey = encodeKey(k);
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

  private synchronized CachedBulkRead<V> loadOrGetBulkRead() {
    CachedBulkRead<V> reader = this.bulkReadResult;
    if (reader != null) {
      return reader;
    }

    Cache<BulkReadCacheKey, CachedBulkRead<V>> bulkReadCache = Caches.subCache(cache, "BulkRead");
    reader = bulkReadCache.peek(BulkReadCacheKey.INSTANCE);
    if (reader != null) {
      this.bulkReadResult = reader;
      return reader;
    }

    Map<Object, Iterable<V>> bulkRead = new HashMap<>();
    StateKey bulkReadStateKey =
        StateKey.newBuilder()
            .setMultimapKeysValuesSideInput(
                StateKey.MultimapKeysValuesSideInput.newBuilder()
                    .setTransformId(
                        keysRequest.getStateKey().getMultimapKeysSideInput().getTransformId())
                    .setSideInputId(
                        keysRequest.getStateKey().getMultimapKeysSideInput().getSideInputId())
                    .setWindow(keysRequest.getStateKey().getMultimapKeysSideInput().getWindow()))
            .build();

    StateRequest bulkReadRequest = keysRequest.toBuilder().setStateKey(bulkReadStateKey).build();
    try {
      // Stream lazily via DataStreamDecoder over chunked state responses so pages holding
      // > BULK_READ_SIZE keys stop decoding after entry BULK_READ_SIZE while still supporting
      // individual KV entries that straddle chunk boundaries.
      DataStreamDecoder<KV<K, Iterable<V>>> entries =
          new DataStreamDecoder<>(
              KvCoder.of(keyCoder, IterableCoder.of(valueCoder)),
              new StateFetchingIterators.LazyBlockingStateFetchingIterator(
                  beamFnStateClient, bulkReadRequest));
      while (bulkRead.size() < BULK_READ_SIZE && entries.hasNext()) {
        KV<K, Iterable<V>> entry = entries.next();
        bulkRead.put(
            keyCoder.structuralValue(entry.getKey()), toUnmodifiableIterable(entry.getValue()));
      }
      boolean complete = !entries.hasNext();
      reader = new CachedBulkRead<>(bulkRead, complete, weighBulkRead(bulkRead));
      bulkReadCache.put(BulkReadCacheKey.INSTANCE, reader);
    } catch (Exception exn) {
      // Do not publish partial/failed bulk reads to the shared cache.
      reader = new CachedBulkRead<>(bulkRead, false, 0L);
    }
    this.bulkReadResult = reader;
    return reader;
  }

  private static <V> Iterable<V> toUnmodifiableIterable(Iterable<V> values) {
    if (values instanceof List) {
      return Collections.unmodifiableList((List<V>) values);
    }
    if (values instanceof Collection) {
      return Collections.unmodifiableCollection((Collection<V>) values);
    }
    return Iterables.unmodifiableIterable(values);
  }

  private static <V> long weighBulkRead(Map<Object, Iterable<V>> bulkRead) {
    // CachedBulkRead + HashMap base (~96 B) + Node[] table (next power of 2 >= size / 0.75).
    int size = bulkRead.size();
    int tableCapacity = size == 0 ? 0 : Integer.highestOneBit(Math.max(16, (size * 4) / 3) * 2 - 1);
    long weight = 96L + (long) tableCapacity * Caches.REFERENCE_SIZE;
    for (Map.Entry<Object, Iterable<V>> entry : bulkRead.entrySet()) {
      // HashMap.Node overhead (~32 B) + structural key + unmodifiable values iterable.
      weight = LongMath.saturatedAdd(weight, 32L);
      weight = LongMath.saturatedAdd(weight, Caches.weigh(entry.getKey()));
      weight = LongMath.saturatedAdd(weight, weighIterable(entry.getValue()));
    }
    return weight;
  }

  private static <V> long weighIterable(Iterable<V> iterable) {
    if (iterable instanceof Collection) {
      Collection<?> collection = (Collection<?>) iterable;
      int size = collection.size();
      if (size == 0) {
        return 40L;
      }
      long shellBytes =
          LongMath.saturatedAdd(48L, LongMath.saturatedMultiply(size, Caches.REFERENCE_SIZE));
      if (size > WEIGH_SAMPLE_LIMIT) {
        long sampleWeight = 0L;
        if (collection instanceof List && collection instanceof RandomAccess) {
          List<?> list = (List<?>) collection;
          for (int i = 0; i < WEIGH_SAMPLE_LIMIT; i++) {
            int idx = (int) (((long) i * size) / WEIGH_SAMPLE_LIMIT);
            sampleWeight = LongMath.saturatedAdd(sampleWeight, Caches.weigh(list.get(idx)));
          }
        } else {
          Iterator<?> it = collection.iterator();
          for (int i = 0; i < WEIGH_SAMPLE_LIMIT && it.hasNext(); i++) {
            sampleWeight = LongMath.saturatedAdd(sampleWeight, Caches.weigh(it.next()));
          }
        }
        long avgElementWeight = Math.max(1L, sampleWeight / WEIGH_SAMPLE_LIMIT);
        return LongMath.saturatedAdd(
            shellBytes, LongMath.saturatedMultiply(avgElementWeight, size));
      }
      long elementsWeight = 0L;
      for (Object element : collection) {
        elementsWeight = LongMath.saturatedAdd(elementsWeight, Caches.weigh(element));
      }
      return LongMath.saturatedAdd(shellBytes, elementsWeight);
    }
    return Caches.weigh(iterable);
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
    // Trim small buffers to an exact-sized LiteralByteString so sub-cache keys do not retain an
    // unused backing byte[128] array.
    return output.toByteStringAndReset();
  }
}
