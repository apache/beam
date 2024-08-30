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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import static org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateUtil.encodeKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Triple;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WindmillMultimap<K, V> extends SimpleWindmillState implements MultimapState<K, V> {

  private final StateNamespace namespace;
  private final StateTag<MultimapState<K, V>> address;
  private final ByteString stateKey;
  private final String stateFamily;
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;
  // Set to true when user clears the entire multimap, so that we can later send delete request to
  // the windmill backend.
  private boolean cleared = false;
  // We use the structural value of the keys as the key in keyStateMap, so that different java
  // Objects with the same content will be treated as the same Multimap key.
  private Map<Object, KeyState> keyStateMap = Maps.newHashMap();
  // If true, all keys are cached in keyStateMap with existence == KNOWN_EXIST.
  private boolean allKeysKnown;
  // True if all contents of this multimap are cached in this object.
  private boolean complete;
  // hasLocalAdditions and hasLocalRemovals track whether there are local changes that needs to be
  // propagated to windmill.
  private boolean hasLocalAdditions = false;
  private boolean hasLocalRemovals = false;

  WindmillMultimap(
      StateNamespace namespace,
      StateTag<MultimapState<K, V>> address,
      String stateFamily,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      boolean isNewShardingKey) {
    this.namespace = namespace;
    this.address = address;
    this.stateKey = encodeKey(namespace, address);
    this.stateFamily = stateFamily;
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
    this.complete = isNewShardingKey;
    this.allKeysKnown = isNewShardingKey;
  }

  private static <K, V> Iterable<Map.Entry<K, V>> unnestCachedEntries(
      Iterable<Map.Entry<Object, Triple<K, Boolean, ConcatIterables<V>>>> cachedEntries) {
    return Iterables.concat(
        Iterables.transform(
            cachedEntries,
            entry ->
                Iterables.transform(
                    entry.getValue().getRight(),
                    v -> new AbstractMap.SimpleEntry<>(entry.getValue().getLeft(), v))));
  }

  @Override
  public void put(K key, V value) {
    final Object structuralKey = keyCoder.structuralValue(key);
    hasLocalAdditions = true;
    keyStateMap.compute(
        structuralKey,
        (k, v) -> {
          if (v == null) v = new KeyState(key);
          v.existence = KeyExistence.KNOWN_EXIST;
          v.localAdditions.add(value);
          return v;
        });
  }

  // Initiates a backend state read to fetch all entries if necessary.
  private Future<Iterable<Map.Entry<ByteString, Iterable<V>>>> necessaryEntriesFromStorageFuture(
      boolean omitValues) {
    if (complete) {
      // Since we're complete, even if there are entries in storage we don't need to read them.
      return Futures.immediateFuture(Collections.emptyList());
    } else {
      return reader.multimapFetchAllFuture(omitValues, stateKey, stateFamily, valueCoder);
    }
  }

  // Initiates a backend state read to fetch a single entry if necessary.
  private Future<Iterable<V>> necessaryKeyEntriesFromStorageFuture(K key) {
    try {
      ByteStringOutputStream keyStream = new ByteStringOutputStream();
      keyCoder.encode(key, keyStream, Coder.Context.OUTER);
      return reader.multimapFetchSingleEntryFuture(
          keyStream.toByteString(), stateKey, stateFamily, valueCoder);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ReadableState<Iterable<V>> get(K key) {
    return new ReadResultReadableState(key);
  }

  @Override
  protected Windmill.WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKeyAndFamily cache)
      throws IOException {
    if (!cleared && !hasLocalAdditions && !hasLocalRemovals) {
      cache.put(namespace, address, this, 1);
      return Windmill.WorkItemCommitRequest.newBuilder().buildPartial();
    }
    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    Windmill.TagMultimapUpdateRequest.Builder builder = commitBuilder.addMultimapUpdatesBuilder();
    builder.setTag(stateKey).setStateFamily(stateFamily);

    if (cleared) {
      builder.setDeleteAll(true);
    }
    if (hasLocalRemovals || hasLocalAdditions) {
      ByteStringOutputStream keyStream = new ByteStringOutputStream();
      ByteStringOutputStream valueStream = new ByteStringOutputStream();
      Iterator<Map.Entry<Object, KeyState>> iterator = keyStateMap.entrySet().iterator();
      while (iterator.hasNext()) {
        KeyState keyState = iterator.next().getValue();
        if (!keyState.removedLocally && keyState.localAdditions.isEmpty()) {
          if (keyState.existence == KeyExistence.KNOWN_NONEXISTENT) iterator.remove();
          continue;
        }
        keyCoder.encode(keyState.originalKey, keyStream, Coder.Context.OUTER);
        ByteString encodedKey = keyStream.toByteStringAndReset();
        Windmill.TagMultimapEntry.Builder entryBuilder = builder.addUpdatesBuilder();
        entryBuilder.setEntryName(encodedKey);
        if (keyState.removedLocally) entryBuilder.setDeleteAll(true);
        keyState.removedLocally = false;
        if (!keyState.localAdditions.isEmpty()) {
          for (V value : keyState.localAdditions) {
            valueCoder.encode(value, valueStream, Coder.Context.OUTER);
            ByteString encodedValue = valueStream.toByteStringAndReset();
            entryBuilder.addValues(encodedValue);
          }
          // Move newly added values from localAdditions to keyState.values as those new values
          // now
          // are also persisted in Windmill. If a key now has no more values and is not
          // KNOWN_EXIST,
          // remove it from cache.
          if (keyState.valuesCached) {
            keyState.values.extendWith(keyState.localAdditions);
            keyState.valuesSize += keyState.localAdditions.size();
          }
          // Create a new localAdditions so that the cached values are unaffected.
          keyState.localAdditions = Lists.newArrayList();
        }
        if (!keyState.valuesCached && keyState.existence != KeyExistence.KNOWN_EXIST) {
          iterator.remove();
        }
      }
    }

    hasLocalAdditions = false;
    hasLocalRemovals = false;
    cleared = false;

    cache.put(namespace, address, this, 1);
    return commitBuilder.buildPartial();
  }

  @Override
  public void remove(K key) {
    final Object structuralKey = keyCoder.structuralValue(key);
    // does not insert key if allKeysKnown.
    KeyState keyState =
        keyStateMap.computeIfAbsent(structuralKey, k -> allKeysKnown ? null : new KeyState(key));
    if (keyState == null || keyState.existence == KeyExistence.KNOWN_NONEXISTENT) {
      return;
    }
    if (keyState.valuesCached && keyState.valuesSize == 0 && !keyState.removedLocally) {
      // no data in windmill and no need to keep state, deleting from local cache is sufficient.
      keyStateMap.remove(structuralKey);
    } else {
      // there may be data in windmill that need to be removed.
      hasLocalRemovals = true;
      keyState.removedLocally = true;
      keyState.values = new ConcatIterables<>();
      keyState.valuesSize = 0;
      keyState.existence = KeyExistence.KNOWN_NONEXISTENT;
    }
    if (!keyState.localAdditions.isEmpty()) {
      keyState.localAdditions = Lists.newArrayList();
    }
    keyState.valuesCached = true;
  }

  @Override
  public void clear() {
    keyStateMap = Maps.newHashMap();
    cleared = true;
    complete = true;
    allKeysKnown = true;
    hasLocalAdditions = false;
    hasLocalRemovals = false;
  }

  @Override
  public ReadableState<Iterable<K>> keys() {
    return new KeysReadableState();
  }

  @Override
  public ReadableState<Iterable<Map.Entry<K, V>>> entries() {
    return new EntriesReadableState();
  }

  @Override
  public ReadableState<Boolean> containsKey(K key) {
    return new ContainsKeyReadableState(key);
  }

  // Currently, isEmpty is implemented by reading all keys and could potentially be optimized.
  // But note that if isEmpty is often followed by iterating over keys then maybe not too bad; if
  // isEmpty is followed by iterating over both keys and values then it won't help much.
  @Override
  public ReadableState<Boolean> isEmpty() {
    return new IsEmptyReadableState();
  }

  private enum KeyExistence {
    // this key is known to exist, it has at least 1 value in either localAdditions or windmill
    KNOWN_EXIST,
    // this key is known to be nonexistent, it has 0 value in both localAdditions and windmill
    KNOWN_NONEXISTENT,
    // we don't know if this key is in this multimap, it has exact 0 value in localAddition, but
    // may have no or any number of values in windmill. This is just to provide a mapping between
    // the original key and the structural key.
    UNKNOWN_EXISTENCE
  }

  private class KeyState {
    final K originalKey;
    KeyExistence existence;
    // valuesCached can be true if only existence == KNOWN_EXIST and all values of this key are
    // cached (both values and localAdditions).
    boolean valuesCached;
    // Represents the values in windmill. When new values are added during user processing, they
    // are added to localAdditions but not values. Those new values will be added to values only
    // after they are persisted into windmill and removed from localAdditions
    ConcatIterables<V> values;
    int valuesSize;

    // When new values are added during user processing, they are added to localAdditions, so that
    // we can later try to persist them in windmill. When a key is removed during user processing,
    // we mark removedLocally to be true so that we can later try to delete it from windmill. If
    // localAdditions is not empty and removedLocally is true, values in localAdditions will be
    // added to windmill after old values in windmill are removed.
    List<V> localAdditions;
    boolean removedLocally;

    KeyState(K originalKey) {
      this.originalKey = originalKey;
      existence = KeyExistence.UNKNOWN_EXISTENCE;
      valuesCached = complete;
      values = new ConcatIterables<>();
      valuesSize = 0;
      localAdditions = Lists.newArrayList();
      removedLocally = false;
    }
  }

  private class ReadResultReadableState implements ReadableState<Iterable<V>> {
    final Object structuralKey;
    private final K key;

    public ReadResultReadableState(K key) {
      this.key = key;
      structuralKey = keyCoder.structuralValue(key);
    }

    @Override
    public Iterable<V> read() {
      KeyState keyState = null;
      if (allKeysKnown) {
        keyState = keyStateMap.get(structuralKey);
        if (keyState == null || keyState.existence == KeyExistence.UNKNOWN_EXISTENCE) {
          if (keyState != null) keyStateMap.remove(structuralKey);
          return Collections.emptyList();
        }
      } else {
        keyState = keyStateMap.computeIfAbsent(structuralKey, k -> new KeyState(key));
      }
      if (keyState.existence == KeyExistence.KNOWN_NONEXISTENT) {
        return Collections.emptyList();
      }
      Iterable<V> localNewValues =
          Iterables.limit(keyState.localAdditions, keyState.localAdditions.size());
      if (keyState.removedLocally) {
        // this key has been removed locally but the removal hasn't been sent to windmill,
        // thus values in windmill(if any) are obsolete, and we only care about local values.
        return Iterables.unmodifiableIterable(localNewValues);
      }
      if (keyState.valuesCached || complete) {
        return Iterables.unmodifiableIterable(
            Iterables.concat(
                Iterables.limit(keyState.values, keyState.valuesSize), localNewValues));
      }
      Future<Iterable<V>> persistedData = necessaryKeyEntriesFromStorageFuture(key);
      try (Closeable scope = scopedReadState()) {
        final Iterable<V> persistedValues = persistedData.get();
        // Iterables.isEmpty() is O(1).
        if (Iterables.isEmpty(persistedValues)) {
          if (keyState.localAdditions.isEmpty()) {
            // empty in both cache and windmill, mark key as KNOWN_NONEXISTENT.
            keyState.existence = KeyExistence.KNOWN_NONEXISTENT;
            return Collections.emptyList();
          }
          return Iterables.unmodifiableIterable(localNewValues);
        }
        keyState.existence = KeyExistence.KNOWN_EXIST;
        if (persistedValues instanceof Weighted) {
          keyState.valuesCached = true;
          ConcatIterables<V> it = new ConcatIterables<>();
          it.extendWith(persistedValues);
          keyState.values = it;
          keyState.valuesSize = Iterables.size(persistedValues);
        }
        return Iterables.unmodifiableIterable(Iterables.concat(persistedValues, localNewValues));
      } catch (InterruptedException | ExecutionException | IOException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new RuntimeException("Unable to read Multimap state", e);
      }
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public ReadableState<Iterable<V>> readLater() {
      WindmillMultimap.this.necessaryKeyEntriesFromStorageFuture(key);
      return this;
    }
  }

  private class KeysReadableState implements ReadableState<Iterable<K>> {

    private Map<Object, K> cachedExistKeys() {
      return keyStateMap.entrySet().stream()
          .filter(entry -> entry.getValue().existence == KeyExistence.KNOWN_EXIST)
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().originalKey));
    }

    @Override
    public Iterable<K> read() {
      if (allKeysKnown) {
        return Iterables.unmodifiableIterable(cachedExistKeys().values());
      }
      Future<Iterable<Map.Entry<ByteString, Iterable<V>>>> persistedData =
          necessaryEntriesFromStorageFuture(true);
      try (Closeable scope = scopedReadState()) {
        Iterable<Map.Entry<ByteString, Iterable<V>>> entries = persistedData.get();
        if (entries instanceof Weighted) {
          // This is a known amount of data, cache them all.
          entries.forEach(
              entry -> {
                try {
                  K originalKey = keyCoder.decode(entry.getKey().newInput(), Coder.Context.OUTER);
                  KeyState keyState =
                      keyStateMap.computeIfAbsent(
                          keyCoder.structuralValue(originalKey), stk -> new KeyState(originalKey));
                  if (keyState.existence == KeyExistence.UNKNOWN_EXISTENCE) {
                    keyState.existence = KeyExistence.KNOWN_EXIST;
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
          allKeysKnown = true;
          keyStateMap
              .values()
              .removeIf(
                  keyState ->
                      keyState.existence != KeyExistence.KNOWN_EXIST && !keyState.removedLocally);
          return Iterables.unmodifiableIterable(cachedExistKeys().values());
        } else {
          Map<Object, K> cachedExistKeys = Maps.newHashMap();
          Set<Object> cachedNonExistKeys = Sets.newHashSet();
          keyStateMap.forEach(
              (structuralKey, keyState) -> {
                switch (keyState.existence) {
                  case KNOWN_EXIST:
                    cachedExistKeys.put(structuralKey, keyState.originalKey);
                    break;
                  case KNOWN_NONEXISTENT:
                    cachedNonExistKeys.add(structuralKey);
                    break;
                  default:
                    break;
                }
              });
          // keysOnlyInWindmill is lazily loaded.
          Iterable<K> keysOnlyInWindmill =
              Iterables.filter(
                  Iterables.transform(
                      entries,
                      entry -> {
                        try {
                          K originalKey =
                              keyCoder.decode(entry.getKey().newInput(), Coder.Context.OUTER);
                          Object structuralKey = keyCoder.structuralValue(originalKey);
                          if (cachedExistKeys.containsKey(structuralKey)
                              || cachedNonExistKeys.contains(structuralKey)) return null;
                          return originalKey;
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }),
                  Objects::nonNull);
          return Iterables.unmodifiableIterable(
              Iterables.concat(cachedExistKeys.values(), keysOnlyInWindmill));
        }
      } catch (InterruptedException | ExecutionException | IOException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new RuntimeException("Unable to read state", e);
      }
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public ReadableState<Iterable<K>> readLater() {
      WindmillMultimap.this.necessaryEntriesFromStorageFuture(true);
      return this;
    }
  }

  private class EntriesReadableState implements ReadableState<Iterable<Map.Entry<K, V>>> {
    @Override
    public Iterable<Map.Entry<K, V>> read() {
      if (complete) {
        return Iterables.unmodifiableIterable(
            unnestCachedEntries(mergedCachedEntries(null).entrySet()));
      }
      Future<Iterable<Map.Entry<ByteString, Iterable<V>>>> persistedData =
          necessaryEntriesFromStorageFuture(false);
      try (Closeable scope = scopedReadState()) {
        Iterable<Map.Entry<ByteString, Iterable<V>>> entries = persistedData.get();
        if (Iterables.isEmpty(entries)) {
          complete = true;
          allKeysKnown = true;
          return Iterables.unmodifiableIterable(
              unnestCachedEntries(mergedCachedEntries(null).entrySet()));
        }
        if (!(entries instanceof Weighted)) {
          return nonWeightedEntries(entries);
        }
        // This is a known amount of data, cache them all.
        entries.forEach(
            entry -> {
              try {
                final K originalKey =
                    keyCoder.decode(entry.getKey().newInput(), Coder.Context.OUTER);
                final Object structuralKey = keyCoder.structuralValue(originalKey);
                KeyState keyState =
                    keyStateMap.computeIfAbsent(structuralKey, k -> new KeyState(originalKey));
                // Ignore any key from windmill that has been marked pending deletion or is
                // fully cached.
                if (keyState.existence == KeyExistence.KNOWN_NONEXISTENT
                    || (keyState.existence == KeyExistence.KNOWN_EXIST && keyState.valuesCached))
                  return;
                // Or else cache contents from windmill.
                keyState.existence = KeyExistence.KNOWN_EXIST;
                keyState.values.extendWith(entry.getValue());
                keyState.valuesSize += Iterables.size(entry.getValue());
                keyState.valuesCached = true;
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
        allKeysKnown = true;
        complete = true;
        return Iterables.unmodifiableIterable(
            unnestCachedEntries(mergedCachedEntries(null).entrySet()));
      } catch (InterruptedException | ExecutionException | IOException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new RuntimeException("Unable to read state", e);
      }
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public ReadableState<Iterable<Map.Entry<K, V>>> readLater() {
      WindmillMultimap.this.necessaryEntriesFromStorageFuture(false);
      return this;
    }

    /**
     * Collect all cached entries into a map and all KNOWN_NONEXISTENT keys to
     * knownNonexistentKeys(if not null). Note that this method is not side-effect-free: it unloads
     * any key that is not KNOWN_EXIST and not pending deletion from cache; also if complete it
     * marks the valuesCached of any key that is KNOWN_EXIST to true, entries() depends on this
     * behavior when the fetched result is weighted to iterate the whole keyStateMap one less time.
     * For each cached key, returns its structural key and a tuple of <original key,
     * keyState.valuesCached, keyState.values + keyState.localAdditions>.
     */
    private Map<Object, Triple<K, Boolean, ConcatIterables<V>>> mergedCachedEntries(
        Set<Object> knownNonexistentKeys) {
      Map<Object, Triple<K, Boolean, ConcatIterables<V>>> cachedEntries = Maps.newHashMap();
      keyStateMap
          .entrySet()
          .removeIf(
              (entry -> {
                Object structuralKey = entry.getKey();
                KeyState keyState = entry.getValue();
                if (complete && keyState.existence == KeyExistence.KNOWN_EXIST) {
                  keyState.valuesCached = true;
                }
                ConcatIterables<V> it = null;
                if (!keyState.localAdditions.isEmpty()) {
                  it = new ConcatIterables<>();
                  it.extendWith(
                      Iterables.limit(keyState.localAdditions, keyState.localAdditions.size()));
                }
                if (keyState.valuesCached) {
                  if (it == null) it = new ConcatIterables<>();
                  it.extendWith(Iterables.limit(keyState.values, keyState.valuesSize));
                }
                if (it != null) {
                  cachedEntries.put(
                      structuralKey, Triple.of(keyState.originalKey, keyState.valuesCached, it));
                }
                if (knownNonexistentKeys != null
                    && keyState.existence == KeyExistence.KNOWN_NONEXISTENT)
                  knownNonexistentKeys.add(structuralKey);
                return (keyState.existence == KeyExistence.KNOWN_NONEXISTENT
                        && !keyState.removedLocally)
                    || keyState.existence == KeyExistence.UNKNOWN_EXISTENCE;
              }));
      return cachedEntries;
    }

    private Iterable<Map.Entry<K, V>> nonWeightedEntries(
        Iterable<Map.Entry<ByteString, Iterable<V>>> lazyWindmillEntries) {
      class ResultIterable implements Iterable<Map.Entry<K, V>> {
        private final Iterable<Map.Entry<ByteString, Iterable<V>>> lazyWindmillEntries;
        private final Map<Object, Triple<K, Boolean, ConcatIterables<V>>> cachedEntries;
        private final Set<Object> knownNonexistentKeys;

        ResultIterable(
            Map<Object, Triple<K, Boolean, ConcatIterables<V>>> cachedEntries,
            Iterable<Map.Entry<ByteString, Iterable<V>>> lazyWindmillEntries,
            Set<Object> knownNonexistentKeys) {
          this.cachedEntries = cachedEntries;
          this.lazyWindmillEntries = lazyWindmillEntries;
          this.knownNonexistentKeys = knownNonexistentKeys;
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
          // Each time when the Iterable returned by entries() is iterated, a new Iterator is
          // created. Every iterator must keep its own copy of seenCachedKeys so that if a key
          // is paginated into multiple iterables from windmill, the cached values of this key
          // will only be returned once.
          Set<Object> seenCachedKeys = Sets.newHashSet();
          // notFullyCachedEntries returns all entries from windmill that are not fully cached
          // and combines them with localAdditions. If a key is fully cached, contents of this
          // key from windmill are ignored.
          Iterable<Triple<Object, K, Iterable<V>>> notFullyCachedEntries =
              Iterables.filter(
                  Iterables.transform(
                      lazyWindmillEntries,
                      entry -> {
                        try {
                          final K key =
                              keyCoder.decode(entry.getKey().newInput(), Coder.Context.OUTER);
                          final Object structuralKey = keyCoder.structuralValue(key);
                          // key is deleted in cache thus fully cached.
                          if (knownNonexistentKeys.contains(structuralKey)) return null;
                          Triple<K, Boolean, ConcatIterables<V>> triple =
                              cachedEntries.get(structuralKey);
                          // no record of key in cache, return content in windmill.
                          if (triple == null) {
                            return Triple.of(structuralKey, key, entry.getValue());
                          }
                          // key is fully cached in cache.
                          if (triple.getMiddle()) return null;

                          // key is not fully cached, combine the content in windmill with local
                          // additions with only the first observed page for the key to ensure
                          // it is not repeated.
                          if (!seenCachedKeys.add(structuralKey)) {
                            return Triple.of(structuralKey, key, entry.getValue());
                          } else {
                            ConcatIterables<V> it = new ConcatIterables<>();
                            it.extendWith(triple.getRight());
                            it.extendWith(entry.getValue());
                            return Triple.of(structuralKey, key, it);
                          }
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }),
                  Objects::nonNull);
          Iterator<Map.Entry<K, V>> unnestWindmill =
              Iterators.concat(
                  Iterables.transform(
                          notFullyCachedEntries,
                          entry ->
                              Iterables.transform(
                                      entry.getRight(),
                                      v -> new AbstractMap.SimpleEntry<>(entry.getMiddle(), v))
                                  .iterator())
                      .iterator());
          Iterator<Map.Entry<K, V>> fullyCached =
              unnestCachedEntries(
                      Iterables.filter(
                          cachedEntries.entrySet(),
                          entry -> !seenCachedKeys.contains(entry.getKey())))
                  .iterator();
          return Iterators.concat(unnestWindmill, fullyCached);
        }
      }

      Set<Object> knownNonexistentKeys = Sets.newHashSet();
      Map<Object, Triple<K, Boolean, ConcatIterables<V>>> cachedEntries =
          mergedCachedEntries(knownNonexistentKeys);
      return Iterables.unmodifiableIterable(
          new ResultIterable(cachedEntries, lazyWindmillEntries, knownNonexistentKeys));
    }
  }

  private class ContainsKeyReadableState implements ReadableState<Boolean> {
    final Object structuralKey;
    private final K key;
    ReadableState<Iterable<V>> values;

    public ContainsKeyReadableState(K key) {
      this.key = key;
      structuralKey = keyCoder.structuralValue(key);
      values = null;
    }

    @Override
    public Boolean read() {
      KeyState keyState = keyStateMap.getOrDefault(structuralKey, null);
      if (keyState != null && keyState.existence != KeyExistence.UNKNOWN_EXISTENCE) {
        return keyState.existence == KeyExistence.KNOWN_EXIST;
      }
      if (values == null) {
        values = WindmillMultimap.this.get(key);
      }
      return !Iterables.isEmpty(values.read());
    }

    @Override
    public ReadableState<Boolean> readLater() {
      if (values == null) {
        values = WindmillMultimap.this.get(key);
      }
      values.readLater();
      return this;
    }
  }

  private class IsEmptyReadableState implements ReadableState<Boolean> {
    ReadableState<Iterable<K>> keys = null;

    @Override
    public Boolean read() {
      for (KeyState keyState : keyStateMap.values()) {
        if (keyState.existence == KeyExistence.KNOWN_EXIST) {
          return false;
        }
      }
      if (keys == null) {
        keys = WindmillMultimap.this.keys();
      }
      return Iterables.isEmpty(keys.read());
    }

    @Override
    public ReadableState<Boolean> readLater() {
      if (keys == null) {
        keys = WindmillMultimap.this.keys();
      }
      keys.readLater();
      return this;
    }
  }
}
