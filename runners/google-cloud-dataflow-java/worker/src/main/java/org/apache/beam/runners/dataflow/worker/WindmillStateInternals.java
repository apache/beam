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
package org.apache.beam.runners.dataflow.worker;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTable;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTag.StateBinder;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateContexts;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Implementation of {@link StateInternals} using Windmill to manage the underlying data. */
class WindmillStateInternals<K> implements StateInternals {

  /**
   * The key will be null when not in a keyed context, from the users perspective. There is still a
   * "key" for the Windmill computation, but it cannot be meaningfully deserialized.
   */
  private final @Nullable K key;

  @Override
  public @Nullable K getKey() {
    return key;
  }

  private static class CachingStateTable<K> extends StateTable {
    private final @Nullable K key;
    private final String stateFamily;
    private final WindmillStateReader reader;
    private final WindmillStateCache.ForKey cache;
    boolean isNewKey;
    private final Supplier<Closeable> scopedReadStateSupplier;

    public CachingStateTable(
        @Nullable K key,
        String stateFamily,
        WindmillStateReader reader,
        WindmillStateCache.ForKey cache,
        boolean isNewKey,
        Supplier<Closeable> scopedReadStateSupplier) {
      this.key = key;
      this.stateFamily = stateFamily;
      this.reader = reader;
      this.cache = cache;
      this.isNewKey = isNewKey;
      this.scopedReadStateSupplier = scopedReadStateSupplier;
    }

    @Override
    protected StateBinder binderForNamespace(
        final StateNamespace namespace, final StateContext<?> c) {
      // Look up state objects in the cache or create new ones if not found.  The state will
      // be added to the cache in persist().
      return new StateBinder() {
        @Override
        public <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder) {
          WindmillBag<T> result = (WindmillBag<T>) cache.get(namespace, address);
          if (result == null) {
            result = new WindmillBag<>(namespace, address, stateFamily, elemCoder, isNewKey);
          }
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }

        @Override
        public <T> SetState<T> bindSet(StateTag<SetState<T>> spec, Coder<T> elemCoder) {
          throw new UnsupportedOperationException(
              String.format("%s is not supported", SetState.class.getSimpleName()));
        }

        @Override
        public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
            StateTag<MapState<KeyT, ValueT>> spec,
            Coder<KeyT> mapKeyCoder,
            Coder<ValueT> mapValueCoder) {
          throw new UnsupportedOperationException(
              String.format("%s is not supported", MapState.class.getSimpleName()));
        }

        @Override
        public WatermarkHoldState bindWatermark(
            StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
          WindmillWatermarkHold result = (WindmillWatermarkHold) cache.get(namespace, address);
          if (result == null) {
            result =
                new WindmillWatermarkHold(
                    namespace, address, stateFamily, timestampCombiner, isNewKey);
          }
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }

        @Override
        public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
            StateTag<CombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            CombineFn<InputT, AccumT, OutputT> combineFn) {
          WindmillCombiningState<InputT, AccumT, OutputT> result =
              new WindmillCombiningState<InputT, AccumT, OutputT>(
                  namespace, address, stateFamily, accumCoder, combineFn, cache, isNewKey);
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }

        @Override
        public <InputT, AccumT, OutputT>
            CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
                StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                Coder<AccumT> accumCoder,
                CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
          return bindCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
        }

        @Override
        public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
          WindmillValue<T> result = (WindmillValue<T>) cache.get(namespace, address);
          if (result == null) {
            result = new WindmillValue<>(namespace, address, stateFamily, coder, isNewKey);
          }
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }
      };
    }
  }

  private WindmillStateCache.ForKey cache;
  Supplier<Closeable> scopedReadStateSupplier;
  private StateTable workItemState;

  public WindmillStateInternals(
      @Nullable K key,
      String stateFamily,
      WindmillStateReader reader,
      boolean isNewKey,
      WindmillStateCache.ForKey cache,
      Supplier<Closeable> scopedReadStateSupplier) {
    this.key = key;
    this.cache = cache;
    this.scopedReadStateSupplier = scopedReadStateSupplier;
    this.workItemState =
        new CachingStateTable<K>(
            key, stateFamily, reader, cache, isNewKey, scopedReadStateSupplier);
  }

  public void persist(final Windmill.WorkItemCommitRequest.Builder commitBuilder) {
    List<Future<WorkItemCommitRequest>> commitsToMerge = new ArrayList<>();

    // Call persist on each first, which may schedule some futures for reading.
    for (State location : workItemState.values()) {
      if (!(location instanceof WindmillState)) {
        throw new IllegalStateException(
            String.format(
                "%s wasn't created by %s -- unable to persist it",
                location.getClass().getSimpleName(), getClass().getSimpleName()));
      }

      try {
        commitsToMerge.add(((WindmillState) location).persist(cache));
      } catch (IOException e) {
        throw new RuntimeException("Unable to persist state", e);
      }
    }

    // All cached State objects now have known values.
    // Clear any references to the underlying reader to prevent space leaks.
    // The next work unit to use these cached State objects will reset the
    // reader to a current reader in case those values are modified.
    for (State location : workItemState.values()) {
      ((WindmillState) location).cleanupAfterWorkItem();
    }

    // Clear out the map of already retrieved state instances.
    workItemState.clear();

    try (Closeable scope = scopedReadStateSupplier.get()) {
      for (Future<WorkItemCommitRequest> commitFuture : commitsToMerge) {
        commitBuilder.mergeFrom(commitFuture.get());
      }
    } catch (ExecutionException | InterruptedException | IOException exc) {
      if (exc instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Failed to retrieve Windmill state during persist()", exc);
    }
  }

  /** Encodes the given namespace and address as {@code &lt;namespace&gt;+&lt;address&gt;}. */
  @VisibleForTesting
  static ByteString encodeKey(StateNamespace namespace, StateTag<?> address) {
    try {
      // Use ByteString.Output rather than concatenation and String.format. We build these keys
      // a lot, and this leads to better performance results. See associated benchmarks.
      ByteString.Output stream = ByteString.newOutput();
      OutputStreamWriter writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8);

      // stringKey starts and ends with a slash.  We separate it from the
      // StateTag ID by a '+' (which is guaranteed not to be in the stringKey) because the
      // ID comes from the user.
      namespace.appendTo(writer);
      writer.write('+');
      address.appendTo(writer);
      writer.flush();
      return stream.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Abstract base class for all Windmill state.
   *
   * <p>Note that these are not thread safe; each state object is associated with a key and thus
   * only accessed by a single thread at once.
   */
  @NotThreadSafe
  private abstract static class WindmillState {
    protected Supplier<Closeable> scopedReadStateSupplier;
    protected WindmillStateReader reader;

    /**
     * Return an asynchronously computed {@link WorkItemCommitRequest}. The request should be of a
     * form that can be merged with others (only add to repeated fields).
     */
    abstract Future<WorkItemCommitRequest> persist(WindmillStateCache.ForKey cache)
        throws IOException;

    /**
     * Prepare this (possibly reused from cache) state for reading from {@code reader} if needed.
     */
    void initializeForWorkItem(
        WindmillStateReader reader, Supplier<Closeable> scopedReadStateSupplier) {
      this.reader = reader;
      this.scopedReadStateSupplier = scopedReadStateSupplier;
    }

    /**
     * This (now cached) state should never need to interact with the reader until the next work
     * item. Clear it to prevent space leaks. The reader will be reset by {@link
     * #initializeForWorkItem} upon the next work item.
     */
    void cleanupAfterWorkItem() {
      this.reader = null;
      this.scopedReadStateSupplier = null;
    }

    Closeable scopedReadState() {
      return scopedReadStateSupplier.get();
    }
  }

  /**
   * Base class for implementations of {@link WindmillState} where the {@link #persist} call does
   * not require any asynchronous reading.
   */
  private abstract static class SimpleWindmillState extends WindmillState {
    @Override
    public final Future<WorkItemCommitRequest> persist(WindmillStateCache.ForKey cache)
        throws IOException {
      return Futures.immediateFuture(persistDirectly(cache));
    }

    /**
     * Returns a {@link WorkItemCommitRequest} that can be used to persist this state to Windmill.
     */
    protected abstract WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKey cache)
        throws IOException;
  }

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<T> address) {
    return workItemState.get(namespace, address, StateContexts.nullContext());
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
    return workItemState.get(namespace, address, c);
  }

  private static class WindmillValue<T> extends SimpleWindmillState implements ValueState<T> {
    private final StateNamespace namespace;
    private final StateTag<ValueState<T>> address;
    private final ByteString stateKey;
    private final String stateFamily;
    private final Coder<T> coder;

    /** Whether we've modified the value since creation of this state. */
    private boolean modified = false;
    /** Whether the in memory value is the true value. */
    private boolean valueIsKnown = false;
    /** The size of the encoded value */
    private long cachedSize = -1;

    private T value;

    private WindmillValue(
        StateNamespace namespace,
        StateTag<ValueState<T>> address,
        String stateFamily,
        Coder<T> coder,
        boolean isNewKey) {
      this.namespace = namespace;
      this.address = address;
      this.stateKey = encodeKey(namespace, address);
      this.stateFamily = stateFamily;
      this.coder = coder;
      if (isNewKey) {
        this.valueIsKnown = true;
        this.value = null;
      }
    }

    @Override
    public void clear() {
      modified = true;
      valueIsKnown = true;
      value = null;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public WindmillValue<T> readLater() {
      getFuture();
      return this;
    }

    @Override
    public T read() {
      try (Closeable scope = scopedReadState()) {
        if (!valueIsKnown) {
          cachedSize = -1;
        }
        value = getFuture().get();
        valueIsKnown = true;
        return value;
      } catch (InterruptedException | ExecutionException | IOException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new RuntimeException("Unable to read value from state", e);
      }
    }

    @Override
    public void write(T value) {
      modified = true;
      valueIsKnown = true;
      cachedSize = -1;
      this.value = value;
    }

    @Override
    protected WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKey cache)
        throws IOException {
      if (!valueIsKnown) {
        // The value was never read, written or cleared.
        // Thus nothing to update in Windmill.
        // And no need to add to global cache.
        return WorkItemCommitRequest.newBuilder().buildPartial();
      }

      ByteString encoded = null;
      if (cachedSize == -1 || modified) {
        ByteString.Output stream = ByteString.newOutput();
        if (value != null) {
          coder.encode(value, stream, Coder.Context.OUTER);
        }
        encoded = stream.toByteString();
        cachedSize = encoded.size();
      }

      // Place in cache to avoid a future read.
      cache.put(namespace, address, this, cachedSize);

      if (!modified) {
        // The value was read, but never written or cleared.
        // But nothing to update in Windmill.
        return WorkItemCommitRequest.newBuilder().buildPartial();
      }

      // The value was written or cleared. Commit that change to Windmill.
      modified = false;
      WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
      commitBuilder
          .addValueUpdatesBuilder()
          .setTag(stateKey)
          .setStateFamily(stateFamily)
          .getValueBuilder()
          .setData(encoded)
          .setTimestamp(Long.MAX_VALUE);
      return commitBuilder.buildPartial();
    }

    private Future<T> getFuture() {
      // WindmillStateReader guarantees that we can ask for a future for a particular tag multiple
      // times and it will efficiently be reused.
      return valueIsKnown
          ? Futures.immediateFuture(value)
          : reader.valueFuture(stateKey, stateFamily, coder);
    }
  }

  private static class WindmillBag<T> extends SimpleWindmillState implements BagState<T> {

    private final StateNamespace namespace;
    private final StateTag<BagState<T>> address;
    private final ByteString stateKey;
    private final String stateFamily;
    private final Coder<T> elemCoder;

    private boolean cleared = false;
    /**
     * If non-{@literal null}, this contains the complete contents of the bag, except for any local
     * additions. If {@literal null} then we don't know if Windmill contains additional values which
     * should be part of the bag. We'll need to read them if the work item actually wants the bag
     * contents.
     */
    private ConcatIterables<T> cachedValues = null;

    private List<T> localAdditions = new ArrayList<>();
    private long encodedSize = 0;

    private WindmillBag(
        StateNamespace namespace,
        StateTag<BagState<T>> address,
        String stateFamily,
        Coder<T> elemCoder,
        boolean isNewKey) {
      this.namespace = namespace;
      this.address = address;
      this.stateKey = encodeKey(namespace, address);
      this.stateFamily = stateFamily;
      this.elemCoder = elemCoder;
      if (isNewKey) {
        this.cachedValues = new ConcatIterables<>();
      }
    }

    @Override
    public void clear() {
      cleared = true;
      cachedValues = new ConcatIterables<>();
      localAdditions = new ArrayList<>();
      encodedSize = 0;
    }

    /**
     * Return iterable over all bag values in Windmill which should contribute to overall bag
     * contents.
     */
    private Iterable<T> fetchData(Future<Iterable<T>> persistedData) {
      try (Closeable scope = scopedReadState()) {
        if (cachedValues != null) {
          return cachedValues.snapshot();
        }
        Iterable<T> data = persistedData.get();
        if (data instanceof Weighted) {
          // We have a known bounded amount of data; cache it.
          cachedValues = new ConcatIterables<>();
          cachedValues.extendWith(data);
          encodedSize = ((Weighted) data).getWeight();
          return cachedValues.snapshot();
        } else {
          // This is an iterable that may not fit in memory at once; don't cache it.
          return data;
        }
      } catch (InterruptedException | ExecutionException | IOException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new RuntimeException("Unable to read state", e);
      }
    }

    public boolean valuesAreCached() {
      return cachedValues != null;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public WindmillBag<T> readLater() {
      getFuture();
      return this;
    }

    @Override
    public Iterable<T> read() {
      return Iterables.concat(
          fetchData(getFuture()), Iterables.limit(localAdditions, localAdditions.size()));
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          WindmillBag.this.readLater();
          return this;
        }

        @Override
        public Boolean read() {
          return Iterables.isEmpty(fetchData(getFuture())) && localAdditions.isEmpty();
        }
      };
    }

    @Override
    public void add(T input) {
      localAdditions.add(input);
    }

    @Override
    public WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKey cache)
        throws IOException {
      WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();

      Windmill.TagBag.Builder bagUpdatesBuilder = null;

      if (cleared) {
        bagUpdatesBuilder = commitBuilder.addBagUpdatesBuilder();
        bagUpdatesBuilder.setDeleteAll(true);
        cleared = false;
      }

      if (!localAdditions.isEmpty()) {
        // Tell Windmill to capture the local additions.
        if (bagUpdatesBuilder == null) {
          bagUpdatesBuilder = commitBuilder.addBagUpdatesBuilder();
        }
        for (T value : localAdditions) {
          ByteString.Output stream = ByteString.newOutput();
          // Encode the value
          elemCoder.encode(value, stream, Coder.Context.OUTER);
          ByteString encoded = stream.toByteString();
          if (cachedValues != null) {
            // We'll capture this value in the cache below.
            // Capture the value's size now since we have it.
            encodedSize += encoded.size();
          }
          bagUpdatesBuilder.addValues(encoded);
        }
      }

      if (bagUpdatesBuilder != null) {
        bagUpdatesBuilder.setTag(stateKey).setStateFamily(stateFamily);
      }

      if (cachedValues != null) {
        if (!localAdditions.isEmpty()) {
          // Capture the local additions in the cached value since we and
          // Windmill are now in agreement.
          cachedValues.extendWith(localAdditions);
        }
        // We now know the complete bag contents, and any read on it will yield a
        // cached value, so cache it for future reads.
        cache.put(namespace, address, this, encodedSize);
      }

      // Don't reuse the localAdditions object; we don't want future changes to it to
      // modify the value of cachedValues.
      localAdditions = new ArrayList<>();

      return commitBuilder.buildPartial();
    }

    private Future<Iterable<T>> getFuture() {
      return cachedValues != null ? null : reader.bagFuture(stateKey, stateFamily, elemCoder);
    }
  }

  private static class ConcatIterables<T> implements Iterable<T> {
    // List of component iterables.  Should only be appended to in order to support snapshot().
    List<Iterable<T>> iterables;

    public ConcatIterables() {
      this.iterables = new ArrayList<>();
    }

    public void extendWith(Iterable<T> iterable) {
      iterables.add(iterable);
    }

    @Override
    public Iterator<T> iterator() {
      return Iterators.concat(Iterables.transform(iterables, Iterable::iterator).iterator());
    }

    /**
     * Returns a view of the current state of this iterable. Remembers the current length of
     * iterables so that the returned value Will not change due to future extendWith() calls.
     */
    public Iterable<T> snapshot() {
      final int limit = iterables.size();
      final List<Iterable<T>> iterablesList = iterables;
      return () ->
          Iterators.concat(
              Iterators.transform(
                  Iterators.limit(iterablesList.iterator(), limit), Iterable::iterator));
    }
  }

  private static class WindmillWatermarkHold extends WindmillState implements WatermarkHoldState {
    // The encoded size of an Instant.
    private static final int ENCODED_SIZE = 8;

    private final TimestampCombiner timestampCombiner;
    private final StateNamespace namespace;
    private final StateTag<WatermarkHoldState> address;
    private final ByteString stateKey;
    private final String stateFamily;

    private boolean cleared = false;
    /**
     * If non-{@literal null}, the known current hold value, or absent if we know the there are no
     * output watermark holds. If {@literal null}, the current hold value could depend on holds in
     * Windmill we do not yet know.
     */
    private Optional<Instant> cachedValue = null;

    private Instant localAdditions = null;

    private WindmillWatermarkHold(
        StateNamespace namespace,
        StateTag<WatermarkHoldState> address,
        String stateFamily,
        TimestampCombiner timestampCombiner,
        boolean isNewKey) {
      this.namespace = namespace;
      this.address = address;
      this.stateKey = encodeKey(namespace, address);
      this.stateFamily = stateFamily;
      this.timestampCombiner = timestampCombiner;
      if (isNewKey) {
        cachedValue = Optional.<Instant>absent();
      }
    }

    @Override
    public void clear() {
      cleared = true;
      cachedValue = Optional.<Instant>absent();
      localAdditions = null;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public WindmillWatermarkHold readLater() {
      getFuture();
      return this;
    }

    @Override
    public Instant read() {
      try (Closeable scope = scopedReadState()) {
        Instant persistedHold = getFuture().get();
        if (persistedHold == null) {
          cachedValue = Optional.absent();
        } else {
          cachedValue = Optional.of(persistedHold);
        }
      } catch (InterruptedException | ExecutionException | IOException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new RuntimeException("Unable to read state", e);
      }

      if (localAdditions == null) {
        return cachedValue.orNull();
      } else if (!cachedValue.isPresent()) {
        return localAdditions;
      } else {
        return timestampCombiner.combine(localAdditions, cachedValue.get());
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void add(Instant outputTime) {
      localAdditions =
          (localAdditions == null)
              ? outputTime
              : timestampCombiner.combine(outputTime, localAdditions);
    }

    @Override
    public TimestampCombiner getTimestampCombiner() {
      return timestampCombiner;
    }

    @Override
    public Future<WorkItemCommitRequest> persist(final WindmillStateCache.ForKey cache) {

      Future<WorkItemCommitRequest> result;

      if (!cleared && localAdditions == null) {
        // No changes, so no need to update Windmill and no need to cache any value.
        return Futures.immediateFuture(WorkItemCommitRequest.newBuilder().buildPartial());
      }

      if (cleared && localAdditions == null) {
        // Just clearing the persisted state; blind delete
        WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
        commitBuilder
            .addWatermarkHoldsBuilder()
            .setTag(stateKey)
            .setStateFamily(stateFamily)
            .setReset(true);

        result = Futures.immediateFuture(commitBuilder.buildPartial());
      } else if (cleared && localAdditions != null) {
        // Since we cleared before adding, we can do a blind overwrite of persisted state
        WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
        commitBuilder
            .addWatermarkHoldsBuilder()
            .setTag(stateKey)
            .setStateFamily(stateFamily)
            .setReset(true)
            .addTimestamps(WindmillTimeUtils.harnessToWindmillTimestamp(localAdditions));

        cachedValue = Optional.of(localAdditions);

        result = Futures.immediateFuture(commitBuilder.buildPartial());
      } else if (!cleared && localAdditions != null) {
        // Otherwise, we need to combine the local additions with the already persisted data
        result = combineWithPersisted();
      } else {
        throw new IllegalStateException("Unreachable condition");
      }

      return Futures.lazyTransform(
          result,
          result1 -> {
            cleared = false;
            localAdditions = null;
            if (cachedValue != null) {
              cache.put(namespace, address, WindmillWatermarkHold.this, ENCODED_SIZE);
            }
            return result1;
          });
    }

    private Future<Instant> getFuture() {
      return cachedValue != null
          ? Futures.immediateFuture(cachedValue.orNull())
          : reader.watermarkFuture(stateKey, stateFamily);
    }

    /**
     * Combines local additions with persisted data and mutates the {@code commitBuilder} to write
     * the result.
     */
    private Future<WorkItemCommitRequest> combineWithPersisted() {
      boolean windmillCanCombine = false;

      // If the combined output time depends only on the window, then we are just blindly adding
      // the same value that may or may not already be present. This depends on the state only being
      // used for one window.
      windmillCanCombine |= timestampCombiner.dependsOnlyOnWindow();

      // If the combined output time depends only on the earliest input timestamp, then because
      // assignOutputTime is monotonic, the hold only depends on the earliest output timestamp
      // (which is the value submitted as a watermark hold). The only way holds for later inputs
      // can be redundant is if the are later (or equal) to the earliest. So taking the MIN
      // implicitly, as Windmill does, has the desired behavior.
      windmillCanCombine |= timestampCombiner.dependsOnlyOnEarliestTimestamp();

      if (windmillCanCombine) {
        // We do a blind write and let Windmill take the MIN
        WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
        commitBuilder
            .addWatermarkHoldsBuilder()
            .setTag(stateKey)
            .setStateFamily(stateFamily)
            .addTimestamps(WindmillTimeUtils.harnessToWindmillTimestamp(localAdditions));

        if (cachedValue != null) {
          cachedValue =
              Optional.of(
                  cachedValue.isPresent()
                      ? timestampCombiner.combine(cachedValue.get(), localAdditions)
                      : localAdditions);
        }

        return Futures.immediateFuture(commitBuilder.buildPartial());
      } else {
        // The non-fast path does a read-modify-write
        return Futures.lazyTransform(
            (cachedValue != null)
                ? Futures.immediateFuture(cachedValue.orNull())
                : reader.watermarkFuture(stateKey, stateFamily),
            priorHold -> {
              cachedValue =
                  Optional.of(
                      (priorHold != null)
                          ? timestampCombiner.combine(priorHold, localAdditions)
                          : localAdditions);
              WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
              commitBuilder
                  .addWatermarkHoldsBuilder()
                  .setTag(stateKey)
                  .setStateFamily(stateFamily)
                  .setReset(true)
                  .addTimestamps(WindmillTimeUtils.harnessToWindmillTimestamp(cachedValue.get()));

              return commitBuilder.buildPartial();
            });
      }
    }
  }

  private static class WindmillCombiningState<InputT, AccumT, OutputT> extends WindmillState
      implements CombiningState<InputT, AccumT, OutputT> {

    private final WindmillBag<AccumT> bag;
    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    /* We use a separate, in-memory AccumT rather than relying on the WindmillWatermarkBag's
     * localAdditions, because we want to combine multiple InputT's to a single AccumT
     * before adding it.
     */
    private AccumT localAdditionsAccum;
    private boolean hasLocalAdditions = false;

    private WindmillCombiningState(
        StateNamespace namespace,
        StateTag<CombiningState<InputT, AccumT, OutputT>> address,
        String stateFamily,
        Coder<AccumT> accumCoder,
        CombineFn<InputT, AccumT, OutputT> combineFn,
        WindmillStateCache.ForKey cache,
        boolean isNewKey) {
      StateTag<BagState<AccumT>> internalBagAddress = StateTags.convertToBagTagInternal(address);
      WindmillBag<AccumT> cachedBag =
          (WindmillBag<AccumT>) cache.get(namespace, internalBagAddress);
      this.bag =
          (cachedBag != null)
              ? cachedBag
              : new WindmillBag<>(namespace, internalBagAddress, stateFamily, accumCoder, isNewKey);
      this.combineFn = combineFn;
      this.localAdditionsAccum = combineFn.createAccumulator();
    }

    @Override
    void initializeForWorkItem(
        WindmillStateReader reader, Supplier<Closeable> scopedReadStateSupplier) {
      super.initializeForWorkItem(reader, scopedReadStateSupplier);
      this.bag.initializeForWorkItem(reader, scopedReadStateSupplier);
    }

    @Override
    void cleanupAfterWorkItem() {
      super.cleanupAfterWorkItem();
      bag.cleanupAfterWorkItem();
    }

    @Override
    public WindmillCombiningState<InputT, AccumT, OutputT> readLater() {
      bag.readLater();
      return this;
    }

    @Override
    public OutputT read() {
      return combineFn.extractOutput(getAccum());
    }

    @Override
    public void add(InputT input) {
      hasLocalAdditions = true;
      localAdditionsAccum = combineFn.addInput(localAdditionsAccum, input);
    }

    @Override
    public void clear() {
      bag.clear();
      localAdditionsAccum = combineFn.createAccumulator();
      hasLocalAdditions = false;
    }

    @Override
    public Future<WorkItemCommitRequest> persist(WindmillStateCache.ForKey cache)
        throws IOException {
      if (hasLocalAdditions) {
        if (COMPACT_NOW.get().get() || bag.valuesAreCached()) {
          // Implicitly clears the bag and combines local and persisted accumulators.
          localAdditionsAccum = getAccum();
        }
        bag.add(combineFn.compact(localAdditionsAccum));
        localAdditionsAccum = combineFn.createAccumulator();
        hasLocalAdditions = false;
      }

      return bag.persist(cache);
    }

    @Override
    public AccumT getAccum() {
      Iterable<AccumT> accums =
          Iterables.concat(bag.read(), Collections.singleton(localAdditionsAccum));

      // Compact things
      AccumT merged = combineFn.mergeAccumulators(accums);
      bag.clear();
      localAdditionsAccum = merged;
      hasLocalAdditions = true;
      return merged;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      final ReadableState<Boolean> bagIsEmpty = bag.isEmpty();
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          bagIsEmpty.readLater();
          return this;
        }

        @Override
        public Boolean read() {
          return !hasLocalAdditions && bagIsEmpty.read();
        }
      };
    }

    @Override
    public void addAccum(AccumT accum) {
      hasLocalAdditions = true;
      localAdditionsAccum = combineFn.mergeAccumulators(Arrays.asList(localAdditionsAccum, accum));
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(accumulators);
    }
  }

  @VisibleForTesting
  static final ThreadLocal<Supplier<Boolean>> COMPACT_NOW =
      ThreadLocal.withInitial(
          () ->
              new Supplier<Boolean>() {
                /* The rate at which, on average, this will return true. */
                static final double RATE = 0.002;
                Random random = new Random();
                long counter = nextSample();

                private long nextSample() {
                  // Use geometric distribution to find next true value.
                  // This lets us avoid invoking random.nextDouble() on every call.
                  return (long) Math.floor(Math.log(random.nextDouble()) / Math.log(1 - RATE));
                }

                @Override
                public Boolean get() {
                  counter--;
                  if (counter < 0) {
                    counter = nextSample();
                    return true;
                  } else {
                    return false;
                  }
                }
              });
}
