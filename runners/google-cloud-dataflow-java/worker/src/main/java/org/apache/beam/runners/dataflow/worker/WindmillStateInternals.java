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

import com.google.auto.value.AutoValue;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTable;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTag.StateBinder;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.SortedListEntry;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.SortedListRange;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagSortedListDeleteRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagSortedListInsertRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagSortedListUpdateRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.OrderedListState;
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
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BoundType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.RangeSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.TreeRangeSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Implementation of {@link StateInternals} using Windmill to manage the underlying data. */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
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
    private final WindmillStateCache.ForKeyAndFamily cache;
    private final boolean isSystemTable;
    boolean isNewKey;
    private final Supplier<Closeable> scopedReadStateSupplier;
    private final StateTable derivedStateTable;

    public CachingStateTable(
        @Nullable K key,
        String stateFamily,
        WindmillStateReader reader,
        WindmillStateCache.ForKeyAndFamily cache,
        boolean isSystemTable,
        boolean isNewKey,
        Supplier<Closeable> scopedReadStateSupplier,
        StateTable derivedStateTable) {
      this.key = key;
      this.stateFamily = stateFamily;
      this.reader = reader;
      this.cache = cache;
      this.isSystemTable = isSystemTable;
      this.isNewKey = isNewKey;
      this.scopedReadStateSupplier = scopedReadStateSupplier;
      this.derivedStateTable = derivedStateTable != null ? derivedStateTable : this;
    }

    @Override
    protected StateBinder binderForNamespace(
        final StateNamespace namespace, final StateContext<?> c) {
      // Look up state objects in the cache or create new ones if not found.  The state will
      // be added to the cache in persist().
      return new StateBinder() {
        @Override
        public <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder) {
          if (isSystemTable) {
            address = StateTags.makeSystemTagInternal(address);
          }
          WindmillBag<T> result = (WindmillBag<T>) cache.get(namespace, address);
          if (result == null) {
            result = new WindmillBag<>(namespace, address, stateFamily, elemCoder, isNewKey);
          }
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }

        @Override
        public <T> SetState<T> bindSet(StateTag<SetState<T>> spec, Coder<T> elemCoder) {
          WindmillSet<T> result =
              new WindmillSet<T>(namespace, spec, stateFamily, elemCoder, cache, isNewKey);
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }

        @Override
        public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(
            StateTag<MapState<KeyT, ValueT>> spec, Coder<KeyT> keyCoder, Coder<ValueT> valueCoder) {
          WindmillMap<KeyT, ValueT> result = (WindmillMap<KeyT, ValueT>) cache.get(namespace, spec);
          if (result == null) {
            result =
                new WindmillMap<KeyT, ValueT>(
                    namespace, spec, stateFamily, keyCoder, valueCoder, isNewKey);
          }
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }

        @Override
        public <T> OrderedListState<T> bindOrderedList(
            StateTag<OrderedListState<T>> spec, Coder<T> elemCoder) {
          if (isSystemTable) {
            spec = StateTags.makeSystemTagInternal(spec);
          }
          WindmillOrderedList<T> result = (WindmillOrderedList<T>) cache.get(namespace, spec);
          if (result == null) {
            result =
                new WindmillOrderedList<>(
                    derivedStateTable, namespace, spec, stateFamily, elemCoder, isNewKey);
          }
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }

        @Override
        public WatermarkHoldState bindWatermark(
            StateTag<WatermarkHoldState> address, TimestampCombiner timestampCombiner) {
          if (isSystemTable) {
            address = StateTags.makeSystemTagInternal(address);
          }
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
          if (isSystemTable) {
            address = StateTags.makeSystemTagInternal(address);
          }
          WindmillCombiningState<InputT, AccumT, OutputT> result =
              new WindmillCombiningState<>(
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
          if (isSystemTable) {
            address = StateTags.makeSystemTagInternal(address);
          }
          return bindCombiningValue(address, accumCoder, CombineFnUtil.bindContext(combineFn, c));
        }

        @Override
        public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
          if (isSystemTable) {
            address = StateTags.makeSystemTagInternal(address);
          }
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

  private WindmillStateCache.ForKeyAndFamily cache;
  Supplier<Closeable> scopedReadStateSupplier;
  private StateTable workItemState;
  private StateTable workItemDerivedState;

  public WindmillStateInternals(
      @Nullable K key,
      String stateFamily,
      WindmillStateReader reader,
      boolean isNewKey,
      WindmillStateCache.ForKeyAndFamily cache,
      Supplier<Closeable> scopedReadStateSupplier) {
    this.key = key;
    this.cache = cache;
    this.scopedReadStateSupplier = scopedReadStateSupplier;
    this.workItemDerivedState =
        new CachingStateTable<>(
            key, stateFamily, reader, cache, true, isNewKey, scopedReadStateSupplier, null);
    this.workItemState =
        new CachingStateTable<>(
            key,
            stateFamily,
            reader,
            cache,
            false,
            isNewKey,
            scopedReadStateSupplier,
            workItemDerivedState);
  }

  private void persist(List<Future<WorkItemCommitRequest>> commitsToMerge, StateTable stateTable) {
    for (State location : stateTable.values()) {
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
    for (State location : stateTable.values()) {
      ((WindmillState) location).cleanupAfterWorkItem();
    }

    // Clear out the map of already retrieved state instances.
    stateTable.clear();
  }

  public void persist(final Windmill.WorkItemCommitRequest.Builder commitBuilder) {
    List<Future<WorkItemCommitRequest>> commitsToMerge = new ArrayList<>();

    // Call persist on each first, which may schedule some futures for reading.
    persist(commitsToMerge, workItemState);
    persist(commitsToMerge, workItemDerivedState);

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

    cache.persist();
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
    abstract Future<WorkItemCommitRequest> persist(WindmillStateCache.ForKeyAndFamily cache)
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
    public final Future<WorkItemCommitRequest> persist(WindmillStateCache.ForKeyAndFamily cache)
        throws IOException {
      return Futures.immediateFuture(persistDirectly(cache));
    }

    /**
     * Returns a {@link WorkItemCommitRequest} that can be used to persist this state to Windmill.
     */
    protected abstract WorkItemCommitRequest persistDirectly(
        WindmillStateCache.ForKeyAndFamily cache) throws IOException;
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
    protected WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKeyAndFamily cache)
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

  // Coder for closed-open ranges.
  private static class RangeCoder<T extends Comparable> extends StructuredCoder<Range<T>> {
    private Coder<T> boundCoder;

    RangeCoder(Coder<T> boundCoder) {
      this.boundCoder = NullableCoder.of(boundCoder);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Lists.newArrayList(boundCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      boundCoder.verifyDeterministic();
      ;
    }

    @Override
    public void encode(Range<T> value, OutputStream outStream) throws CoderException, IOException {
      Preconditions.checkState(
          value.lowerBoundType().equals(BoundType.CLOSED), "unexpected range " + value);
      Preconditions.checkState(
          value.upperBoundType().equals(BoundType.OPEN), "unexpected range " + value);
      boundCoder.encode(value.hasLowerBound() ? value.lowerEndpoint() : null, outStream);
      boundCoder.encode(value.hasUpperBound() ? value.upperEndpoint() : null, outStream);
    }

    @Override
    public Range<T> decode(InputStream inStream) throws CoderException, IOException {
      @Nullable T lower = boundCoder.decode(inStream);
      @Nullable T upper = boundCoder.decode(inStream);
      if (lower == null) {
        return upper != null ? Range.lessThan(upper) : Range.all();
      } else if (upper == null) {
        return Range.atLeast(lower);
      } else {
        return Range.closedOpen(lower, upper);
      }
    }
  }

  private static class RangeSetCoder<T extends Comparable> extends CustomCoder<RangeSet<T>> {
    private SetCoder<Range<T>> rangesCoder;

    RangeSetCoder(Coder<T> boundCoder) {
      this.rangesCoder = SetCoder.of(new RangeCoder<>(boundCoder));
    }

    @Override
    public void encode(RangeSet<T> value, OutputStream outStream) throws IOException {
      rangesCoder.encode(value.asRanges(), outStream);
    }

    @Override
    public RangeSet<T> decode(InputStream inStream) throws CoderException, IOException {
      return TreeRangeSet.create(rangesCoder.decode(inStream));
    }
  }

  /**
   * Tracker for the ids used in an ordered list.
   *
   * <p>Windmill accepts an int64 id for each timestamped-element in the list. Unique elements are
   * identified by the pair of timestamp and id. This means that tow unique elements e1, e2 must
   * have different (ts1, id1), (ts2, id2) pairs. To accomplish this we bucket time into five-minute
   * buckets, and store a free list of ids available for each bucket.
   *
   * <p>When a timestamp range is deleted, we remove id tracking for elements in that range. In
   * order to handle the case where a range is deleted piecemeal, we track sub-range deletions for
   * each range. For example:
   *
   * <p>12:00 - 12:05 ids 12:05 - 12:10 ids
   *
   * <p>delete 12:00-12:06
   *
   * <p>12:00 - 12:05 *removed* 12:05 - 12:10 ids subranges deleted 12:05-12:06
   *
   * <p>delete 12:06 - 12:07
   *
   * <p>12:05 - 12:10 ids subranges deleted 12:05-12:07
   *
   * <p>delete 12:07 - 12:10
   *
   * <p>12:05 - 12:10 *removed*
   */
  static final class IdTracker {
    static final String IDS_AVAILABLE_STR = "IdsAvailable";
    static final String DELETIONS_STR = "Deletions";

    static final long MIN_ID = Long.MIN_VALUE;
    static final long MAX_ID = Long.MAX_VALUE;

    // We track ids on five-minute boundaries.
    private static final Duration RESOLUTION = Duration.standardMinutes(5);
    static final MapCoder<Range<Instant>, RangeSet<Long>> IDS_AVAILABLE_CODER =
        MapCoder.of(new RangeCoder<>(InstantCoder.of()), new RangeSetCoder<>(VarLongCoder.of()));
    static final MapCoder<Range<Instant>, RangeSet<Instant>> SUBRANGE_DELETIONS_CODER =
        MapCoder.of(new RangeCoder<>(InstantCoder.of()), new RangeSetCoder<>(InstantCoder.of()));
    private final StateTag<ValueState<Map<Range<Instant>, RangeSet<Long>>>> idsAvailableTag;
    // A map from five-minute ranges to the set of ids available in that interval.
    final ValueState<Map<Range<Instant>, RangeSet<Long>>> idsAvailableValue;
    private final StateTag<ValueState<Map<Range<Instant>, RangeSet<Instant>>>> subRangeDeletionsTag;
    // If a timestamp-range in the map has been partially cleared, the cleared intervals are stored
    // here.
    final ValueState<Map<Range<Instant>, RangeSet<Instant>>> subRangeDeletionsValue;

    IdTracker(
        StateTable stateTable,
        StateNamespace namespace,
        StateTag<?> spec,
        String stateFamily,
        boolean complete) {
      this.idsAvailableTag =
          StateTags.makeSystemTagInternal(
              StateTags.value(spec.getId() + IDS_AVAILABLE_STR, IDS_AVAILABLE_CODER));
      this.idsAvailableValue =
          stateTable.get(namespace, idsAvailableTag, StateContexts.nullContext());
      this.subRangeDeletionsTag =
          StateTags.makeSystemTagInternal(
              StateTags.value(spec.getId() + DELETIONS_STR, SUBRANGE_DELETIONS_CODER));
      this.subRangeDeletionsValue =
          stateTable.get(namespace, subRangeDeletionsTag, StateContexts.nullContext());
    }

    static <ValueT extends Comparable<? super ValueT>>
        Map<Range<Instant>, RangeSet<ValueT>> newSortedRangeMap(Class<ValueT> valueClass) {
      return Maps.newTreeMap(
          Comparator.<Range<Instant>, Instant>comparing(Range::lowerEndpoint)
              .thenComparing(Range::upperEndpoint));
    }

    private Range<Instant> getTrackedRange(Instant ts) {
      Instant snapped =
          new Instant(ts.getMillis() - ts.plus(RESOLUTION).getMillis() % RESOLUTION.getMillis());
      return Range.closedOpen(snapped, snapped.plus(RESOLUTION));
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    void readLater() {
      idsAvailableValue.readLater();
      subRangeDeletionsValue.readLater();
    }

    Map<Range<Instant>, RangeSet<Long>> readIdsAvailable() {
      Map<Range<Instant>, RangeSet<Long>> idsAvailable = idsAvailableValue.read();
      return idsAvailable != null ? idsAvailable : newSortedRangeMap(Long.class);
    }

    Map<Range<Instant>, RangeSet<Instant>> readSubRangeDeletions() {
      Map<Range<Instant>, RangeSet<Instant>> subRangeDeletions = subRangeDeletionsValue.read();
      return subRangeDeletions != null ? subRangeDeletions : newSortedRangeMap(Instant.class);
    }

    void clear() throws ExecutionException, InterruptedException {
      idsAvailableValue.clear();
      subRangeDeletionsValue.clear();
    }

    <T> void add(
        SortedSet<TimestampedValueWithId<T>> elements, BiConsumer<TimestampedValue<T>, Long> output)
        throws ExecutionException, InterruptedException {
      Range<Long> currentIdRange = null;
      long currentId = 0;

      Range<Instant> currentTsRange = null;
      RangeSet<Instant> currentTsRangeDeletions = null;

      Map<Range<Instant>, RangeSet<Long>> idsAvailable = readIdsAvailable();
      Map<Range<Instant>, RangeSet<Instant>> subRangeDeletions = readSubRangeDeletions();

      RangeSet<Long> availableIdsForTsRange = null;
      Iterator<Range<Long>> idRangeIter = null;
      RangeSet<Long> idsUsed = TreeRangeSet.create();
      for (TimestampedValueWithId<T> pendingAdd : elements) {
        // Since elements are in increasing ts order, often we'll be able to reuse the previous
        // iteration's range.
        if (currentTsRange == null
            || !currentTsRange.contains(pendingAdd.getValue().getTimestamp())) {
          if (availableIdsForTsRange != null) {
            // We're moving onto a new ts range. Remove all used ids
            availableIdsForTsRange.removeAll(idsUsed);
            idsUsed = TreeRangeSet.create();
          }

          // Lookup the range for the current timestamp.
          currentTsRange = getTrackedRange(pendingAdd.getValue().getTimestamp());
          // Lookup available ids for this timestamp range. If nothing there, we default to all ids
          // available.
          availableIdsForTsRange =
              idsAvailable.computeIfAbsent(
                  currentTsRange,
                  r -> TreeRangeSet.create(ImmutableList.of(Range.closedOpen(MIN_ID, MAX_ID))));
          idRangeIter = availableIdsForTsRange.asRanges().iterator();
          currentIdRange = null;
          currentTsRangeDeletions = subRangeDeletions.get(currentTsRange);
        }

        if (currentIdRange == null || currentId >= currentIdRange.upperEndpoint()) {
          // Move to the next range of free ids, and start assigning ranges from there.
          currentIdRange = idRangeIter.next();
          currentId = currentIdRange.lowerEndpoint();
        }

        if (currentTsRangeDeletions != null) {
          currentTsRangeDeletions.remove(
              Range.closedOpen(
                  pendingAdd.getValue().getTimestamp(),
                  pendingAdd.getValue().getTimestamp().plus(1)));
        }
        idsUsed.add(Range.closedOpen(currentId, currentId + 1));
        output.accept(pendingAdd.getValue(), currentId++);
      }
      if (availableIdsForTsRange != null) {
        availableIdsForTsRange.removeAll(idsUsed);
      }
      writeValues(idsAvailable, subRangeDeletions);
    }

    // Remove a timestamp range. Returns ids freed up.
    void remove(Range<Instant> tsRange) throws ExecutionException, InterruptedException {
      Map<Range<Instant>, RangeSet<Long>> idsAvailable = readIdsAvailable();
      Map<Range<Instant>, RangeSet<Instant>> subRangeDeletions = readSubRangeDeletions();

      for (Range<Instant> current = getTrackedRange(tsRange.lowerEndpoint());
          current.lowerEndpoint().isBefore(tsRange.upperEndpoint());
          current = getTrackedRange(current.lowerEndpoint().plus(RESOLUTION))) {
        // TODO(reuvenlax): shouldn't need to iterate over all ranges.
        boolean rangeCleared;
        if (!tsRange.encloses(current)) {
          // This can happen if the beginning or the end of tsRange doesn't fall on a RESOLUTION
          // boundary. Since we
          // are deleting a portion of a tracked range, track what we are deleting.
          RangeSet<Instant> rangeDeletions =
              subRangeDeletions.computeIfAbsent(current, r -> TreeRangeSet.create());
          rangeDeletions.add(tsRange.intersection(current));
          // If we ended up deleting the whole range, than we can simply remove it from the tracking
          // map.
          rangeCleared = rangeDeletions.encloses(current);
        } else {
          rangeCleared = true;
        }
        if (rangeCleared) {
          // Remove the range from both maps.
          idsAvailable.remove(current);
          subRangeDeletions.remove(current);
        }
      }
      writeValues(idsAvailable, subRangeDeletions);
    }

    private void writeValues(
        Map<Range<Instant>, RangeSet<Long>> idsAvailable,
        Map<Range<Instant>, RangeSet<Instant>> subRangeDeletions) {
      if (idsAvailable.isEmpty()) {
        idsAvailable.clear();
      } else {
        idsAvailableValue.write(idsAvailable);
      }
      if (subRangeDeletions.isEmpty()) {
        subRangeDeletionsValue.clear();
      } else {
        subRangeDeletionsValue.write(subRangeDeletions);
      }
    }
  }

  @AutoValue
  abstract static class TimestampedValueWithId<T> {
    private static final Comparator<TimestampedValueWithId<?>> COMPARATOR =
        Comparator.<TimestampedValueWithId<?>, Instant>comparing(v -> v.getValue().getTimestamp())
            .thenComparingLong(TimestampedValueWithId::getId);

    abstract TimestampedValue<T> getValue();

    abstract long getId();

    static <T> TimestampedValueWithId<T> of(TimestampedValue<T> value, long id) {
      return new AutoValue_WindmillStateInternals_TimestampedValueWithId<>(value, id);
    }

    static <T> TimestampedValueWithId<T> bound(Instant ts) {
      return of(TimestampedValue.of(null, ts), Long.MIN_VALUE);
    }
  }

  static class WindmillOrderedList<T> extends SimpleWindmillState implements OrderedListState<T> {
    private final StateNamespace namespace;
    private final StateTag<OrderedListState<T>> spec;
    private final ByteString stateKey;
    private final String stateFamily;
    private final Coder<T> elemCoder;
    private boolean complete;
    private boolean cleared = false;
    // We need to sort based on timestamp, but we need objects with the same timestamp to be treated
    // as unique. We can't use a MultiSet as we can't construct a comparator that uniquely
    // identifies objects,
    // so we construct a unique in-memory long ids for each element.
    private SortedSet<TimestampedValueWithId<T>> pendingAdds =
        Sets.newTreeSet(TimestampedValueWithId.COMPARATOR);

    private RangeSet<Instant> pendingDeletes = TreeRangeSet.create();
    private IdTracker idTracker;

    // The default proto values for SortedListRange correspond to the minimum and maximum
    // timestamps.
    static final long MIN_TS_MICROS = SortedListRange.getDefaultInstance().getStart();
    static final long MAX_TS_MICROS = SortedListRange.getDefaultInstance().getLimit();

    private WindmillOrderedList(
        StateTable derivedStateTable,
        StateNamespace namespace,
        StateTag<OrderedListState<T>> spec,
        String stateFamily,
        Coder<T> elemCoder,
        boolean isNewKey) {
      this.namespace = namespace;
      this.spec = spec;

      this.stateKey = encodeKey(namespace, spec);
      this.stateFamily = stateFamily;
      this.elemCoder = elemCoder;
      this.complete = isNewKey;
      this.idTracker = new IdTracker(derivedStateTable, namespace, spec, stateFamily, complete);
    }

    @Override
    public Iterable<TimestampedValue<T>> read() {
      return readRange(null, null);
    }

    private SortedSet<TimestampedValueWithId<T>> getPendingAddRange(
        @Nullable Instant minTimestamp, @Nullable Instant limitTimestamp) {
      SortedSet<TimestampedValueWithId<T>> pendingInRange = pendingAdds;
      if (minTimestamp != null && limitTimestamp != null) {
        pendingInRange =
            pendingInRange.subSet(
                TimestampedValueWithId.bound(minTimestamp),
                TimestampedValueWithId.bound(limitTimestamp));
      } else if (minTimestamp == null && limitTimestamp != null) {
        pendingInRange = pendingInRange.headSet(TimestampedValueWithId.bound(limitTimestamp));
      } else if (limitTimestamp == null && minTimestamp != null) {
        pendingInRange = pendingInRange.tailSet(TimestampedValueWithId.bound(minTimestamp));
      }
      return pendingInRange;
    }

    @Override
    public Iterable<TimestampedValue<T>> readRange(
        @Nullable Instant minTimestamp, @Nullable Instant limitTimestamp) {
      idTracker.readLater();

      final Future<Iterable<TimestampedValue<T>>> future = getFuture(minTimestamp, limitTimestamp);
      try (Closeable scope = scopedReadState()) {
        SortedSet<TimestampedValueWithId<T>> pendingInRange =
            getPendingAddRange(minTimestamp, limitTimestamp);

        // Transform the return iterator so it has the same type as pendingAdds. We need to ensure
        // that the ids don't overlap with any in pendingAdds, so begin with pendingAdds.size().
        Iterable<TimestampedValueWithId<T>> data =
            new Iterable<TimestampedValueWithId<T>>() {
              private Iterable<TimestampedValue<T>> iterable = future.get();

              @Override
              public Iterator<TimestampedValueWithId<T>> iterator() {
                return new Iterator<TimestampedValueWithId<T>>() {
                  private Iterator<TimestampedValue<T>> iter = iterable.iterator();
                  private long currentId = pendingAdds.size();

                  @Override
                  public boolean hasNext() {
                    return iter.hasNext();
                  }

                  @Override
                  public TimestampedValueWithId<T> next() {
                    return TimestampedValueWithId.of(iter.next(), currentId++);
                  }
                };
              }
            };

        Iterable<TimestampedValueWithId<T>> includingAdds =
            Iterables.mergeSorted(
                ImmutableList.of(data, pendingInRange), TimestampedValueWithId.COMPARATOR);
        Iterable<TimestampedValue<T>> fullIterable =
            Iterables.filter(
                Iterables.transform(includingAdds, TimestampedValueWithId::getValue),
                tv -> !pendingDeletes.contains(tv.getTimestamp()));
        // TODO(reuvenlax): If we have a known bounded amount of data, cache known ranges.
        return fullIterable;
      } catch (InterruptedException | ExecutionException | IOException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new RuntimeException("Unable to read state", e);
      }
    }

    @Override
    public void clear() {
      cleared = true;
      complete = true;
      pendingAdds.clear();
      pendingDeletes.clear();
      try {
        idTracker.clear();
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
      getPendingAddRange(minTimestamp, limitTimestamp).clear();
      pendingDeletes.add(Range.closedOpen(minTimestamp, limitTimestamp));
    }

    @Override
    public void add(TimestampedValue<T> value) {
      // We use the current size of the container as the in-memory id. This works because
      // pendingAdds is completely
      // cleared when it is processed (otherwise we could end up with duplicate elements in the same
      // container). These
      // are not the ids that will be sent to windmill.
      pendingAdds.add(TimestampedValueWithId.of(value, pendingAdds.size()));
      // Leave pendingDeletes alone. Since we can have multiple values with the same timestamp, we
      // may still need
      // overlapping deletes to remove previous entries at this timestamp.
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          WindmillOrderedList.this.readLater();
          return this;
        }

        @Override
        public Boolean read() {
          return Iterables.isEmpty(WindmillOrderedList.this.read());
        }
      };
    }

    @Override
    public OrderedListState<T> readLater() {
      return readRangeLater(null, null);
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public OrderedListState<T> readRangeLater(
        @Nullable Instant minTimestamp, @Nullable Instant limitTimestamp) {
      idTracker.readLater();
      getFuture(minTimestamp, limitTimestamp);
      return this;
    }

    @Override
    public WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKeyAndFamily cache)
        throws IOException {
      WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
      TagSortedListUpdateRequest.Builder updatesBuilder =
          commitBuilder
              .addSortedListUpdatesBuilder()
              .setStateFamily(cache.getStateFamily())
              .setTag(stateKey);
      try {
        if (cleared) {
          // Default range.
          updatesBuilder.addDeletesBuilder().build();
          cleared = false;
        }

        if (!pendingAdds.isEmpty()) {
          // TODO(reuvenlax): Once we start caching data, we should remove this line. We have it
          // here now
          // because once we persist
          // added data we forget about it from the cache, so the object is no longer complete.
          complete = false;

          TagSortedListInsertRequest.Builder insertBuilder = updatesBuilder.addInsertsBuilder();
          idTracker.add(
              pendingAdds,
              (elem, id) -> {
                try {
                  ByteString.Output elementStream = ByteString.newOutput();
                  elemCoder.encode(elem.getValue(), elementStream, Context.OUTER);
                  insertBuilder.addEntries(
                      SortedListEntry.newBuilder()
                          .setValue(elementStream.toByteString())
                          .setSortKey(
                              WindmillTimeUtils.harnessToWindmillTimestamp(elem.getTimestamp()))
                          .setId(id));
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
          pendingAdds.clear();
          insertBuilder.build();
        }

        if (!pendingDeletes.isEmpty()) {
          for (Range<Instant> range : pendingDeletes.asRanges()) {
            TagSortedListDeleteRequest.Builder deletesBuilder = updatesBuilder.addDeletesBuilder();
            deletesBuilder.setRange(
                SortedListRange.newBuilder()
                    .setStart(WindmillTimeUtils.harnessToWindmillTimestamp(range.lowerEndpoint()))
                    .setLimit(WindmillTimeUtils.harnessToWindmillTimestamp(range.upperEndpoint())));
            deletesBuilder.build();
            idTracker.remove(range);
          }
          pendingDeletes.clear();
        }
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      return commitBuilder.buildPartial();
    }

    private Future<Iterable<TimestampedValue<T>>> getFuture(
        @Nullable Instant minTimestamp, @Nullable Instant limitTimestamp) {
      long startSortKey =
          minTimestamp != null
              ? WindmillTimeUtils.harnessToWindmillTimestamp(minTimestamp)
              : MIN_TS_MICROS;
      long limitSortKey =
          limitTimestamp != null
              ? WindmillTimeUtils.harnessToWindmillTimestamp(limitTimestamp)
              : MAX_TS_MICROS;

      if (complete) {
        // Right now we don't cache any data, so complete means an empty list.
        // TODO(reuvenlax): change this once we start caching data.
        return Futures.immediateFuture(Collections.emptyList());
      }
      return reader.orderedListFuture(
          Range.closedOpen(startSortKey, limitSortKey), stateKey, stateFamily, elemCoder);
    }
  }

  static class WindmillSet<K> extends SimpleWindmillState implements SetState<K> {
    WindmillMap<K, Boolean> windmillMap;

    WindmillSet(
        StateNamespace namespace,
        StateTag<SetState<K>> address,
        String stateFamily,
        Coder<K> keyCoder,
        WindmillStateCache.ForKey cache,
        boolean isNewKey) {
      StateTag<MapState<K, Boolean>> internalMapAddress =
          StateTags.convertToMapTagInternal(address);
      WindmillMap<K, Boolean> cachedMap =
          (WindmillMap<K, Boolean>) cache.get(namespace, internalMapAddress);
      this.windmillMap =
          (cachedMap != null)
              ? cachedMap
              : new WindmillMap<>(
                  namespace,
                  internalMapAddress,
                  stateFamily,
                  keyCoder,
                  BooleanCoder.of(),
                  isNewKey);
    }

    @Override
    protected WorkItemCommitRequest persistDirectly(ForKey cache) throws IOException {
      return windmillMap.persistDirectly(cache);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<
            @UnknownKeyFor @NonNull @Initialized Boolean>
        contains(K k) {
      return windmillMap.getOrDefault(k, false);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<
            @UnknownKeyFor @NonNull @Initialized Boolean>
        addIfAbsent(K k) {
      return new ReadableState<Boolean>() {
        ReadableState<Boolean> putState = windmillMap.putIfAbsent(k, true);

        @Override
        public @Nullable Boolean read() {
          Boolean result = putState.read();
          return (result != null) ? result : false;
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized ReadableState<Boolean> readLater() {
          putState = putState.readLater();
          return this;
        }
      };
    }

    @Override
    public void remove(K k) {
      windmillMap.remove(k);
    }

    @Override
    public void add(K value) {
      windmillMap.put(value, true);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<
            @UnknownKeyFor @NonNull @Initialized Boolean>
        isEmpty() {
      return windmillMap.isEmpty();
    }

    @Override
    public @Nullable Iterable<K> read() {
      return windmillMap.keys().read();
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized SetState<K> readLater() {
      windmillMap.keys().readLater();
      return this;
    }

    @Override
    public void clear() {
      windmillMap.clear();
    }

    @Override
    void initializeForWorkItem(
        WindmillStateReader reader, Supplier<Closeable> scopedReadStateSupplier) {
      windmillMap.initializeForWorkItem(reader, scopedReadStateSupplier);
    }

    @Override
    void cleanupAfterWorkItem() {
      windmillMap.cleanupAfterWorkItem();
    }
  }

  static class WindmillMap<K, V> extends SimpleWindmillState implements MapState<K, V> {
    private final StateNamespace namespace;
    private final StateTag<MapState<K, V>> address;
    private final ByteString stateKeyPrefix;
    private final String stateFamily;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private boolean complete;

    //  TODO(reuvenlax): Should we evict items from the cache? We would have to make sure
    // that anything in the cache that is not committed is not evicted. negativeCache could be
    // evicted whenever we want.
    private Map<K, V> cachedValues = Maps.newHashMap();
    private Set<K> negativeCache = Sets.newHashSet();
    private boolean cleared = false;

    private Set<K> localAdditions = Sets.newHashSet();
    private Set<K> localRemovals = Sets.newHashSet();

    WindmillMap(
        StateNamespace namespace,
        StateTag<MapState<K, V>> address,
        String stateFamily,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        boolean isNewKey) {
      this.namespace = namespace;
      this.address = address;
      this.stateKeyPrefix = encodeKey(namespace, address);
      this.stateFamily = stateFamily;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.complete = isNewKey;
    }

    private K userKeyFromProtoKey(ByteString tag) throws IOException {
      Preconditions.checkState(tag.startsWith(stateKeyPrefix));
      ByteString keyBytes = tag.substring(stateKeyPrefix.size());
      return keyCoder.decode(keyBytes.newInput(), Context.OUTER);
    }

    private ByteString protoKeyFromUserKey(K key) throws IOException {
      ByteString.Output keyStream = ByteString.newOutput();
      stateKeyPrefix.writeTo(keyStream);
      keyCoder.encode(key, keyStream, Context.OUTER);
      return keyStream.toByteString();
    }

    @Override
    protected WorkItemCommitRequest persistDirectly(ForKey cache) throws IOException {
      if (!cleared && localAdditions.isEmpty() && localRemovals.isEmpty()) {
        // No changes, so return directly.
        return WorkItemCommitRequest.newBuilder().buildPartial();
      }

      WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();

      if (cleared) {
        commitBuilder
            .addTagValuePrefixDeletesBuilder()
            .setStateFamily(stateFamily)
            .setTagPrefix(stateKeyPrefix);
      }
      cleared = false;

      for (K key : localAdditions) {
        ByteString keyBytes = protoKeyFromUserKey(key);
        ByteString.Output valueStream = ByteString.newOutput();
        valueCoder.encode(cachedValues.get(key), valueStream, Context.OUTER);
        ByteString valueBytes = valueStream.toByteString();

        commitBuilder
            .addValueUpdatesBuilder()
            .setTag(keyBytes)
            .setStateFamily(stateFamily)
            .getValueBuilder()
            .setData(valueBytes)
            .setTimestamp(Long.MAX_VALUE);
      }
      localAdditions.clear();

      for (K key : localRemovals) {
        ByteString.Output keyStream = ByteString.newOutput();
        stateKeyPrefix.writeTo(keyStream);
        keyCoder.encode(key, keyStream, Context.OUTER);
        ByteString keyBytes = keyStream.toByteString();
        // Leaving data blank means that we delete the tag.
        commitBuilder
            .addValueUpdatesBuilder()
            .setTag(keyBytes)
            .setStateFamily(stateFamily)
            .getValueBuilder()
            .setTimestamp(Long.MAX_VALUE);

        V cachedValue = cachedValues.remove(key);
        if (cachedValue != null) {
          ByteString.Output valueStream = ByteString.newOutput();
          valueCoder.encode(cachedValues.get(key), valueStream, Context.OUTER);
        }
      }
      negativeCache.addAll(localRemovals);
      localRemovals.clear();

      // TODO(reuvenlax): We should store in the cache parameter, as that would enable caching the
      // map
      // between work items, reducing fetches to Windmill. To do so, we need keep track of the
      // encoded size
      // of the map, and to do so efficiently (i.e. without iterating over the entire map on every
      // persist)
      // we need to track the sizes of each map entry.
      cache.put(namespace, address, this, 1);
      return commitBuilder.buildPartial();
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<V> get(K key) {
      return getOrDefault(key, null);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<V> getOrDefault(
        K key, @Nullable V defaultValue) {
      return new ReadableState<V>() {
        @Override
        public @Nullable V read() {
          Future<V> persistedData = getFutureForKey(key);
          try (Closeable scope = scopedReadState()) {
            if (localRemovals.contains(key) || negativeCache.contains(key)) {
              return null;
            }
            @Nullable V cachedValue = cachedValues.get(key);
            if (cachedValue != null || complete) {
              return cachedValue;
            }

            V persistedValue = persistedData.get();
            if (persistedValue == null) {
              negativeCache.add(key);
              return defaultValue;
            }
            // TODO: Don't do this if it was already in cache.
            cachedValues.put(key, persistedValue);
            return persistedValue;
          } catch (InterruptedException | ExecutionException | IOException e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Unable to read state", e);
          }
        }

        @Override
        @SuppressWarnings("FutureReturnValueIgnored")
        public @UnknownKeyFor @NonNull @Initialized ReadableState<V> readLater() {
          WindmillMap.this.getFutureForKey(key);
          return this;
        }
      };
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<
            @UnknownKeyFor @NonNull @Initialized Iterable<K>>
        keys() {
      ReadableState<Iterable<Map.Entry<K, V>>> entries = entries();
      return new ReadableState<Iterable<K>>() {
        @Override
        public @Nullable Iterable<K> read() {
          return Iterables.transform(entries.read(), e -> e.getKey());
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized ReadableState<Iterable<K>> readLater() {
          entries.readLater();
          return this;
        }
      };
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<
            @UnknownKeyFor @NonNull @Initialized Iterable<V>>
        values() {
      ReadableState<Iterable<Map.Entry<K, V>>> entries = entries();
      return new ReadableState<Iterable<V>>() {
        @Override
        public @Nullable Iterable<V> read() {
          return Iterables.transform(entries.read(), e -> e.getValue());
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized ReadableState<Iterable<V>> readLater() {
          entries.readLater();
          return this;
        }
      };
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<
            @UnknownKeyFor @NonNull @Initialized Iterable<
                @UnknownKeyFor @NonNull @Initialized Entry<K, V>>>
        entries() {
      return new ReadableState<Iterable<Entry<K, V>>>() {
        @Override
        public Iterable<Entry<K, V>> read() {
          if (complete) {
            return Iterables.unmodifiableIterable(cachedValues.entrySet());
          }
          Future<Iterable<Map.Entry<ByteString, V>>> persistedData = getFuture();
          try (Closeable scope = scopedReadState()) {
            Iterable<Map.Entry<ByteString, V>> data = persistedData.get();
            Iterable<Map.Entry<K, V>> transformedData =
                Iterables.<Map.Entry<ByteString, V>, Map.Entry<K, V>>transform(
                    data,
                    entry -> {
                      try {
                        return new AbstractMap.SimpleEntry<>(
                            userKeyFromProtoKey(entry.getKey()), entry.getValue());
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    });

            if (data instanceof Weighted) {
              // This is a known amount of data. Cache it all.
              transformedData.forEach(
                  e -> {
                    // The cached data overrides what is read from state, so call putIfAbsent.
                    cachedValues.putIfAbsent(e.getKey(), e.getValue());
                  });
              complete = true;
              return Iterables.unmodifiableIterable(cachedValues.entrySet());
            } else {
              // This means that the result might be too large to cache, so don't add it to the
              // local cache. Instead merge the iterables, giving priority to any local additions
              // (represented in cachedValued and localRemovals) that may not have been committed
              // yet.
              return Iterables.unmodifiableIterable(
                  Iterables.concat(
                      cachedValues.entrySet(),
                      Iterables.filter(
                          transformedData,
                          e ->
                              !cachedValues.containsKey(e.getKey())
                                  && !localRemovals.contains(e.getKey()))));
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
        public @UnknownKeyFor @NonNull @Initialized ReadableState<Iterable<Entry<K, V>>>
            readLater() {
          WindmillMap.this.getFuture();
          return this;
        }
      };
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        // TODO(reuvenlax): Can we find a more efficient way of implementing isEmpty than reading
        // the entire map?
        ReadableState<Iterable<K>> keys = WindmillMap.this.keys();

        @Override
        public @Nullable Boolean read() {
          return Iterables.isEmpty(keys.read());
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized ReadableState<Boolean> readLater() {
          keys.readLater();
          return this;
        }
      };
    }

    @Override
    public void put(K key, V value) {
      V oldValue = cachedValues.put(key, value);
      if (valueCoder.consistentWithEquals() && value.equals(oldValue)) {
        return;
      }
      localAdditions.add(key);
      localRemovals.remove(key);
      negativeCache.remove(key);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<V> computeIfAbsent(
        K key, Function<? super K, ? extends V> mappingFunction) {
      return new ReadableState<V>() {
        @Override
        public @Nullable V read() {
          Future<V> persistedData = getFutureForKey(key);
          try (Closeable scope = scopedReadState()) {
            if (localRemovals.contains(key) || negativeCache.contains(key)) {
              return null;
            }
            @Nullable V cachedValue = cachedValues.get(key);
            if (cachedValue != null || complete) {
              return cachedValue;
            }

            V persistedValue = persistedData.get();
            if (persistedValue == null) {
              // This is a new value. Add it to the map and return null.
              put(key, mappingFunction.apply(key));
              return null;
            }
            // TODO: Don't do this if it was already in cache.
            cachedValues.put(key, persistedValue);
            return persistedValue;
          } catch (InterruptedException | ExecutionException | IOException e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Unable to read state", e);
          }
        }

        @Override
        @SuppressWarnings("FutureReturnValueIgnored")
        public @UnknownKeyFor @NonNull @Initialized ReadableState<V> readLater() {
          WindmillMap.this.getFutureForKey(key);
          return this;
        }
      };
    }

    @Override
    public void remove(K key) {
      if (localRemovals.add(key)) {
        cachedValues.remove(key);
        localAdditions.remove(key);
      }
    }

    @Override
    public void clear() {
      cachedValues.clear();
      localAdditions.clear();
      localRemovals.clear();
      negativeCache.clear();
      cleared = true;
      complete = true;
    }

    private Future<V> getFutureForKey(K key) {
      try {
        ByteString.Output keyStream = ByteString.newOutput();
        stateKeyPrefix.writeTo(keyStream);
        keyCoder.encode(key, keyStream, Context.OUTER);
        return reader.valueFuture(keyStream.toByteString(), stateFamily, valueCoder);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private Future<Iterable<Map.Entry<ByteString, V>>> getFuture() {
      if (complete) {
        // The caller will merge in local cached values.
        return Futures.immediateFuture(Collections.emptyList());
      } else {
        return reader.valuePrefixFuture(stateKeyPrefix, stateFamily, valueCoder);
      }
    }
  };

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
    public WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKeyAndFamily cache)
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
    public Future<WorkItemCommitRequest> persist(final WindmillStateCache.ForKeyAndFamily cache) {

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
        WindmillStateCache.ForKeyAndFamily cache,
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
    public Future<WorkItemCommitRequest> persist(WindmillStateCache.ForKeyAndFamily cache)
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
