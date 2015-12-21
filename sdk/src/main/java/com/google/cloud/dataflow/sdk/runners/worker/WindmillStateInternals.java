/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.WorkItemCommitRequest;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;
import com.google.cloud.dataflow.sdk.util.Weighted;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.state.BagState;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueStateInternal;
import com.google.cloud.dataflow.sdk.util.state.MergingStateInternals;
import com.google.cloud.dataflow.sdk.util.state.State;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateTable;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTag.StateBinder;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.cloud.dataflow.sdk.util.state.WatermarkStateInternal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;

import org.joda.time.Instant;

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

/**
 * Implementation of {@link StateInternals} using Windmill to manage the underlying data.
 */
class WindmillStateInternals extends MergingStateInternals {
  private static class CachingStateTable extends StateTable {
    private final String stateFamily;
    private final WindmillStateReader reader;
    private final WindmillStateCache.ForKey cache;
    private final Supplier<StateSampler.ScopedState> scopedReadStateSupplier;

    public CachingStateTable(String stateFamily,
        WindmillStateReader reader, WindmillStateCache.ForKey cache,
        Supplier<StateSampler.ScopedState> scopedReadStateSupplier) {
      this.stateFamily = stateFamily;
      this.reader = reader;
      this.cache = cache;
      this.scopedReadStateSupplier = scopedReadStateSupplier;
    }

    @Override
    protected StateBinder binderForNamespace(final StateNamespace namespace) {
      // Look up state objects in the cache or create new ones if not found.  The state will
      // be added to the cache in persist().
      return new StateBinder() {
        @Override
        public <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder) {
          WindmillBag<T> result = (WindmillBag<T>) cache.get(namespace, address);
          if (result == null) {
            result = new WindmillBag<T>(namespace, address, stateFamily, elemCoder);
          }
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }

        @Override
        public <W extends BoundedWindow> WatermarkStateInternal bindWatermark(
            StateTag<WatermarkStateInternal> address, OutputTimeFn<? super W> outputTimeFn) {
          WindmillWatermarkState result = (WindmillWatermarkState) cache.get(namespace, address);
          if (result == null) {
            result = new WindmillWatermarkState(namespace, address, stateFamily, outputTimeFn);
          }
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }

        @Override
        public <InputT, AccumT, OutputT> CombiningValueStateInternal<InputT, AccumT, OutputT>
        bindCombiningValue(StateTag<CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn) {
          WindmillCombiningValue<InputT, AccumT, OutputT> result = new WindmillCombiningValue<>(
              namespace, address, stateFamily, accumCoder, combineFn, cache);
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }

        @Override
        public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
          WindmillValue<T> result = (WindmillValue<T>) cache.get(namespace, address);
          if (result == null) {
            result = new WindmillValue<T>(namespace, address, stateFamily, coder);
          }
          result.initializeForWorkItem(reader, scopedReadStateSupplier);
          return result;
        }
      };
    }
  };

  private WindmillStateCache.ForKey cache;
  Supplier<StateSampler.ScopedState> scopedReadStateSupplier;
  private StateTable workItemState;

  public WindmillStateInternals(String stateFamily, WindmillStateReader reader,
      WindmillStateCache.ForKey cache, Supplier<StateSampler.ScopedState> scopedReadStateSupplier) {
    this.cache = cache;
    this.scopedReadStateSupplier = scopedReadStateSupplier;
    this.workItemState = new CachingStateTable(stateFamily, reader, cache, scopedReadStateSupplier);
  }

  public void persist(final Windmill.WorkItemCommitRequest.Builder commitBuilder) {
    List<Future<WorkItemCommitRequest>> commitsToMerge = new ArrayList<>();

    // Call persist on each first, which may schedule some futures for reading.
    for (State location : workItemState.values()) {
      if (!(location instanceof WindmillState)) {
        throw new IllegalStateException(String.format(
            "%s wasn't created by %s -- unable to persist it",
            location.getClass().getSimpleName(),
            getClass().getSimpleName()));
      }

      try {
        commitsToMerge.add(((WindmillState) location).persist(cache));
      } catch (IOException e) {
        throw new RuntimeException("Unable to persist state", e);
      }
    }

    // Clear out the map of already retrieved state instances.
    workItemState.clear();

    try (StateSampler.ScopedState scope = scopedReadStateSupplier.get()) {
      for (Future<WorkItemCommitRequest> commitFuture : commitsToMerge) {
        commitBuilder.mergeFrom(commitFuture.get());
      }
    } catch (ExecutionException | InterruptedException exc) {
      throw new RuntimeException("Failed to retrieve Windmill state during persist()", exc);
    }
  }

  /**
   * Encodes the given namespace and address as {@code &lt;namespace&gt;+&lt;address&gt;}.
   */
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
      throw Throwables.propagate(e);
    }
  }

  /**
   * Abstract base class for all Windmill state.
   *
   * <p>Note that these are not thread safe; each state object is associated with a key
   * and thus only accessed by a single thread at once.
   */
  @NotThreadSafe
  private abstract static class WindmillState {
    protected Supplier<StateSampler.ScopedState> scopedReadStateSupplier;
    protected WindmillStateReader reader;

    /**
     * Return an asynchronously computed {@link WorkItemCommitRequest}. The request should
     * be of a form that can be merged with others (only add to repeated fields).
     */
    abstract Future<WorkItemCommitRequest> persist(WindmillStateCache.ForKey cache)
        throws IOException;

    void initializeForWorkItem(
        WindmillStateReader reader, Supplier<StateSampler.ScopedState> scopedReadStateSupplier) {
      this.reader = reader;
      this.scopedReadStateSupplier = scopedReadStateSupplier;
    }

    StateSampler.ScopedState scopedReadState() {
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
     * Returns a {@link WorkItemCommitRequest} that can be used to persist this state to
     * Windmill.
     */
    protected abstract WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKey cache)
        throws IOException;
  }

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<T> address) {
    return workItemState.get(namespace, address);
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
    private T value;

    private WindmillValue(StateNamespace namespace, StateTag<ValueState<T>> address,
        String stateFamily, Coder<T> coder) {
      this.namespace = namespace;
      this.address = address;
      this.stateKey = encodeKey(namespace, address);
      this.stateFamily = stateFamily;
      this.coder = coder;
    }

    @Override
    public void clear() {
      modified = true;
      valueIsKnown = true;
      value = null;
    }

    @Override
    public StateContents<T> get() {
      final Future<T> future = valueIsKnown ? Futures.immediateFuture(value)
                                            : reader.valueFuture(stateKey, stateFamily, coder);

      return new StateContents<T>() {
        @Override
        public T read() {
          try (StateSampler.ScopedState scope = scopedReadState()) {
            valueIsKnown = true;
            return future.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Unable to read value from state", e);
          }
        }
      };
    }

    @Override
    public void set(T value) {
      modified = true;
      valueIsKnown = true;
      this.value = value;
    }

    @Override
    protected WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKey cache)
        throws IOException {
      if (!modified) {
        // No in-memory changes.
        return WorkItemCommitRequest.newBuilder().buildPartial();
      }

      ByteString.Output stream = ByteString.newOutput();
      if (value != null) {
        coder.encode(value, stream, Coder.Context.OUTER);
      }
      ByteString encoded = stream.toByteString();

      WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
      // Update the entry of the cache with the new value and change in encoded size.
      cache.put(namespace, address, this, encoded.size());

      modified = false;

      commitBuilder
          .addValueUpdatesBuilder()
          .setTag(stateKey)
          .setStateFamily(stateFamily)
          .getValueBuilder()
          .setData(encoded)
          .setTimestamp(Long.MAX_VALUE);

      return commitBuilder.buildPartial();
    }
  }

  private static class WindmillBag<T> extends SimpleWindmillState implements BagState<T> {

    private final StateNamespace namespace;
    private final StateTag<BagState<T>> address;
    private final ByteString stateKey;
    private final String stateFamily;
    private final Coder<T> elemCoder;

    private boolean cleared;
    // Cache of all values in this bag. Null if the persisted state is unknown.
    private ConcatIterables<T> cachedValues = null;
    private List<T> localAdditions = new ArrayList<>();
    private long encodedSize = 0;

    private WindmillBag(StateNamespace namespace, StateTag<BagState<T>> address, String stateFamily,
        Coder<T> elemCoder) {
      this.namespace = namespace;
      this.address = address;
      this.stateKey = encodeKey(namespace, address);
      this.stateFamily = stateFamily;
      this.elemCoder = elemCoder;
    }

    @Override
    public void clear() {
      cleared = true;
      cachedValues = new ConcatIterables<T>();
      localAdditions.clear();
      encodedSize = 0;
    }

    private Iterable<T> fetchData(Future<Iterable<T>> persistedData) {
      try (StateSampler.ScopedState scope = scopedReadState()) {
        if (cachedValues != null) {
          return cachedValues;
        }
        Iterable<T> data = persistedData.get();
        if (data instanceof Weighted) {
          // We have a known bounded amount of data; cache it.
          cachedValues = new ConcatIterables<T>();
          cachedValues.extendWith(data);
          encodedSize = ((Weighted) data).getWeight();
          return cachedValues;
        } else {
          // This is an iterable that may not fit in memory at once; don't cache it.
          return data;
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Unable to read state", e);
      }
    }

    public boolean valuesAreCached() {
      return cachedValues != null;
    }

    @Override
    public StateContents<Iterable<T>> get() {
      // If we clear after calling get() but before calling read(), technically we didn't need the
      // underlying windmill read. But, we need to register the desire now if we aren't going to
      // clear (in order to get it added to the prefetch).
      final Future<Iterable<T>> persistedData = (cachedValues != null)
          ? null
          : reader.listFuture(stateKey, stateFamily, elemCoder);

      return new StateContents<Iterable<T>>() {
        @Override
        public Iterable<T> read() {
          return Iterables.concat(fetchData(persistedData), localAdditions);
        }
      };
    }

    @Override
    public StateContents<Boolean> isEmpty() {
      // If we clear after calling isEmpty() but before calling read(), technically we didn't need
      // the underlying windmill read. But, we need to register the desire now if we aren't going to
      // clear (in order to get it added to the prefetch).
      final Future<Iterable<T>> persistedData = (cachedValues != null)
          ? null
          : reader.listFuture(stateKey, stateFamily, elemCoder);

      return new StateContents<Boolean>() {
        @Override
        public Boolean read() {
          return Iterables.isEmpty(fetchData(persistedData)) && localAdditions.isEmpty();
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

      if (cleared) {
        commitBuilder.addListUpdatesBuilder()
            .setTag(stateKey)
            .setStateFamily(stateFamily)
            .setEndTimestamp(Long.MAX_VALUE);
      }

      if (!localAdditions.isEmpty()) {
        byte[] zero = {0x0};
        Windmill.TagList.Builder listUpdatesBuilder =
            commitBuilder.addListUpdatesBuilder().setTag(stateKey).setStateFamily(stateFamily);
        for (T value : localAdditions) {
          ByteString.Output stream = ByteString.newOutput();

          // Windmill does not support empty data for tag list state; prepend a zero byte.
          stream.write(zero);

          // Encode the value
          elemCoder.encode(value, stream, Coder.Context.OUTER);
          ByteString encoded = stream.toByteString();
          if (cachedValues != null) {
            encodedSize += encoded.size() - 1;
          }

          listUpdatesBuilder.addValuesBuilder()
              .setData(encoded)
              .setTimestamp(Long.MAX_VALUE);
        }
      }

      if (cachedValues != null) {
        cachedValues.extendWith(localAdditions);
        // Don't reuse the localAdditions object; we don't want future changes to it to modify the
        // value of cachedValues.
        localAdditions = new ArrayList<T>();
        cache.put(namespace, address, this, encodedSize);
      } else {
        localAdditions.clear();
      }
      cleared = false;

      return commitBuilder.buildPartial();
    }
  }

  private static class ConcatIterables<T> implements Iterable<T> {
    List<Iterable<T>> iterables;

    public ConcatIterables() {
      this.iterables = new ArrayList<>();
    }

    public void extendWith(Iterable<T> iterable) {
      iterables.add(iterable);
    }

    @Override
    public Iterator<T> iterator() {
      return Iterators.concat(
          Iterables.transform(
                  iterables,
                  new Function<Iterable<T>, Iterator<T>>() {
                    @Override
                    public Iterator<T> apply(Iterable<T> iterable) {
                      return iterable.iterator();
                    }
                  })
              .iterator());
    }
  }

  private static class WindmillWatermarkState
      extends WindmillState implements WatermarkStateInternal {
    // The encoded size of an Instant.
    private static final int ENCODED_SIZE = 8;

    private final OutputTimeFn<?> outputTimeFn;
    private final StateNamespace namespace;
    private final StateTag<WatermarkStateInternal> address;
    private final ByteString stateKey;
    private final String stateFamily;

    private boolean cleared = false;
    // The hold value, Optional.absent() if no hold, or null if unknown.
    private Optional<Instant> cachedValue = null;
    private Instant localAdditions = null;

    private WindmillWatermarkState(StateNamespace namespace,
        StateTag<WatermarkStateInternal> address, String stateFamily,
        OutputTimeFn<?> outputTimeFn) {
      this.namespace = namespace;
      this.address = address;
      this.stateKey = encodeKey(namespace, address);
      this.stateFamily = stateFamily;
      this.outputTimeFn = outputTimeFn;
    }

    @Override
    public void clear() {
      cleared = true;
      cachedValue = Optional.<Instant>absent();
      localAdditions = null;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Does nothing. There is only one hold and it is not extraneous.
     * See {@link MergedWatermarkStateInternal} for a nontrivial implementation.
     */
    @Override
    public void releaseExtraneousHolds() { }

    @Override
    public StateContents<Instant> get() {
      // If we clear after calling get() but before calling read(), technically we didn't need the
      // underlying windmill read. But, we need to register the desire now if we aren't going to
      // clear (in order to get it added to the prefetch).
      final Future<Instant> persistedData = (cachedValue != null)
          ? Futures.immediateFuture(cachedValue.orNull())
          : reader.watermarkFuture(stateKey, stateFamily);

      return new StateContents<Instant>() {
        @Override
        public Instant read() {
          try (StateSampler.ScopedState scope = scopedReadState()) {
            Instant persistedHold = persistedData.get();
            if (persistedHold == null || persistedHold.equals(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
              cachedValue = Optional.absent();
            } else {
              cachedValue = Optional.of(persistedHold);
            }
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Unable to read state", e);
          }

          if (localAdditions == null) {
            return cachedValue.orNull();
          } else if (!cachedValue.isPresent()) {
            return localAdditions;
          } else {
            return outputTimeFn.combine(localAdditions, cachedValue.get());
          }
        }
      };
    }

    @Override
    public StateContents<Boolean> isEmpty() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void add(Instant outputTime) {
      localAdditions = (localAdditions == null) ? outputTime
          : outputTimeFn.combine(outputTime, localAdditions);
    }

    @Override
    public Future<WorkItemCommitRequest> persist(final WindmillStateCache.ForKey cache) {
      Future<WorkItemCommitRequest> result;

      if (!cleared && localAdditions == null) {
        // Nothing to do
        return Futures.immediateFuture(WorkItemCommitRequest.newBuilder().buildPartial());
      } else if (cleared && localAdditions == null) {
        // Just clearing the persisted state; blind delete
        WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
        commitBuilder.addWatermarkHoldsBuilder()
            .setTag(stateKey)
            .setStateFamily(stateFamily)
            .setReset(true);

        result = Futures.immediateFuture(commitBuilder.buildPartial());
      } else if (cleared && localAdditions != null) {
        // Since we cleared before adding, we can do a blind overwrite of persisted state
        WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
        commitBuilder.addWatermarkHoldsBuilder()
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
          result, new Function<WorkItemCommitRequest, WorkItemCommitRequest>() {
            @Override
            public WorkItemCommitRequest apply(WorkItemCommitRequest result) {
              cleared = false;
              localAdditions = null;
              if (cachedValue != null) {
                cache.put(
                    namespace, address, WindmillWatermarkState.this, ENCODED_SIZE);
              }
              return result;
            }
          });
    }

    /**
     * Combines local additions with persisted data and mutates the {@code commitBuilder}
     * to write the result.
     */
    private Future<WorkItemCommitRequest> combineWithPersisted() {
      boolean windmillCanCombine = false;

      // If the combined output time depends only on the window, then we are just blindly adding
      // the same value that may or may not already be present. This depends on the state only being
      // used for one window.
      windmillCanCombine |= outputTimeFn.dependsOnlyOnWindow();

      // If the combined output time depends only on the earliest input timestamp, then because
      // assignOutputTime is monotonic, the hold only depends on the earliest output timestamp
      // (which is the value submitted as a watermark hold). The only way holds for later inputs
      // can be redundant is if the are later (or equal) to the earliest. So taking the MIN
      // implicitly, as Windmill does, has the desired behavior.
      windmillCanCombine |= outputTimeFn.dependsOnlyOnEarliestInputTimestamp();

      if (windmillCanCombine) {
        // We do a blind write and let Windmill take the MIN
        WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
        commitBuilder.addWatermarkHoldsBuilder()
            .setTag(stateKey)
            .setStateFamily(stateFamily)
            .addTimestamps(
                WindmillTimeUtils.harnessToWindmillTimestamp(localAdditions));

        if (cachedValue != null) {
          cachedValue = Optional.of(cachedValue.isPresent()
              ? outputTimeFn.combine(cachedValue.get(), localAdditions)
              : localAdditions);
        }

         return Futures.immediateFuture(commitBuilder.buildPartial());
      } else {
        // The non-fast path does a read-modify-write
        return Futures.lazyTransform((cachedValue != null)
                ? Futures.immediateFuture(cachedValue.orNull())
                : reader.watermarkFuture(stateKey, stateFamily),
            new Function<Instant, WorkItemCommitRequest>() {
              @Override
              public WorkItemCommitRequest apply(Instant priorHold) {
                cachedValue = Optional.of((priorHold != null)
                        ? outputTimeFn.combine(priorHold, localAdditions)
                        : localAdditions);

                WorkItemCommitRequest.Builder commitBuilder = WorkItemCommitRequest.newBuilder();
                commitBuilder.addWatermarkHoldsBuilder()
                    .setTag(stateKey)
                    .setStateFamily(stateFamily)
                    .setReset(true)
                    .addTimestamps(WindmillTimeUtils.harnessToWindmillTimestamp(cachedValue.get()));

                return commitBuilder.buildPartial();
              }
            });
      }
    }
  }

  private static class WindmillCombiningValue<InputT, AccumT, OutputT>
      extends WindmillState implements CombiningValueStateInternal<InputT, AccumT, OutputT> {
    private final WindmillBag<AccumT> bag;
    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    /* We use a separate, in-memory AccumT rather than relying on the WindmillWatermarkBag's
     * localAdditions, because we want to combine multiple InputT's to a single AccumT
     * before adding it.
     */
    private AccumT localAdditionsAccum;
    private boolean hasLocalAdditions = false;

    private WindmillCombiningValue(StateNamespace namespace,
        StateTag<CombiningValueStateInternal<InputT, AccumT, OutputT>> address, String stateFamily,
        Coder<AccumT> accumCoder, CombineFn<InputT, AccumT, OutputT> combineFn,
        WindmillStateCache.ForKey cache) {
      StateTag<BagState<AccumT>> internalBagAddress = StateTags.bag(address.getId(), accumCoder);
      WindmillBag<AccumT> cachedBag =
          (WindmillBag<AccumT>) cache.get(namespace, internalBagAddress);
      this.bag =
          (cachedBag != null)
              ? cachedBag
              : new WindmillBag<>(namespace, internalBagAddress, stateFamily, accumCoder);
      this.combineFn = combineFn;
      this.localAdditionsAccum = combineFn.createAccumulator();
    }

    @Override
    void initializeForWorkItem(
        WindmillStateReader reader, Supplier<StateSampler.ScopedState> scopedReadStateSupplier) {
      super.initializeForWorkItem(reader, scopedReadStateSupplier);
      this.bag.initializeForWorkItem(reader, scopedReadStateSupplier);
    }

    @Override
    public StateContents<OutputT> get() {
      final StateContents<AccumT> accum = getAccum();
      return new StateContents<OutputT>() {
        @Override
        public OutputT read() {
          return combineFn.extractOutput(accum.read());
        }
      };
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
          localAdditionsAccum = getAccum().read();
        }
        bag.add(combineFn.compact(localAdditionsAccum));
        localAdditionsAccum = combineFn.createAccumulator();
        hasLocalAdditions = false;
      }

      return bag.persist(cache);
    }

    @Override
    public StateContents<AccumT> getAccum() {
      final StateContents<Iterable<AccumT>> future = bag.get();

      return new StateContents<AccumT>() {
        @Override
        public AccumT read() {
          Iterable<AccumT> accums = Iterables.concat(
              future.read(), Collections.singleton(localAdditionsAccum));

          // Compact things
          AccumT merged = combineFn.mergeAccumulators(accums);
          bag.clear();
          localAdditionsAccum = merged;
          hasLocalAdditions = true;
          return merged;
        }
      };
    }

    @Override
    public StateContents<Boolean> isEmpty() {
      final StateContents<Boolean> isEmptyFuture = bag.isEmpty();

      return new StateContents<Boolean>() {
        @Override
        public Boolean read() {
          return !hasLocalAdditions && isEmptyFuture.read();
        }
      };
    }


    @Override
    public void addAccum(AccumT accum) {
      hasLocalAdditions = true;
      localAdditionsAccum = combineFn.mergeAccumulators(Arrays.asList(localAdditionsAccum, accum));
    }
  }

  @VisibleForTesting
  static final ThreadLocal<Supplier<Boolean>> COMPACT_NOW =
      new ThreadLocal<Supplier<Boolean>>() {
        public Supplier<Boolean> initialValue() {
          return new Supplier<Boolean>() {
            /* The rate at which, on average, this will return true. */
            static final double RATE = 0.002;
            Random random = new Random();
            long counter = nextSample();

            private long nextSample() {
              // Use geometric distribution to find next true value.
              // This lets us avoid invoking random.nextDouble() on every call.
              return (long) Math.floor(Math.log(random.nextDouble()) / Math.log(1 - RATE));
            }

            public Boolean get() {
              counter--;
              if (counter < 0) {
                counter = nextSample();
                return true;
              } else {
                return false;
              }
            }
          };
        }
      };
}
