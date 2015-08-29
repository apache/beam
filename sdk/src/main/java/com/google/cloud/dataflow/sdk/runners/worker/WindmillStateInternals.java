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
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
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
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.cloud.dataflow.sdk.util.state.WatermarkStateInternal;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link StateInternals} using Windmill to manage the underlying data.
 */
public class WindmillStateInternals extends MergingStateInternals {

  private final StateTable inMemoryState =
      new StateTable() {
        @Override
        protected StateBinder binderForNamespace(final StateNamespace namespace) {
          return new StateBinder() {
            @Override
            public <T> BagState<T> bindBag(StateTag<BagState<T>> address, Coder<T> elemCoder) {
              return new WindmillBag<>(encodeKey(namespace, address), stateFamily, elemCoder,
                  reader, scopedReadStateSupplier);
            }

            @Override
            public <T> WatermarkStateInternal bindWatermark(
                StateTag<WatermarkStateInternal> address) {
              return new WindmillWatermarkState(
                  encodeKey(namespace, address), stateFamily, reader, scopedReadStateSupplier);
            }

            @Override
            public <InputT, AccumT, OutputT>
                CombiningValueStateInternal<InputT, AccumT, OutputT> bindCombiningValue(
                    StateTag<CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
                    Coder<AccumT> accumCoder,
                    CombineFn<InputT, AccumT, OutputT> combineFn) {
              return new WindmillCombiningValue<>(encodeKey(namespace, address), stateFamily,
                  accumCoder, combineFn, reader, scopedReadStateSupplier);
            }

            @Override
            public <T> ValueState<T> bindValue(StateTag<ValueState<T>> address, Coder<T> coder) {
              return new WindmillValue<>(encodeKey(namespace, address), stateFamily, coder, reader,
                  scopedReadStateSupplier);
            }
          };
        }
      };

  private final String prefix;
  private final String stateFamily;
  private final WindmillStateReader reader;
  private final boolean useStateFamilies;
  private final Supplier<StateSampler.ScopedState> scopedReadStateSupplier;

  public WindmillStateInternals(String prefix, boolean useStateFamilies,
      WindmillStateReader reader, final StateSampler stateSampler, final String stepName) {
    this.prefix = prefix;
    if (useStateFamilies) {
      this.stateFamily = prefix;
    } else {
      this.stateFamily = "";
    }
    this.reader = reader;
    this.useStateFamilies = useStateFamilies;
    this.scopedReadStateSupplier = new Supplier<StateSampler.ScopedState>() {
      private int readState = -1;

      @Override
      public StateSampler.ScopedState get() {
        if (stateSampler == null) {
          return null;
        }
        if (readState == -1) {
          readState = stateSampler.stateForName(stepName + "-windmill-read");
        }
        return stateSampler.scopedState(readState);
      }
    };
  }

  public void persist(final Windmill.WorkItemCommitRequest.Builder commitBuilder) {
    // Call persist on each first, which may schedule some futures for reading.
    for (State location : inMemoryState.values()) {
      if (!(location instanceof WindmillState)) {
        throw new IllegalStateException(String.format(
            "%s wasn't created by %s -- unable to persist it",
            location.getClass().getSimpleName(),
            getClass().getSimpleName()));
      }

      try {
        ((WindmillState) location).persist(commitBuilder);
      } catch (IOException e) {
        throw new RuntimeException("Unable to persist state", e);
      }
    }

    // Kick off the fetches that prevent blind-writes. We do this before returning
    // to ensure that the reads have happened before the persist actually happens.
    reader.startBatchAndBlock();

    // Clear out the map of already retrieved state instances.
    inMemoryState.clear();
  }

  private ByteString encodeKey(StateNamespace namespace, StateTag<?> address) {
    if (useStateFamilies) {
      // We don't use prefix here, since it's being set as the stateFamily.
      return ByteString.copyFromUtf8(
          String.format("%s+%s", namespace.stringKey(), address.getId()));
    } else {
      // stringKey starts and ends with a slash. We don't need to seperate it from prefix, because
      // the prefix is guaranteed to be unique and non-overlapping. We separate it from the
      // StateTag ID by a '+' (which is guaranteed not to be in the stringKey) because the
      // ID comes from the user.
      return ByteString.copyFromUtf8(String.format(
          "%s%s+%s", prefix, namespace.stringKey(), address.getId()));
    }
  }

  private interface WindmillState {
    void persist(Windmill.WorkItemCommitRequest.Builder commitBuilder) throws IOException;
  }

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<T> address) {
    return inMemoryState.get(namespace, address);
  }

  private static class WindmillValue<T> implements ValueState<T>, WindmillState {

    private final ByteString stateKey;
    private final String stateFamily;
    private final Coder<T> coder;
    private final WindmillStateReader reader;
    private final Supplier<StateSampler.ScopedState> readStateSupplier;

    /** Whether we've modified the value since creation of this state. */
    private boolean modified = false;
    private T modifiedValue;

    private WindmillValue(ByteString stateKey, String stateFamily, Coder<T> coder,
        WindmillStateReader reader, Supplier<StateSampler.ScopedState> readStateSupplier) {
      this.stateKey = stateKey;
      this.stateFamily = stateFamily;
      this.coder = coder;
      this.reader = reader;
      this.readStateSupplier = readStateSupplier;
    }

    @Override
    public void clear() {
      modified = true;
      modifiedValue = null;
    }

    @Override
    public StateContents<T> get() {
      final Future<T> future = modified ? null : reader.valueFuture(stateKey, stateFamily, coder);

      return new StateContents<T>() {
        @Override
        public T read() {
          try (StateSampler.ScopedState scope = readStateSupplier.get()) {
            return modified ? modifiedValue : future.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Unable to read value from state", e);
          }
        }
      };
    }

    @Override
    public void set(T value) {
      modified = true;
      modifiedValue = value;
    }

    @Override
    public void persist(
        Windmill.WorkItemCommitRequest.Builder commitBuilder) throws IOException {
      if (!modified) {
        // No in-memory changes.
        return;
      }

      // We can't write without doing a read, so we need to kick off a read if we get here.
      // Call reader.valueFuture directly, since our read() method will avoid actually reading from
      // Windmill since the value is already inMemory.
      reader.valueFuture(stateKey, stateFamily, coder);

      ByteString.Output stream = ByteString.newOutput();
      if (modifiedValue != null) {
        coder.encode(modifiedValue, stream, Coder.Context.OUTER);
      }

      commitBuilder
          .addValueUpdatesBuilder()
          .setTag(stateKey)
          .setStateFamily(stateFamily)
          .getValueBuilder()
          .setData(stream.toByteString())
          .setTimestamp(Long.MAX_VALUE);
    }
  }

  private static class WindmillBag<T> implements BagState<T>, WindmillState {

    private final ByteString stateKey;
    private final String stateFamily;
    private final Coder<T> elemCoder;
    private final WindmillStateReader reader;
    private final Supplier<StateSampler.ScopedState> readStateSupplier;

    private boolean cleared = false;
    private final List<T> localAdditions = new ArrayList<>();

    private WindmillBag(ByteString stateKey, String stateFamily, Coder<T> elemCoder,
        WindmillStateReader reader, Supplier<StateSampler.ScopedState> readStateSupplier) {
      this.stateKey = stateKey;
      this.stateFamily = stateFamily;
      this.elemCoder = elemCoder;
      this.reader = reader;
      this.readStateSupplier = readStateSupplier;
    }

    @Override
    public void clear() {
      cleared = true;
      localAdditions.clear();
    }

    @Override
    public StateContents<Iterable<T>> get() {
      // If we clear after calling get() but before calling read(), technically we didn't need the
      // underlying windmill read. But, we need to register the desire now if we aren't going to
      // clear (in order to get it added to the prefetch).
      final Future<Iterable<T>> persistedData = cleared
          ? Futures.<Iterable<T>>immediateFuture(Collections.<T>emptyList())
          : reader.listFuture(stateKey, stateFamily, elemCoder);

      return new StateContents<Iterable<T>>() {
        @Override
        public Iterable<T> read() {
          try (StateSampler.ScopedState scope = readStateSupplier.get()) {
            // We need to check cleared again, because it may have become clear in between creating
            // the future and calling read.
            Iterable<T> input = cleared ? Collections.<T>emptyList() : persistedData.get();
            return Iterables.concat(input, localAdditions);
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Unable to read state", e);
          }
        }
      };
    }

    @Override
    public StateContents<Boolean> isEmpty() {
      // If we clear after calling isEmpty() but before calling read(), technically we didn't need
      // the underlying windmill read. But, we need to register the desire now if we aren't going to
      // clear (in order to get it added to the prefetch).
      final Future<Iterable<T>> persistedData = cleared
          ? Futures.<Iterable<T>>immediateFuture(Collections.<T>emptyList())
          : reader.listFuture(stateKey, stateFamily, elemCoder);

      return new StateContents<Boolean>() {
        @Override
        public Boolean read() {
          try (StateSampler.ScopedState scope = readStateSupplier.get()) {
            // We need to check cleared again, because it may have become clear in between creating
            // the future and calling read.
            Iterable<T> input = cleared ? Collections.<T>emptyList() : persistedData.get();
            return Iterables.isEmpty(input) && Iterables.isEmpty(localAdditions);
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Unable to read state", e);
          }
        }
      };
    }

    @Override
    public void add(T input) {
      localAdditions.add(input);
    }

    @Override
    public void persist(Windmill.WorkItemCommitRequest.Builder commitBuilder) throws IOException {
      if (cleared) {
        // If we do a delete, we need to have done a read to prevent Windmill complaining about
        // blind deletes. We use the underlying reader, because we normally skip the actual read
        // if we've already cleared the state.
        reader.listFuture(stateKey, stateFamily, elemCoder);
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

          listUpdatesBuilder.addValuesBuilder()
              .setData(stream.toByteString())
              .setTimestamp(Long.MAX_VALUE);
        }
      }
    }
  }

  private static class WindmillWatermarkState implements WatermarkStateInternal, WindmillState {

    private final ByteString stateKey;
    private final String stateFamily;
    private final WindmillStateReader reader;
    private final Supplier<StateSampler.ScopedState> readStateSupplier;

    private boolean cleared = false;
    private Instant localAdditions = null;

    private WindmillWatermarkState(ByteString stateKey, String stateFamily,
        WindmillStateReader reader, Supplier<StateSampler.ScopedState> readStateSupplier) {
      this.stateKey = stateKey;
      this.stateFamily = stateFamily;
      this.reader = reader;
      this.readStateSupplier = readStateSupplier;
    }

    @Override
    public void clear() {
      cleared = true;
      localAdditions = null;
    }

    @Override
    public StateContents<Instant> get() {
      // If we clear after calling get() but before calling read(), technically we didn't need the
      // underlying windmill read. But, we need to register the desire now if we aren't going to
      // clear (in order to get it added to the prefetch).
      final Future<Instant> persistedData = cleared
          ? Futures.<Instant>immediateFuture(null)
          : reader.watermarkFuture(stateKey, stateFamily);

      return new StateContents<Instant>() {
        @Override
        public Instant read() {
          Instant value = localAdditions;
          if (!cleared) {
          try (StateSampler.ScopedState scope = readStateSupplier.get()) {
              Instant persisted = persistedData.get();
              if (value == null || (persisted != null && persisted.isBefore(value))) {
                value = persisted;
              }
            } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException("Unable to read state", e);
            }
          }
          return value;
        }
      };
    }

    @Override
    public StateContents<Boolean> isEmpty() {
      // If we clear after calling get() but before calling read(), technically we didn't need the
      // underlying windmill read. But, we need to register the desire now if we aren't going to
      // clear (in order to get it added to the prefetch).
      final Future<Instant> persistedData = cleared
          ? Futures.<Instant>immediateFuture(null)
          : reader.watermarkFuture(stateKey, stateFamily);

      return new StateContents<Boolean>() {
        @Override
        public Boolean read() {
          try (StateSampler.ScopedState scope = readStateSupplier.get()) {
            return localAdditions == null && (cleared || persistedData.get() == null);
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Unable to read state", e);
          }
        }
      };
    }

    @Override
    public void add(Instant watermarkHold) {
      if (localAdditions == null || watermarkHold.isBefore(localAdditions)) {
        localAdditions = watermarkHold;
      }
    }

    @Override
    public void persist(Windmill.WorkItemCommitRequest.Builder commitBuilder) {
      // If we do a delete, we need to have done a read
      if (cleared) {
        reader.watermarkFuture(stateKey, stateFamily);
        commitBuilder.addWatermarkHoldsBuilder()
            .setTag(stateKey)
            .setStateFamily(stateFamily)
            .setReset(true);
      }

      if (localAdditions != null) {
        commitBuilder.addWatermarkHoldsBuilder()
            .setTag(stateKey)
            .setStateFamily(stateFamily)
            .addTimestamps(TimeUnit.MILLISECONDS.toMicros(localAdditions.getMillis()));
      }
    }
  }

  private static class WindmillCombiningValue<InputT, AccumT, OutputT>
      implements CombiningValueStateInternal<InputT, AccumT, OutputT>, WindmillState {

    private final WindmillBag<AccumT> bag;
    private final CombineFn<InputT, AccumT, OutputT> combineFn;

    /* We use a separate, in-memory AccumT rather than relying on the WindmillWatermarkBag's
     * localAdditions, because we want to combine multiple InputT's to a single AccumT
     * before adding it.
     */
    private AccumT localAdditionsAccum;
    private boolean hasLocalAdditions = false;

    private WindmillCombiningValue(ByteString stateKey, String stateFamily,
        Coder<AccumT> accumCoder,
        CombineFn<InputT, AccumT, OutputT> combineFn,
        WindmillStateReader reader, Supplier<StateSampler.ScopedState> readStateSupplier) {
      this.bag = new WindmillBag<>(stateKey, stateFamily, accumCoder, reader, readStateSupplier);
      this.combineFn = combineFn;
      this.localAdditionsAccum = combineFn.createAccumulator();
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
    public void persist(Windmill.WorkItemCommitRequest.Builder commitBuilder) throws IOException {
      if (hasLocalAdditions) {
        bag.add(localAdditionsAccum);
        localAdditionsAccum = combineFn.createAccumulator();
        hasLocalAdditions = false;
      }
      bag.persist(commitBuilder);
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
}
