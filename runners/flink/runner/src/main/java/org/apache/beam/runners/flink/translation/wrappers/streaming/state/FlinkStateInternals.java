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
package org.apache.beam.runners.flink.translation.wrappers.streaming.state;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.util.state.AccumulatorCombiningState;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.util.state.StateContext;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces;
import org.apache.beam.sdk.util.state.StateTable;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.StateTags;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.protobuf.ByteString;

import org.apache.flink.util.InstantiationUtil;
import org.joda.time.Instant;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * An implementation of the Beam {@link StateInternals}. This implementation simply keeps elements in memory.
 * This state is periodically checkpointed by Flink, for fault-tolerance.
 *
 * TODO: State should be rewritten to redirect to Flink per-key state so that coders and combiners don't need
 * to be serialized along with encoded values when snapshotting.
 */
public class FlinkStateInternals<K> implements StateInternals<K> {

  private final K key;

  private final Coder<K> keyCoder;

  private final Coder<? extends BoundedWindow> windowCoder;

  private final OutputTimeFn<? super BoundedWindow> outputTimeFn;

  private Instant watermarkHoldAccessor;

  public FlinkStateInternals(K key,
                             Coder<K> keyCoder,
                             Coder<? extends BoundedWindow> windowCoder,
                             OutputTimeFn<? super BoundedWindow> outputTimeFn) {
    this.key = key;
    this.keyCoder = keyCoder;
    this.windowCoder = windowCoder;
    this.outputTimeFn = outputTimeFn;
  }

  public Instant getWatermarkHold() {
    return watermarkHoldAccessor;
  }

  /**
   * This is the interface state has to implement in order for it to be fault tolerant when
   * executed by the FlinkRunner.
   */
  private interface CheckpointableIF {

    boolean shouldPersist();

    void persistState(StateCheckpointWriter checkpointBuilder) throws IOException;
  }

  protected final StateTable<K> inMemoryState = new StateTable<K>() {
    @Override
    protected StateTag.StateBinder binderForNamespace(final StateNamespace namespace, final StateContext<?> c) {
      return new StateTag.StateBinder<K>() {

        @Override
        public <T> ValueState<T> bindValue(StateTag<? super K, ValueState<T>> address, Coder<T> coder) {
          return new FlinkInMemoryValue<>(encodeKey(namespace, address), coder);
        }

        @Override
        public <T> BagState<T> bindBag(StateTag<? super K, BagState<T>> address, Coder<T> elemCoder) {
          return new FlinkInMemoryBag<>(encodeKey(namespace, address), elemCoder);
        }

        @Override
        public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT> bindCombiningValue(
            StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder, Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
          return new FlinkInMemoryKeyedCombiningValue<>(encodeKey(namespace, address), combineFn, accumCoder, c);
        }

        @Override
        public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValue(
            StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
          return new FlinkInMemoryKeyedCombiningValue<>(encodeKey(namespace, address), combineFn, accumCoder, c);
        }

        @Override
        public <InputT, AccumT, OutputT> AccumulatorCombiningState<InputT, AccumT, OutputT> bindKeyedCombiningValueWithContext(
            StateTag<? super K, AccumulatorCombiningState<InputT, AccumT, OutputT>> address,
            Coder<AccumT> accumCoder,
            CombineWithContext.KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn) {
          return new FlinkInMemoryKeyedCombiningValue<>(encodeKey(namespace, address), combineFn, accumCoder, c);
        }

        @Override
        public <W extends BoundedWindow> WatermarkHoldState<W> bindWatermark(StateTag<? super K, WatermarkHoldState<W>> address, OutputTimeFn<? super W> outputTimeFn) {
          return new FlinkWatermarkHoldStateImpl<>(encodeKey(namespace, address), outputTimeFn);
        }
      };
    }
  };

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public <StateT extends State> StateT state(StateNamespace namespace, StateTag<? super K, StateT> address) {
    return inMemoryState.get(namespace, address, null);
  }

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<? super K, T> address, StateContext<?> c) {
    return inMemoryState.get(namespace, address, c);
  }

  public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
    checkpointBuilder.writeInt(getNoOfElements());

    for (State location : inMemoryState.values()) {
      if (!(location instanceof CheckpointableIF)) {
        throw new IllegalStateException(String.format(
            "%s wasn't created by %s -- unable to persist it",
            location.getClass().getSimpleName(),
            getClass().getSimpleName()));
      }
      ((CheckpointableIF) location).persistState(checkpointBuilder);
    }
  }

  public void restoreState(StateCheckpointReader checkpointReader, ClassLoader loader)
      throws IOException, ClassNotFoundException {

    // the number of elements to read.
    int noOfElements = checkpointReader.getInt();
    for (int i = 0; i < noOfElements; i++) {
      decodeState(checkpointReader, loader);
    }
  }

  /**
   * We remove the first character which encodes the type of the stateTag ('s' for system
   * and 'u' for user). For more details check out the source of
   * {@link StateTags.StateTagBase#getId()}.
   */
  private void decodeState(StateCheckpointReader reader, ClassLoader loader)
      throws IOException, ClassNotFoundException {

    StateType stateItemType = StateType.deserialize(reader);
    ByteString stateKey = reader.getTag();

    // first decode the namespace and the tagId...
    String[] namespaceAndTag = stateKey.toStringUtf8().split("\\+");
    if (namespaceAndTag.length != 2) {
      throw new IllegalArgumentException("Invalid stateKey " + stateKey.toString() + ".");
    }
    StateNamespace namespace = StateNamespaces.fromString(namespaceAndTag[0], windowCoder);

    // ... decide if it is a system or user stateTag...
    char ownerTag = namespaceAndTag[1].charAt(0);
    if (ownerTag != 's' && ownerTag != 'u') {
      throw new RuntimeException("Invalid StateTag name.");
    }
    boolean isSystemTag = ownerTag == 's';
    String tagId = namespaceAndTag[1].substring(1);

    // ...then decode the coder (if there is one)...
    Coder<?> coder = null;
    switch (stateItemType) {
      case VALUE:
      case LIST:
      case ACCUMULATOR:
        ByteString coderBytes = reader.getData();
        coder = InstantiationUtil.deserializeObject(coderBytes.toByteArray(), loader);
        break;
      case WATERMARK:
        break;
    }

    // ...then decode the combiner function (if there is one)...
    CombineWithContext.KeyedCombineFnWithContext<? super K, ?, ?, ?> combineFn = null;
    switch (stateItemType) {
      case ACCUMULATOR:
        ByteString combinerBytes = reader.getData();
        combineFn = InstantiationUtil.deserializeObject(combinerBytes.toByteArray(), loader);
        break;
      case VALUE:
      case LIST:
      case WATERMARK:
        break;
    }

    //... and finally, depending on the type of the state being decoded,
    // 1) create the adequate stateTag,
    // 2) create the state container,
    // 3) restore the actual content.
    switch (stateItemType) {
      case VALUE: {
        StateTag stateTag = StateTags.value(tagId, coder);
        stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
        @SuppressWarnings("unchecked")
        FlinkInMemoryValue<?> value = (FlinkInMemoryValue<?>) inMemoryState.get(namespace, stateTag, null);
        value.restoreState(reader);
        break;
      }
      case WATERMARK: {
        @SuppressWarnings("unchecked")
        StateTag<Object, WatermarkHoldState<BoundedWindow>> stateTag = StateTags.watermarkStateInternal(tagId, outputTimeFn);
        stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
        @SuppressWarnings("unchecked")
        FlinkWatermarkHoldStateImpl<?> watermark = (FlinkWatermarkHoldStateImpl<?>) inMemoryState.get(namespace, stateTag, null);
        watermark.restoreState(reader);
        break;
      }
      case LIST: {
        StateTag stateTag = StateTags.bag(tagId, coder);
        stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
        FlinkInMemoryBag<?> bag = (FlinkInMemoryBag<?>) inMemoryState.get(namespace, stateTag, null);
        bag.restoreState(reader);
        break;
      }
      case ACCUMULATOR: {
        @SuppressWarnings("unchecked")
        StateTag<K, AccumulatorCombiningState<?, ?, ?>> stateTag = StateTags.keyedCombiningValueWithContext(tagId, (Coder) coder, combineFn);
        stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
        @SuppressWarnings("unchecked")
        FlinkInMemoryKeyedCombiningValue<?, ?, ?> combiningValue =
            (FlinkInMemoryKeyedCombiningValue<?, ?, ?>) inMemoryState.get(namespace, stateTag, null);
        combiningValue.restoreState(reader);
        break;
      }
      default:
        throw new RuntimeException("Unknown State Type " + stateItemType + ".");
    }
  }

  private ByteString encodeKey(StateNamespace namespace, StateTag<? super K, ?> address) {
    StringBuilder sb = new StringBuilder();
    try {
      namespace.appendTo(sb);
      sb.append('+');
      address.appendTo(sb);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ByteString.copyFromUtf8(sb.toString());
  }

  private int getNoOfElements() {
    int noOfElements = 0;
    for (State state : inMemoryState.values()) {
      if (!(state instanceof CheckpointableIF)) {
        throw new RuntimeException("State Implementations used by the " +
            "Flink Dataflow Runner should implement the CheckpointableIF interface.");
      }

      if (((CheckpointableIF) state).shouldPersist()) {
        noOfElements++;
      }
    }
    return noOfElements;
  }

  private final class FlinkInMemoryValue<T> implements ValueState<T>, CheckpointableIF {

    private final ByteString stateKey;
    private final Coder<T> elemCoder;

    private T value = null;

    public FlinkInMemoryValue(ByteString stateKey, Coder<T> elemCoder) {
      this.stateKey = stateKey;
      this.elemCoder = elemCoder;
    }

    @Override
    public void clear() {
      value = null;
    }

    @Override
    public void write(T input) {
      this.value = input;
    }

    @Override
    public T read() {
      return value;
    }

    @Override
    public ValueState<T> readLater() {
      // Ignore
      return this;
    }

    @Override
    public boolean shouldPersist() {
      return value != null;
    }

    @Override
    public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
      if (value != null) {
        // serialize the coder.
        byte[] coder = InstantiationUtil.serializeObject(elemCoder);

        // encode the value into a ByteString
        ByteString.Output stream = ByteString.newOutput();
        elemCoder.encode(value, stream, Coder.Context.OUTER);
        ByteString data = stream.toByteString();

        checkpointBuilder.addValueBuilder()
          .setTag(stateKey)
          .setData(coder)
          .setData(data);
      }
    }

    public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
      ByteString valueContent = checkpointReader.getData();
      T outValue = elemCoder.decode(new ByteArrayInputStream(valueContent.toByteArray()), Coder.Context.OUTER);
      write(outValue);
    }
  }

  private final class FlinkWatermarkHoldStateImpl<W extends BoundedWindow>
      implements WatermarkHoldState<W>, CheckpointableIF {

    private final ByteString stateKey;

    private Instant minimumHold = null;

    private OutputTimeFn<? super W> outputTimeFn;

    public FlinkWatermarkHoldStateImpl(ByteString stateKey, OutputTimeFn<? super W> outputTimeFn) {
      this.stateKey = stateKey;
      this.outputTimeFn = outputTimeFn;
    }

    @Override
    public void clear() {
      // Even though we're clearing we can't remove this from the in-memory state map, since
      // other users may already have a handle on this WatermarkBagInternal.
      minimumHold = null;
      watermarkHoldAccessor = null;
    }

    @Override
    public void add(Instant watermarkHold) {
      if (minimumHold == null || minimumHold.isAfter(watermarkHold)) {
        watermarkHoldAccessor = watermarkHold;
        minimumHold = watermarkHold;
      }
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          return minimumHold == null;
        }

        @Override
        public ReadableState<Boolean> readLater() {
          // Ignore
          return this;
        }
      };
    }

    @Override
    public OutputTimeFn<? super W> getOutputTimeFn() {
      return outputTimeFn;
    }

    @Override
    public Instant read() {
      return minimumHold;
    }

    @Override
    public WatermarkHoldState<W> readLater() {
      // Ignore
      return this;
    }

    @Override
    public String toString() {
      return Objects.toString(minimumHold);
    }

    @Override
    public boolean shouldPersist() {
      return minimumHold != null;
    }

    @Override
    public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
      if (minimumHold != null) {
        checkpointBuilder.addWatermarkHoldsBuilder()
            .setTag(stateKey)
            .setTimestamp(minimumHold);
      }
    }

    public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
      Instant watermark = checkpointReader.getTimestamp();
      add(watermark);
    }
  }


  private static <K, InputT, AccumT, OutputT> CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> withContext(
      final Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
    return new CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT>() {
      @Override
      public AccumT createAccumulator(K key, CombineWithContext.Context c) {
        return combineFn.createAccumulator(key);
      }

      @Override
      public AccumT addInput(K key, AccumT accumulator, InputT value, CombineWithContext.Context c) {
        return combineFn.addInput(key, accumulator, value);
      }

      @Override
      public AccumT mergeAccumulators(K key, Iterable<AccumT> accumulators, CombineWithContext.Context c) {
        return combineFn.mergeAccumulators(key, accumulators);
      }

      @Override
      public OutputT extractOutput(K key, AccumT accumulator, CombineWithContext.Context c) {
        return combineFn.extractOutput(key, accumulator);
      }
    };
  }

  private static <K, InputT, AccumT, OutputT> CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> withKeyAndContext(
      final Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
    return new CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT>() {
      @Override
      public AccumT createAccumulator(K key, CombineWithContext.Context c) {
        return combineFn.createAccumulator();
      }

      @Override
      public AccumT addInput(K key, AccumT accumulator, InputT value, CombineWithContext.Context c) {
        return combineFn.addInput(accumulator, value);
      }

      @Override
      public AccumT mergeAccumulators(K key, Iterable<AccumT> accumulators, CombineWithContext.Context c) {
        return combineFn.mergeAccumulators(accumulators);
      }

      @Override
      public OutputT extractOutput(K key, AccumT accumulator, CombineWithContext.Context c) {
        return combineFn.extractOutput(accumulator);
      }
    };
  }

  private final class FlinkInMemoryKeyedCombiningValue<InputT, AccumT, OutputT>
      implements AccumulatorCombiningState<InputT, AccumT, OutputT>, CheckpointableIF {

    private final ByteString stateKey;
    private final CombineWithContext.KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn;
    private final Coder<AccumT> accumCoder;
    private final CombineWithContext.Context context;

    private AccumT accum = null;
    private boolean isClear = true;

    private FlinkInMemoryKeyedCombiningValue(ByteString stateKey,
                                             Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
                                             Coder<AccumT> accumCoder,
                                             final StateContext<?> stateContext) {
      this(stateKey, withKeyAndContext(combineFn), accumCoder, stateContext);
    }


    private FlinkInMemoryKeyedCombiningValue(ByteString stateKey,
                                             Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn,
                                             Coder<AccumT> accumCoder,
                                             final StateContext<?> stateContext) {
      this(stateKey, withContext(combineFn), accumCoder, stateContext);
    }

    private FlinkInMemoryKeyedCombiningValue(ByteString stateKey,
                                             CombineWithContext.KeyedCombineFnWithContext<? super K, InputT, AccumT, OutputT> combineFn,
                                             Coder<AccumT> accumCoder,
                                             final StateContext<?> stateContext) {
      checkNotNull(combineFn);
      checkNotNull(accumCoder);

      this.stateKey = stateKey;
      this.combineFn = combineFn;
      this.accumCoder = accumCoder;
      this.context = new CombineWithContext.Context() {
        @Override
        public PipelineOptions getPipelineOptions() {
          return stateContext.getPipelineOptions();
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view) {
          return stateContext.sideInput(view);
        }
      };
      accum = combineFn.createAccumulator(key, context);
    }

    @Override
    public void clear() {
      accum = combineFn.createAccumulator(key, context);
      isClear = true;
    }

    @Override
    public void add(InputT input) {
      isClear = false;
      accum = combineFn.addInput(key, accum, input, context);
    }

    @Override
    public AccumT getAccum() {
      return accum;
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          // Ignore
          return this;
        }

        @Override
        public Boolean read() {
          return isClear;
        }
      };
    }

    @Override
    public void addAccum(AccumT accum) {
      isClear = false;
      this.accum = combineFn.mergeAccumulators(key, Arrays.asList(this.accum, accum), context);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      return combineFn.mergeAccumulators(key, accumulators, context);
    }

    @Override
    public OutputT read() {
      return combineFn.extractOutput(key, accum, context);
    }

    @Override
    public AccumulatorCombiningState<InputT, AccumT, OutputT> readLater() {
      // Ignore
      return this;
    }

    @Override
    public boolean shouldPersist() {
      return !isClear;
    }

    @Override
    public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
      if (!isClear) {
        // serialize the coder.
        byte[] coder = InstantiationUtil.serializeObject(accumCoder);

        // serialize the combiner.
        byte[] combiner = InstantiationUtil.serializeObject(combineFn);

        // encode the accumulator into a ByteString
        ByteString.Output stream = ByteString.newOutput();
        accumCoder.encode(accum, stream, Coder.Context.OUTER);
        ByteString data = stream.toByteString();

        // put the flag that the next serialized element is an accumulator
        checkpointBuilder.addAccumulatorBuilder()
          .setTag(stateKey)
          .setData(coder)
          .setData(combiner)
          .setData(data);
      }
    }

    public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
      ByteString valueContent = checkpointReader.getData();
      AccumT accum = this.accumCoder.decode(new ByteArrayInputStream(valueContent.toByteArray()), Coder.Context.OUTER);
      addAccum(accum);
    }
  }

  private static final class FlinkInMemoryBag<T> implements BagState<T>, CheckpointableIF {
    private final List<T> contents = new ArrayList<>();

    private final ByteString stateKey;
    private final Coder<T> elemCoder;

    public FlinkInMemoryBag(ByteString stateKey, Coder<T> elemCoder) {
      this.stateKey = stateKey;
      this.elemCoder = elemCoder;
    }

    @Override
    public void clear() {
      contents.clear();
    }

    @Override
    public Iterable<T> read() {
      return contents;
    }

    @Override
    public BagState<T> readLater() {
      // Ignore
      return this;
    }

    @Override
    public void add(T input) {
      contents.add(input);
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public ReadableState<Boolean> readLater() {
          // Ignore
          return this;
        }

        @Override
        public Boolean read() {
          return contents.isEmpty();
        }
      };
    }

    @Override
    public boolean shouldPersist() {
      return !contents.isEmpty();
    }

    @Override
    public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
      if (!contents.isEmpty()) {
        // serialize the coder.
        byte[] coder = InstantiationUtil.serializeObject(elemCoder);

        checkpointBuilder.addListUpdatesBuilder()
            .setTag(stateKey)
            .setData(coder)
            .writeInt(contents.size());

        for (T item : contents) {
          // encode the element
          ByteString.Output stream = ByteString.newOutput();
          elemCoder.encode(item, stream, Coder.Context.OUTER);
          ByteString data = stream.toByteString();

          // add the data to the checkpoint.
          checkpointBuilder.setData(data);
        }
      }
    }

    public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
      int noOfValues = checkpointReader.getInt();
      for (int j = 0; j < noOfValues; j++) {
        ByteString valueContent = checkpointReader.getData();
        T outValue = elemCoder.decode(new ByteArrayInputStream(valueContent.toByteArray()), Coder.Context.OUTER);
        add(outValue);
      }
    }
  }
}
