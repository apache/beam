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

import static org.apache.flink.util.Preconditions.checkArgument;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.HeapInternalTimerService;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

/**
 * {@link StateInternals} that uses {@link KeyGroupCheckpointedOperator}
 * to checkpoint state.
 *
 * <p>Note:
 * Ignore index of key.
 * Just implement BagState.
 *
 * <p>Reference from {@link HeapInternalTimerService} to the local key-group range.
 */
public class FlinkKeyGroupStateInternals<K> implements StateInternals {

  private final Coder<K> keyCoder;
  private final KeyGroupsList localKeyGroupRange;
  private KeyedStateBackend keyedStateBackend;
  private final int localKeyGroupRangeStartIdx;

  // stateName -> namespace -> (valueCoder, value)
  private final Map<String, Tuple2<Coder<?>, Map<String, ?>>>[] stateTables;

  public FlinkKeyGroupStateInternals(
      Coder<K> keyCoder,
      KeyedStateBackend keyedStateBackend) {
    this.keyCoder = keyCoder;
    this.keyedStateBackend = keyedStateBackend;
    this.localKeyGroupRange = keyedStateBackend.getKeyGroupRange();
    // find the starting index of the local key-group range
    int startIdx = Integer.MAX_VALUE;
    for (Integer keyGroupIdx : localKeyGroupRange) {
      startIdx = Math.min(keyGroupIdx, startIdx);
    }
    this.localKeyGroupRangeStartIdx = startIdx;
    stateTables = (Map<String, Tuple2<Coder<?>, Map<String, ?>>>[])
        new Map[localKeyGroupRange.getNumberOfKeyGroups()];
    for (int i = 0; i < stateTables.length; i++) {
      stateTables[i] = new HashMap<>();
    }
  }

  @Override
  public K getKey() {
    ByteBuffer keyBytes = (ByteBuffer) keyedStateBackend.getCurrentKey();
    try {
      return CoderUtils.decodeFromByteArray(keyCoder, keyBytes.array());
    } catch (CoderException e) {
      throw new RuntimeException("Error decoding key.", e);
    }
  }

  @Override
  public <T extends State> T state(
      final StateNamespace namespace,
      StateTag<T> address,
      final StateContext<?> context) {

    return address.bind(
        new StateTag.StateBinder() {

          @Override
          public <T> ValueState<T> bindValue(
              StateTag<ValueState<T>> address, Coder<T> coder) {
            throw new UnsupportedOperationException(
                String.format("%s is not supported", ValueState.class.getSimpleName()));
          }

          @Override
          public <T> BagState<T> bindBag(
              StateTag<BagState<T>> address, Coder<T> elemCoder) {

            return new FlinkKeyGroupBagState<>(address, namespace, elemCoder);
          }

          @Override
          public <T> SetState<T> bindSet(
              StateTag<SetState<T>> address, Coder<T> elemCoder) {
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
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValue(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
            throw new UnsupportedOperationException("bindCombiningValue is not supported.");
          }

          @Override
          public <InputT, AccumT, OutputT>
              CombiningState<InputT, AccumT, OutputT> bindCombiningValueWithContext(
                  StateTag<CombiningState<InputT, AccumT, OutputT>> address,
                  Coder<AccumT> accumCoder,
                  CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
            throw new UnsupportedOperationException(
                "bindCombiningValueWithContext is not supported.");
          }

          @Override
          public WatermarkHoldState bindWatermark(
              StateTag<WatermarkHoldState> address,
              TimestampCombiner timestampCombiner) {
            throw new UnsupportedOperationException(
                String.format("%s is not supported", CombiningState.class.getSimpleName()));
          }
        });
  }

  /**
   * Reference from {@link Combine.CombineFn}.
   *
   * <p>Accumulators are stored in each KeyGroup, call addInput() when a element comes,
   * call extractOutput() to produce the desired value when need to read data.
   */
  interface KeyGroupCombiner<InputT, AccumT, OutputT> {

    /**
     * Returns a new, mutable accumulator value, representing the accumulation
     * of zero input values.
     */
    AccumT createAccumulator();

    /**
     * Adds the given input value to the given accumulator, returning the
     * new accumulator value.
     */
    AccumT addInput(AccumT accumulator, InputT input);

    /**
     * Returns the output value that is the result of all accumulators from KeyGroups
     * that are assigned to this operator.
     */
    OutputT extractOutput(Iterable<AccumT> accumulators);
  }

  private abstract class AbstractKeyGroupState<InputT, AccumT, OutputT> {

    private String stateName;
    private String namespace;
    private Coder<AccumT> coder;
    private KeyGroupCombiner<InputT, AccumT, OutputT> keyGroupCombiner;

    AbstractKeyGroupState(
        String stateName,
        String namespace,
        Coder<AccumT> coder,
        KeyGroupCombiner<InputT, AccumT, OutputT> keyGroupCombiner) {
      this.stateName = stateName;
      this.namespace = namespace;
      this.coder = coder;
      this.keyGroupCombiner = keyGroupCombiner;
    }

    /**
     * Choose keyGroup of input and addInput to accumulator.
     */
    void addInput(InputT input) {
      int keyGroupIdx = keyedStateBackend.getCurrentKeyGroupIndex();
      int localIdx = getIndexForKeyGroup(keyGroupIdx);
      Map<String, Tuple2<Coder<?>, Map<String, ?>>> stateTable = stateTables[localIdx];
      Tuple2<Coder<?>, Map<String, ?>> tuple2 = stateTable.get(stateName);
      if (tuple2 == null) {
        tuple2 = new Tuple2<>();
        tuple2.f0 = coder;
        tuple2.f1 = new HashMap<>();
        stateTable.put(stateName, tuple2);
      }
      Map<String, AccumT> map = (Map<String, AccumT>) tuple2.f1;
      AccumT accumulator = map.get(namespace);
      if (accumulator == null) {
        accumulator = keyGroupCombiner.createAccumulator();
      }
      accumulator = keyGroupCombiner.addInput(accumulator, input);
      map.put(namespace, accumulator);
    }

    /**
     * Get all accumulators and invoke extractOutput().
     */
    OutputT extractOutput() {
      List<AccumT> accumulators = new ArrayList<>(stateTables.length);
      for (Map<String, Tuple2<Coder<?>, Map<String, ?>>> stateTable : stateTables) {
        Tuple2<Coder<?>, Map<String, ?>> tuple2 = stateTable.get(stateName);
        if (tuple2 != null) {
          AccumT accumulator = (AccumT) tuple2.f1.get(namespace);
          if (accumulator != null) {
            accumulators.add(accumulator);
          }
        }
      }
      return keyGroupCombiner.extractOutput(accumulators);
    }

    /**
     * Find the first accumulator and return immediately.
     */
    boolean isEmptyInternal() {
      for (Map<String, Tuple2<Coder<?>, Map<String, ?>>> stateTable : stateTables) {
        Tuple2<Coder<?>, Map<String, ?>> tuple2 = stateTable.get(stateName);
        if (tuple2 != null) {
          AccumT accumulator = (AccumT) tuple2.f1.get(namespace);
          if (accumulator != null) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * Clear accumulators and clean empty map.
     */
    void clearInternal() {
      for (Map<String, Tuple2<Coder<?>, Map<String, ?>>> stateTable : stateTables) {
        Tuple2<Coder<?>, Map<String, ?>> tuple2 = stateTable.get(stateName);
        if (tuple2 != null) {
          tuple2.f1.remove(namespace);
          if (tuple2.f1.isEmpty()) {
            stateTable.remove(stateName);
          }
        }
      }
    }

  }

  private int getIndexForKeyGroup(int keyGroupIdx) {
    checkArgument(localKeyGroupRange.contains(keyGroupIdx),
        "Key Group " + keyGroupIdx + " does not belong to the local range.");
    return keyGroupIdx - this.localKeyGroupRangeStartIdx;
  }

  private class KeyGroupBagCombiner<T> implements KeyGroupCombiner<T, List<T>, Iterable<T>> {

    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public Iterable<T> extractOutput(Iterable<List<T>> accumulators) {
      List<T> result = new ArrayList<>();
      // maybe can return an unmodifiable view.
      for (List<T> list : accumulators) {
        result.addAll(list);
      }
      return result;
    }
  }

  private class FlinkKeyGroupBagState<T> extends AbstractKeyGroupState<T, List<T>, Iterable<T>>
      implements BagState<T> {

    private final StateNamespace namespace;
    private final StateTag<BagState<T>> address;

    FlinkKeyGroupBagState(
        StateTag<BagState<T>> address,
        StateNamespace namespace,
        Coder<T> coder) {
      super(
          address.getId(), namespace.stringKey(), ListCoder.of(coder), new KeyGroupBagCombiner<>());
      this.namespace = namespace;
      this.address = address;
    }

    @Override
    public void add(T input) {
      addInput(input);
    }

    @Override
    public BagState<T> readLater() {
      return this;
    }

    @Override
    public Iterable<T> read() {
      Iterable<T> result = extractOutput();
      return result != null ? result : Collections.emptyList();
    }

    @Override
    public ReadableState<Boolean> isEmpty() {
      return new ReadableState<Boolean>() {
        @Override
        public Boolean read() {
          try {
            return isEmptyInternal();
          } catch (Exception e) {
            throw new RuntimeException("Error reading state.", e);
          }

        }

        @Override
        public ReadableState<Boolean> readLater() {
          return this;
        }
      };
    }

    @Override
    public void clear() {
      clearInternal();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FlinkKeyGroupBagState<?> that = (FlinkKeyGroupBagState<?>) o;

      return namespace.equals(that.namespace) && address.equals(that.address);

    }

    @Override
    public int hashCode() {
      int result = namespace.hashCode();
      result = 31 * result + address.hashCode();
      return result;
    }
  }

  /**
   * Snapshots the state {@code (stateName -> (valueCoder && (namespace -> value)))} for a given
   * {@code keyGroupIdx}.
   *
   * @param keyGroupIdx the id of the key-group to be put in the snapshot.
   * @param out the stream to write to.
   */
  public void snapshotKeyGroupState(int keyGroupIdx, DataOutputStream out) throws Exception {
    int localIdx = getIndexForKeyGroup(keyGroupIdx);
    Map<String, Tuple2<Coder<?>, Map<String, ?>>> stateTable = stateTables[localIdx];
    Preconditions.checkState(stateTable.size() <= Short.MAX_VALUE,
        "Too many States: " + stateTable.size() + ". Currently at most "
            + Short.MAX_VALUE + " states are supported");
    out.writeShort(stateTable.size());
    for (Map.Entry<String, Tuple2<Coder<?>, Map<String, ?>>> entry : stateTable.entrySet()) {
      out.writeUTF(entry.getKey());
      Coder coder = entry.getValue().f0;
      InstantiationUtil.serializeObject(out, coder);
      Map<String, ?> map = entry.getValue().f1;
      out.writeInt(map.size());
      for (Map.Entry<String, ?> entry1 : map.entrySet()) {
        StringUtf8Coder.of().encode(entry1.getKey(), out);
        coder.encode(entry1.getValue(), out);
      }
    }
  }

  /**
   * Restore the state {@code (stateName -> (valueCoder && (namespace -> value)))}
   * for a given {@code keyGroupIdx}.
   *
   * @param keyGroupIdx the id of the key-group to be put in the snapshot.
   * @param in the stream to read from.
   * @param userCodeClassLoader the class loader that will be used to deserialize
   *                            the valueCoder.
   */
  public void restoreKeyGroupState(int keyGroupIdx, DataInputStream in,
                                   ClassLoader userCodeClassLoader) throws Exception {
    int localIdx = getIndexForKeyGroup(keyGroupIdx);
    Map<String, Tuple2<Coder<?>, Map<String, ?>>> stateTable = stateTables[localIdx];
    int numStates = in.readShort();
    for (int i = 0; i < numStates; ++i) {
      String stateName = in.readUTF();
      Coder coder = InstantiationUtil.deserializeObject(in, userCodeClassLoader);
      Tuple2<Coder<?>, Map<String, ?>> tuple2 = stateTable.get(stateName);
      if (tuple2 == null) {
        tuple2 = new Tuple2<>();
        tuple2.f0 = coder;
        tuple2.f1 = new HashMap<>();
        stateTable.put(stateName, tuple2);
      }
      Map<String, Object> map = (Map<String, Object>) tuple2.f1;
      int mapSize = in.readInt();
      for (int j = 0; j < mapSize; j++) {
        String namespace = StringUtf8Coder.of().decode(in);
        Object value = coder.decode(in);
        map.put(namespace, value);
      }
    }
  }

}
