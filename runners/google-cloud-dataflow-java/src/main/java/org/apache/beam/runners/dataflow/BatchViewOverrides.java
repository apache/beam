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
package org.apache.beam.runners.dataflow;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.util.WindowedValue.valueInEmptyWindows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.runners.dataflow.internal.IsmFormat;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.MetadataKeyCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.GloballyAsSingletonView;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.View.AsSingleton;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Dataflow batch overrides for {@link CreatePCollectionView}, specialized for different view types.
 */
class BatchViewOverrides {
  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsMap View.AsMap} for the
   * Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the key's byte
   * representation. Each record is structured as follows:
   * <ul>
   *   <li>Key 1: User key K</li>
   *   <li>Key 2: Window</li>
   *   <li>Key 3: 0L (constant)</li>
   *   <li>Value: Windowed value</li>
   * </ul>
   *
   * <p>Alongside the data records, there are the following metadata records:
   * <ul>
   *   <li>Key 1: Metadata Key</li>
   *   <li>Key 2: Window</li>
   *   <li>Key 3: Index [0, size of map]</li>
   *   <li>Value: variable length long byte representation of size of map if index is 0,
   *              otherwise the byte representation of a key</li>
   * </ul>
   * The {@code [META, Window, 0]} record stores the number of unique keys per window, while
   * {@code [META, Window, i]}  for {@code i} in {@code [1, size of map]} stores a the users key.
   * This allows for one to access the size of the map by looking at {@code [META, Window, 0]}
   * and iterate over all the keys by accessing {@code [META, Window, i]} for {@code i} in
   * {@code [1, size of map]}.
   *
   * <p>Note that in the case of a non-deterministic key coder, we fallback to using
   * {@link org.apache.beam.sdk.transforms.View.AsSingleton View.AsSingleton} printing
   * a warning to users to specify a deterministic key coder.
   */
  static class BatchViewAsMap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>> {

    /**
     * A {@link DoFn} which groups elements by window boundaries. For each group,
     * the group of elements is transformed into a {@link TransformedMap}.
     * The transformed {@code Map<K, V>} is backed by a {@code Map<K, WindowedValue<V>>}
     * and contains a function {@code WindowedValue<V> -> V}.
     *
     * <p>Outputs {@link IsmRecord}s having:
     * <ul>
     *   <li>Key 1: Window</li>
     *   <li>Value: Transformed map containing a transform that removes the encapsulation
     *              of the window around each value,
     *              {@code Map<K, WindowedValue<V>> -> Map<K, V>}.</li>
     * </ul>
     */
    static class ToMapDoFn<K, V, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, WindowedValue<KV<K, V>>>>>,
                IsmRecord<WindowedValue<TransformedMap<K,
                    WindowedValue<V>,
                    V>>>> {

      private final Coder<W> windowCoder;
      ToMapDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c)
          throws Exception {
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        Optional<W> previousWindow = Optional.absent();
        Map<K, WindowedValue<V>> map = new HashMap<>();
        for (KV<W, WindowedValue<KV<K, V>>> kv : c.element().getValue()) {
          Object currentWindowStructuralValue = windowCoder.structuralValue(kv.getKey());
          if (previousWindowStructuralValue.isPresent()
              && !previousWindowStructuralValue.get().equals(currentWindowStructuralValue)) {
            // Construct the transformed map containing all the elements since we
            // are at a window boundary.
            c.output(IsmRecord.of(
                ImmutableList.of(previousWindow.get()),
                valueInEmptyWindows(new TransformedMap<>(WindowedValueToValue.<V>of(), map))));
            map = new HashMap<>();
          }

          // Verify that the user isn't trying to insert the same key multiple times.
          checkState(!map.containsKey(kv.getValue().getValue().getKey()),
              "Multiple values [%s, %s] found for single key [%s] within window [%s].",
              map.get(kv.getValue().getValue().getKey()),
              kv.getValue().getValue().getValue(),
              kv.getKey());
          map.put(kv.getValue().getValue().getKey(),
              kv.getValue().withValue(kv.getValue().getValue().getValue()));
          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          previousWindow = Optional.of(kv.getKey());
        }

        // The last value for this hash is guaranteed to be at a window boundary
        // so we output a transformed map containing all the elements since the last
        // window boundary.
        c.output(IsmRecord.of(
            ImmutableList.of(previousWindow.get()),
            valueInEmptyWindows(new TransformedMap<>(WindowedValueToValue.<V>of(), map))));
      }
    }

    private final DataflowRunner runner;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsMap(DataflowRunner runner, View.AsMap<K, V> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Map<K, V>> expand(PCollection<KV<K, V>> input) {
      return this.<BoundedWindow>applyInternal(input);
    }

    private <W extends BoundedWindow> PCollectionView<Map<K, V>>
    applyInternal(PCollection<KV<K, V>> input) {

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        PCollectionView<Map<K, V>> view = PCollectionViews.mapView(
            input, input.getWindowingStrategy(), inputCoder);
        return BatchViewAsMultimap.applyForMapLike(runner, input, view, true /* unique keys */);
      } catch (NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);

        // Since the key coder is not deterministic, we convert the map into a singleton
        // and return a singleton view equivalent.
        return applyForSingletonFallback(input);
      }
    }

    @Override
    protected String getKindString() {
      return "BatchViewAsMap";
    }

    /** Transforms the input {@link PCollection} into a singleton {@link Map} per window. */
    private <W extends BoundedWindow> PCollectionView<Map<K, V>>
    applyForSingletonFallback(PCollection<KV<K, V>> input) {
      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();

      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder<Function<WindowedValue<V>, V>> transformCoder =
          (Coder) SerializableCoder.of(WindowedValueToValue.class);

      Coder<TransformedMap<K, WindowedValue<V>, V>> finalValueCoder =
          TransformedMapCoder.of(
              transformCoder,
              MapCoder.of(
                  inputCoder.getKeyCoder(),
                  FullWindowedValueCoder.of(inputCoder.getValueCoder(), windowCoder)));

      TransformedMap<K, WindowedValue<V>, V> defaultValue = new TransformedMap<>(
          WindowedValueToValue.<V>of(),
          ImmutableMap.<K, WindowedValue<V>>of());

      return BatchViewAsSingleton.<KV<K, V>, TransformedMap<K, WindowedValue<V>, V>,
          Map<K, V>,
          W> applyForSingleton(
          runner,
          input,
          new ToMapDoFn<K, V, W>(windowCoder),
          true,
          defaultValue,
          finalValueCoder);
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsMultimap View.AsMultimap} for the
   * Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the key's byte
   * representation. Each record is structured as follows:
   * <ul>
   *   <li>Key 1: User key K</li>
   *   <li>Key 2: Window</li>
   *   <li>Key 3: Index offset for a given key and window.</li>
   *   <li>Value: Windowed value</li>
   * </ul>
   *
   * <p>Alongside the data records, there are the following metadata records:
   * <ul>
   *   <li>Key 1: Metadata Key</li>
   *   <li>Key 2: Window</li>
   *   <li>Key 3: Index [0, size of map]</li>
   *   <li>Value: variable length long byte representation of size of map if index is 0,
   *              otherwise the byte representation of a key</li>
   * </ul>
   * The {@code [META, Window, 0]} record stores the number of unique keys per window, while
   * {@code [META, Window, i]}  for {@code i} in {@code [1, size of map]} stores a the users key.
   * This allows for one to access the size of the map by looking at {@code [META, Window, 0]}
   * and iterate over all the keys by accessing {@code [META, Window, i]} for {@code i} in
   * {@code [1, size of map]}.
   *
   * <p>Note that in the case of a non-deterministic key coder, we fallback to using
   * {@link org.apache.beam.sdk.transforms.View.AsSingleton View.AsSingleton} printing
   * a warning to users to specify a deterministic key coder.
   */
  static class BatchViewAsMultimap<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>> {
    /**
     * A {@link PTransform} that groups elements by the hash of window's byte representation
     * if the input {@link PCollection} is not within the global window. Otherwise by the hash
     * of the window and key's byte representation. This {@link PTransform} also sorts
     * the values by the combination of the window and key's byte representations.
     */
    private static class GroupByKeyHashAndSortByKeyAndWindow<K, V, W extends BoundedWindow>
        extends PTransform<PCollection<KV<K, V>>,
        PCollection<KV<Integer, Iterable<KV<KV<K, W>, WindowedValue<V>>>>>> {

      @SystemDoFnInternal
      private static class GroupByKeyHashAndSortByKeyAndWindowDoFn<K, V, W>
          extends DoFn<KV<K, V>, KV<Integer, KV<KV<K, W>, WindowedValue<V>>>> {

        private final IsmRecordCoder<?> coder;
        private GroupByKeyHashAndSortByKeyAndWindowDoFn(IsmRecordCoder<?> coder) {
          this.coder = coder;
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow untypedWindow) throws Exception {
          @SuppressWarnings("unchecked")
          W window = (W) untypedWindow;

          c.output(
              KV.of(coder.hash(ImmutableList.of(c.element().getKey())),
                  KV.of(KV.of(c.element().getKey(), window),
                      WindowedValue.of(
                          c.element().getValue(),
                          c.timestamp(),
                          untypedWindow,
                          c.pane()))));
        }
      }

      private final IsmRecordCoder<?> coder;
      public GroupByKeyHashAndSortByKeyAndWindow(IsmRecordCoder<?> coder) {
        this.coder = coder;
      }

      @Override
      public PCollection<KV<Integer, Iterable<KV<KV<K, W>, WindowedValue<V>>>>>
      expand(PCollection<KV<K, V>> input) {

        @SuppressWarnings("unchecked")
        Coder<W> windowCoder = (Coder<W>)
            input.getWindowingStrategy().getWindowFn().windowCoder();
        @SuppressWarnings("unchecked")
        KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();

        PCollection<KV<Integer, KV<KV<K, W>, WindowedValue<V>>>> keyedByHash;
        keyedByHash = input.apply(
            ParDo.of(new GroupByKeyHashAndSortByKeyAndWindowDoFn<K, V, W>(coder)));
        keyedByHash.setCoder(
            KvCoder.of(
                VarIntCoder.of(),
                KvCoder.of(KvCoder.of(inputCoder.getKeyCoder(), windowCoder),
                    FullWindowedValueCoder.of(inputCoder.getValueCoder(), windowCoder))));

        return keyedByHash.apply(
            new GroupByKeyAndSortValuesOnly<Integer, KV<K, W>, WindowedValue<V>>());
      }
    }

    /**
     * A {@link DoFn} which creates {@link IsmRecord}s comparing successive elements windows
     * and keys to locate window and key boundaries. The main output {@link IsmRecord}s have:
     * <ul>
     *   <li>Key 1: Window</li>
     *   <li>Key 2: User key K</li>
     *   <li>Key 3: Index offset for a given key and window.</li>
     *   <li>Value: Windowed value</li>
     * </ul>
     *
     * <p>Additionally, we output all the unique keys per window seen to {@code outputForEntrySet}
     * and the unique key count per window to {@code outputForSize}.
     *
     * <p>Finally, if this {@link DoFn} has been requested to perform unique key checking, it will
     * throw an {@link IllegalStateException} if more than one key per window is found.
     */
    static class ToIsmRecordForMapLikeDoFn<K, V, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<KV<K, W>, WindowedValue<V>>>>,
        IsmRecord<WindowedValue<V>>> {

      private final TupleTag<KV<Integer, KV<W, Long>>> outputForSize;
      private final TupleTag<KV<Integer, KV<W, K>>> outputForEntrySet;
      private final Coder<W> windowCoder;
      private final Coder<K> keyCoder;
      private final IsmRecordCoder<WindowedValue<V>> ismCoder;
      private final boolean uniqueKeysExpected;
      ToIsmRecordForMapLikeDoFn(
          TupleTag<KV<Integer, KV<W, Long>>> outputForSize,
          TupleTag<KV<Integer, KV<W, K>>> outputForEntrySet,
          Coder<W> windowCoder,
          Coder<K> keyCoder,
          IsmRecordCoder<WindowedValue<V>> ismCoder,
          boolean uniqueKeysExpected) {
        this.outputForSize = outputForSize;
        this.outputForEntrySet = outputForEntrySet;
        this.windowCoder = windowCoder;
        this.keyCoder = keyCoder;
        this.ismCoder = ismCoder;
        this.uniqueKeysExpected = uniqueKeysExpected;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        long currentKeyIndex = 0;
        // We use one based indexing while counting
        long currentUniqueKeyCounter = 1;
        Iterator<KV<KV<K, W>, WindowedValue<V>>> iterator = c.element().getValue().iterator();

        KV<KV<K, W>, WindowedValue<V>> currentValue = iterator.next();
        Object currentKeyStructuralValue =
            keyCoder.structuralValue(currentValue.getKey().getKey());
        Object currentWindowStructuralValue =
            windowCoder.structuralValue(currentValue.getKey().getValue());

        while (iterator.hasNext()) {
          KV<KV<K, W>, WindowedValue<V>> nextValue = iterator.next();
          Object nextKeyStructuralValue =
              keyCoder.structuralValue(nextValue.getKey().getKey());
          Object nextWindowStructuralValue =
              windowCoder.structuralValue(nextValue.getKey().getValue());

          outputDataRecord(c, currentValue, currentKeyIndex);

          final long nextKeyIndex;
          final long nextUniqueKeyCounter;

          // Check to see if its a new window
          if (!currentWindowStructuralValue.equals(nextWindowStructuralValue)) {
            // The next value is a new window, so we output for size the number of unique keys
            // seen and the last key of the window. We also reset the next key index the unique
            // key counter.
            outputMetadataRecordForSize(c, currentValue, currentUniqueKeyCounter);
            outputMetadataRecordForEntrySet(c, currentValue);

            nextKeyIndex = 0;
            nextUniqueKeyCounter = 1;
          } else if (!currentKeyStructuralValue.equals(nextKeyStructuralValue)){
            // It is a new key within the same window so output the key for the entry set,
            // reset the key index and increase the count of unique keys seen within this window.
            outputMetadataRecordForEntrySet(c, currentValue);

            nextKeyIndex = 0;
            nextUniqueKeyCounter = currentUniqueKeyCounter + 1;
          } else if (!uniqueKeysExpected) {
            // It is not a new key so we don't have to output the number of elements in this
            // window or increase the unique key counter. All we do is increase the key index.

            nextKeyIndex = currentKeyIndex + 1;
            nextUniqueKeyCounter = currentUniqueKeyCounter;
          } else {
            throw new IllegalStateException(String.format(
                "Unique keys are expected but found key %s with values %s and %s in window %s.",
                currentValue.getKey().getKey(),
                currentValue.getValue().getValue(),
                nextValue.getValue().getValue(),
                currentValue.getKey().getValue()));
          }

          currentValue = nextValue;
          currentWindowStructuralValue = nextWindowStructuralValue;
          currentKeyStructuralValue = nextKeyStructuralValue;
          currentKeyIndex = nextKeyIndex;
          currentUniqueKeyCounter = nextUniqueKeyCounter;
        }

        outputDataRecord(c, currentValue, currentKeyIndex);
        outputMetadataRecordForSize(c, currentValue, currentUniqueKeyCounter);
        // The last value for this hash is guaranteed to be at a window boundary
        // so we output a record with the number of unique keys seen.
        outputMetadataRecordForEntrySet(c, currentValue);
      }

      /** This outputs the data record. */
      private void outputDataRecord(
          ProcessContext c, KV<KV<K, W>, WindowedValue<V>> value, long keyIndex) {
        IsmRecord<WindowedValue<V>> ismRecord = IsmRecord.of(
            ImmutableList.of(
                value.getKey().getKey(),
                value.getKey().getValue(),
                keyIndex),
            value.getValue());
        c.output(ismRecord);
      }

      /**
       * This outputs records which will be used to compute the number of keys for a given window.
       */
      private void outputMetadataRecordForSize(
          ProcessContext c, KV<KV<K, W>, WindowedValue<V>> value, long uniqueKeyCount) {
        c.output(outputForSize,
            KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(),
                value.getKey().getValue())),
                KV.of(value.getKey().getValue(), uniqueKeyCount)));
      }

      /** This outputs records which will be used to construct the entry set. */
      private void outputMetadataRecordForEntrySet(
          ProcessContext c, KV<KV<K, W>, WindowedValue<V>> value) {
        c.output(outputForEntrySet,
            KV.of(ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(),
                value.getKey().getValue())),
                KV.of(value.getKey().getValue(), value.getKey().getKey())));
      }
    }

    /**
     * A {@link DoFn} which outputs a metadata {@link IsmRecord} per window of:
     * <ul>
     *   <li>Key 1: META key</li>
     *   <li>Key 2: window</li>
     *   <li>Key 3: 0L (constant)</li>
     *   <li>Value: sum of values for window</li>
     * </ul>
     *
     * <p>This {@link DoFn} is meant to be used to compute the number of unique keys
     * per window for map and multimap side inputs.
     */
    static class ToIsmMetadataRecordForSizeDoFn<K, V, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, Long>>>, IsmRecord<WindowedValue<V>>> {
      private final Coder<W> windowCoder;
      ToIsmMetadataRecordForSizeDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Iterator<KV<W, Long>> iterator = c.element().getValue().iterator();
        KV<W, Long> currentValue = iterator.next();
        Object currentWindowStructuralValue = windowCoder.structuralValue(currentValue.getKey());
        long size = 0;
        while (iterator.hasNext()) {
          KV<W, Long> nextValue = iterator.next();
          Object nextWindowStructuralValue = windowCoder.structuralValue(nextValue.getKey());

          size += currentValue.getValue();
          if (!currentWindowStructuralValue.equals(nextWindowStructuralValue)) {
            c.output(IsmRecord.<WindowedValue<V>>meta(
                ImmutableList.of(IsmFormat.getMetadataKey(), currentValue.getKey(), 0L),
                CoderUtils.encodeToByteArray(VarLongCoder.of(), size)));
            size = 0;
          }

          currentValue = nextValue;
          currentWindowStructuralValue = nextWindowStructuralValue;
        }

        size += currentValue.getValue();
        // Output the final value since it is guaranteed to be on a window boundary.
        c.output(IsmRecord.<WindowedValue<V>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), currentValue.getKey(), 0L),
            CoderUtils.encodeToByteArray(VarLongCoder.of(), size)));
      }
    }

    /**
     * A {@link DoFn} which outputs a metadata {@link IsmRecord} per window and key pair of:
     * <ul>
     *   <li>Key 1: META key</li>
     *   <li>Key 2: window</li>
     *   <li>Key 3: index offset (1-based index)</li>
     *   <li>Value: key</li>
     * </ul>
     *
     * <p>This {@link DoFn} is meant to be used to output index to key records
     * per window for map and multimap side inputs.
     */
    static class ToIsmMetadataRecordForKeyDoFn<K, V, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, K>>>, IsmRecord<WindowedValue<V>>> {

      private final Coder<K> keyCoder;
      private final Coder<W> windowCoder;
      ToIsmMetadataRecordForKeyDoFn(Coder<K> keyCoder, Coder<W> windowCoder) {
        this.keyCoder = keyCoder;
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Iterator<KV<W, K>> iterator = c.element().getValue().iterator();
        KV<W, K> currentValue = iterator.next();
        Object currentWindowStructuralValue = windowCoder.structuralValue(currentValue.getKey());
        long elementsInWindow = 1;
        while (iterator.hasNext()) {
          KV<W, K> nextValue = iterator.next();
          Object nextWindowStructuralValue = windowCoder.structuralValue(nextValue.getKey());

          c.output(IsmRecord.<WindowedValue<V>>meta(
              ImmutableList.of(IsmFormat.getMetadataKey(), currentValue.getKey(), elementsInWindow),
              CoderUtils.encodeToByteArray(keyCoder, currentValue.getValue())));
          elementsInWindow += 1;

          if (!currentWindowStructuralValue.equals(nextWindowStructuralValue)) {
            elementsInWindow = 1;
          }

          currentValue = nextValue;
          currentWindowStructuralValue = nextWindowStructuralValue;
        }

        // Output the final value since it is guaranteed to be on a window boundary.
        c.output(IsmRecord.<WindowedValue<V>>meta(
            ImmutableList.of(IsmFormat.getMetadataKey(), currentValue.getKey(), elementsInWindow),
            CoderUtils.encodeToByteArray(keyCoder, currentValue.getValue())));
      }
    }

    /**
     * A {@link DoFn} which partitions sets of elements by window boundaries. Within each
     * partition, the set of elements is transformed into a {@link TransformedMap}.
     * The transformed {@code Map<K, Iterable<V>>} is backed by a
     * {@code Map<K, Iterable<WindowedValue<V>>>} and contains a function
     * {@code Iterable<WindowedValue<V>> -> Iterable<V>}.
     *
     * <p>Outputs {@link IsmRecord}s having:
     * <ul>
     *   <li>Key 1: Window</li>
     *   <li>Value: Transformed map containing a transform that removes the encapsulation
     *              of the window around each value,
     *              {@code Map<K, Iterable<WindowedValue<V>>> -> Map<K, Iterable<V>>}.</li>
     * </ul>
     */
    static class ToMultimapDoFn<K, V, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, WindowedValue<KV<K, V>>>>>,
        IsmRecord<WindowedValue<TransformedMap<K,
            Iterable<WindowedValue<V>>,
            Iterable<V>>>>> {

      private final Coder<W> windowCoder;
      ToMultimapDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c)
          throws Exception {
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        Optional<W> previousWindow = Optional.absent();
        Multimap<K, WindowedValue<V>> multimap = HashMultimap.create();
        for (KV<W, WindowedValue<KV<K, V>>> kv : c.element().getValue()) {
          Object currentWindowStructuralValue = windowCoder.structuralValue(kv.getKey());
          if (previousWindowStructuralValue.isPresent()
              && !previousWindowStructuralValue.get().equals(currentWindowStructuralValue)) {
            // Construct the transformed map containing all the elements since we
            // are at a window boundary.
            @SuppressWarnings({"unchecked", "rawtypes"})
            Map<K, Iterable<WindowedValue<V>>> resultMap = (Map) multimap.asMap();
            c.output(IsmRecord.<WindowedValue<TransformedMap<K,
                Iterable<WindowedValue<V>>,
                Iterable<V>>>>of(
                ImmutableList.of(previousWindow.get()),
                valueInEmptyWindows(
                    new TransformedMap<>(
                        IterableWithWindowedValuesToIterable.<V>of(), resultMap))));
            multimap = HashMultimap.create();
          }

          multimap.put(kv.getValue().getValue().getKey(),
              kv.getValue().withValue(kv.getValue().getValue().getValue()));
          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          previousWindow = Optional.of(kv.getKey());
        }

        // The last value for this hash is guaranteed to be at a window boundary
        // so we output a transformed map containing all the elements since the last
        // window boundary.
        @SuppressWarnings({"unchecked", "rawtypes"})
        Map<K, Iterable<WindowedValue<V>>> resultMap = (Map) multimap.asMap();
        c.output(IsmRecord.<WindowedValue<TransformedMap<K,
            Iterable<WindowedValue<V>>,
            Iterable<V>>>>of(
            ImmutableList.of(previousWindow.get()),
            valueInEmptyWindows(
                new TransformedMap<>(IterableWithWindowedValuesToIterable.<V>of(), resultMap))));
      }
    }

    private final DataflowRunner runner;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsMultimap(DataflowRunner runner, View.AsMultimap<K, V> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Map<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      return this.<BoundedWindow>applyInternal(input);
    }

    private <W extends BoundedWindow> PCollectionView<Map<K, Iterable<V>>>
    applyInternal(PCollection<KV<K, V>> input) {
      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
      try {
        PCollectionView<Map<K, Iterable<V>>> view = PCollectionViews.multimapView(
            input, input.getWindowingStrategy(), inputCoder);

        return applyForMapLike(runner, input, view, false /* unique keys not expected */);
      } catch (NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);

        // Since the key coder is not deterministic, we convert the map into a singleton
        // and return a singleton view equivalent.
        return applyForSingletonFallback(input);
      }
    }

    /** Transforms the input {@link PCollection} into a singleton {@link Map} per window. */
    private <W extends BoundedWindow> PCollectionView<Map<K, Iterable<V>>>
    applyForSingletonFallback(PCollection<KV<K, V>> input) {
      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();

      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder<Function<Iterable<WindowedValue<V>>, Iterable<V>>> transformCoder =
          (Coder) SerializableCoder.of(IterableWithWindowedValuesToIterable.class);

      Coder<TransformedMap<K, Iterable<WindowedValue<V>>, Iterable<V>>> finalValueCoder =
          TransformedMapCoder.of(
              transformCoder,
              MapCoder.of(
                  inputCoder.getKeyCoder(),
                  IterableCoder.of(
                      FullWindowedValueCoder.of(inputCoder.getValueCoder(), windowCoder))));

      TransformedMap<K, Iterable<WindowedValue<V>>, Iterable<V>> defaultValue =
          new TransformedMap<>(
              IterableWithWindowedValuesToIterable.<V>of(),
              ImmutableMap.<K, Iterable<WindowedValue<V>>>of());

      return BatchViewAsSingleton.<KV<K, V>,
          TransformedMap<K, Iterable<WindowedValue<V>>, Iterable<V>>,
          Map<K, Iterable<V>>,
          W> applyForSingleton(
          runner,
          input,
          new ToMultimapDoFn<K, V, W>(windowCoder),
          true,
          defaultValue,
          finalValueCoder);
    }

    private static <K, V, W extends BoundedWindow, ViewT> PCollectionView<ViewT> applyForMapLike(
        DataflowRunner runner,
        PCollection<KV<K, V>> input,
        PCollectionView<ViewT> view,
        boolean uniqueKeysExpected) throws NonDeterministicException {

      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      @SuppressWarnings({"rawtypes", "unchecked"})
      KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();

      // If our key coder is deterministic, we can use the key portion of each KV
      // part of a composite key containing the window , key and index.
      inputCoder.getKeyCoder().verifyDeterministic();

      IsmRecordCoder<WindowedValue<V>> ismCoder =
          coderForMapLike(windowCoder, inputCoder.getKeyCoder(), inputCoder.getValueCoder());

      // Create the various output tags representing the main output containing the data stream
      // and the additional outputs containing the metadata about the size and entry set.
      TupleTag<IsmRecord<WindowedValue<V>>> mainOutputTag = new TupleTag<>();
      TupleTag<KV<Integer, KV<W, Long>>> outputForSizeTag = new TupleTag<>();
      TupleTag<KV<Integer, KV<W, K>>> outputForEntrySetTag = new TupleTag<>();

      // Process all the elements grouped by key hash, and sorted by key and then window
      // outputting to all the outputs defined above.
      PCollectionTuple outputTuple = input
          .apply("GBKaSVForData", new GroupByKeyHashAndSortByKeyAndWindow<K, V, W>(ismCoder))
          .apply(ParDo.of(new ToIsmRecordForMapLikeDoFn<>(
              outputForSizeTag, outputForEntrySetTag,
              windowCoder, inputCoder.getKeyCoder(), ismCoder, uniqueKeysExpected))
              .withOutputTags(mainOutputTag,
                  TupleTagList.of(
                      ImmutableList.<TupleTag<?>>of(outputForSizeTag,
                          outputForEntrySetTag))));

      // Set the coder on the main data output.
      PCollection<IsmRecord<WindowedValue<V>>> perHashWithReifiedWindows =
          outputTuple.get(mainOutputTag);
      perHashWithReifiedWindows.setCoder(ismCoder);

      // Set the coder on the metadata output for size and process the entries
      // producing a [META, Window, 0L] record per window storing the number of unique keys
      // for each window.
      PCollection<KV<Integer, KV<W, Long>>> outputForSize = outputTuple.get(outputForSizeTag);
      outputForSize.setCoder(
          KvCoder.of(VarIntCoder.of(),
              KvCoder.of(windowCoder, VarLongCoder.of())));
      PCollection<IsmRecord<WindowedValue<V>>> windowMapSizeMetadata = outputForSize
          .apply("GBKaSVForSize", new GroupByKeyAndSortValuesOnly<Integer, W, Long>())
          .apply(ParDo.of(new ToIsmMetadataRecordForSizeDoFn<K, V, W>(windowCoder)));
      windowMapSizeMetadata.setCoder(ismCoder);

      // Set the coder on the metadata output destined to build the entry set and process the
      // entries producing a [META, Window, Index] record per window key pair storing the key.
      PCollection<KV<Integer, KV<W, K>>> outputForEntrySet =
          outputTuple.get(outputForEntrySetTag);
      outputForEntrySet.setCoder(
          KvCoder.of(VarIntCoder.of(),
              KvCoder.of(windowCoder, inputCoder.getKeyCoder())));
      PCollection<IsmRecord<WindowedValue<V>>> windowMapKeysMetadata = outputForEntrySet
          .apply("GBKaSVForKeys", new GroupByKeyAndSortValuesOnly<Integer, W, K>())
          .apply(ParDo.of(
              new ToIsmMetadataRecordForKeyDoFn<K, V, W>(inputCoder.getKeyCoder(), windowCoder)));
      windowMapKeysMetadata.setCoder(ismCoder);

      // Set that all these outputs should be materialized using an indexed format.
      runner.addPCollectionRequiringIndexedFormat(perHashWithReifiedWindows);
      runner.addPCollectionRequiringIndexedFormat(windowMapSizeMetadata);
      runner.addPCollectionRequiringIndexedFormat(windowMapKeysMetadata);

      PCollectionList<IsmRecord<WindowedValue<V>>> outputs =
          PCollectionList.of(ImmutableList.of(
              perHashWithReifiedWindows, windowMapSizeMetadata, windowMapKeysMetadata));

      return Pipeline.applyTransform(outputs,
          Flatten.<IsmRecord<WindowedValue<V>>>pCollections())
          .apply(CreateDataflowView.<IsmRecord<WindowedValue<V>>,
              ViewT>of(view));
    }

    @Override
    protected String getKindString() {
      return "BatchViewAsMultimap";
    }

    static <V> IsmRecordCoder<WindowedValue<V>> coderForMapLike(
        Coder<? extends BoundedWindow> windowCoder, Coder<?> keyCoder, Coder<V> valueCoder) {
      // TODO: swap to use a variable length long coder which has values which compare
      // the same as their byte representation compare lexicographically within the key coder
      return IsmRecordCoder.of(
          1, // We use only the key for hashing when producing value records
          2, // Since the key is not present, we add the window to the hash when
          // producing metadata records
          ImmutableList.of(
              MetadataKeyCoder.of(keyCoder),
              windowCoder,
              BigEndianLongCoder.of()),
          FullWindowedValueCoder.of(valueCoder, windowCoder));
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsSingleton View.AsSingleton} for the
   * Dataflow runner in batch mode.
   *
   * <p>Creates a set of files in the {@link IsmFormat} sharded by the hash of the windows
   * byte representation and with records having:
   * <ul>
   *   <li>Key 1: Window</li>
   *   <li>Value: Windowed value</li>
   * </ul>
   */
  static class BatchViewAsSingleton<T>
      extends PTransform<PCollection<T>, PCollectionView<T>> {

    /**
     * A {@link DoFn} that outputs {@link IsmRecord}s. These records are structured as follows:
     * <ul>
     *   <li>Key 1: Window
     *   <li>Value: Windowed value
     * </ul>
     */
    static class IsmRecordForSingularValuePerWindowDoFn<T, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>,
        IsmRecord<WindowedValue<T>>> {

      private final Coder<W> windowCoder;
      IsmRecordForSingularValuePerWindowDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        T previousValue = null;

        Iterator<KV<W, WindowedValue<T>>> iterator = c.element().getValue().iterator();
        while (iterator.hasNext()) {
          KV<W, WindowedValue<T>> next = iterator.next();
          Object currentWindowStructuralValue = windowCoder.structuralValue(next.getKey());

          // Verify that the user isn't trying to have more than one element per window as
          // a singleton.
          checkState(!previousWindowStructuralValue.isPresent()
                  || !previousWindowStructuralValue.get().equals(currentWindowStructuralValue),
              "Multiple values [%s, %s] found for singleton within window [%s].",
              previousValue,
              next.getValue().getValue(),
              next.getKey());

          c.output(
              IsmRecord.of(
                  ImmutableList.of(next.getKey()), next.getValue()));

          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          previousValue = next.getValue().getValue();
        }
      }
    }

    private final DataflowRunner runner;
    private final View.AsSingleton<T> transform;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsSingleton(DataflowRunner runner, View.AsSingleton<T> transform) {
      this.runner = runner;
      this.transform = transform;
    }

    @Override
    public PCollectionView<T> expand(PCollection<T> input) {
      @SuppressWarnings("unchecked")
      Coder<BoundedWindow> windowCoder = (Coder<BoundedWindow>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      return BatchViewAsSingleton.<T, T, T, BoundedWindow>applyForSingleton(
          runner,
          input,
          new IsmRecordForSingularValuePerWindowDoFn<T, BoundedWindow>(windowCoder),
          transform.hasDefaultValue(),
          transform.defaultValue(),
          input.getCoder());
    }

    static <T, FinalT, ViewT, W extends BoundedWindow> PCollectionView<ViewT>
    applyForSingleton(
        DataflowRunner runner,
        PCollection<T> input,
        DoFn<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>,
            IsmRecord<WindowedValue<FinalT>>> doFn,
        boolean hasDefault,
        FinalT defaultValue,
        Coder<FinalT> defaultValueCoder) {

      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      @SuppressWarnings({"rawtypes", "unchecked"})
      PCollectionView<ViewT> view =
          (PCollectionView<ViewT>) PCollectionViews.<FinalT, W>singletonView(
              (PCollection) input,
              (WindowingStrategy) input.getWindowingStrategy(),
              hasDefault,
              defaultValue,
              defaultValueCoder);

      IsmRecordCoder<WindowedValue<FinalT>> ismCoder =
          coderForSingleton(windowCoder, defaultValueCoder);

      PCollection<IsmRecord<WindowedValue<FinalT>>> reifiedPerWindowAndSorted = input
          .apply(new GroupByWindowHashAsKeyAndWindowAsSortKey<T, W>(ismCoder))
          .apply(ParDo.of(doFn));
      reifiedPerWindowAndSorted.setCoder(ismCoder);

      runner.addPCollectionRequiringIndexedFormat(reifiedPerWindowAndSorted);
      return reifiedPerWindowAndSorted.apply(
          CreateDataflowView.<IsmRecord<WindowedValue<FinalT>>, ViewT>of(view));
    }

    @Override
    protected String getKindString() {
      return "BatchViewAsSingleton";
    }

    static <T> IsmRecordCoder<WindowedValue<T>> coderForSingleton(
        Coder<? extends BoundedWindow> windowCoder, Coder<T> valueCoder) {
      return IsmRecordCoder.of(
          1, // We hash using only the window
          0, // There are no metadata records
          ImmutableList.<Coder<?>>of(windowCoder),
          FullWindowedValueCoder.of(valueCoder, windowCoder));
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsList View.AsList} for the
   * Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the window's byte representation
   * and with records having:
   * <ul>
   *   <li>Key 1: Window</li>
   *   <li>Key 2: Index offset within window</li>
   *   <li>Value: Windowed value</li>
   * </ul>
   */
  static class BatchViewAsList<T>
      extends PTransform<PCollection<T>, PCollectionView<List<T>>> {
    /**
     * A {@link DoFn} which creates {@link IsmRecord}s assuming that each element is within the
     * global window. Each {@link IsmRecord} has
     * <ul>
     *   <li>Key 1: Global window</li>
     *   <li>Key 2: Index offset within window</li>
     *   <li>Value: Windowed value</li>
     * </ul>
     */
    @SystemDoFnInternal
    static class ToIsmRecordForGlobalWindowDoFn<T>
        extends DoFn<T, IsmRecord<WindowedValue<T>>> {

      long indexInBundle;
      @StartBundle
      public void startBundle(Context c) throws Exception {
        indexInBundle = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        c.output(IsmRecord.of(
            ImmutableList.of(GlobalWindow.INSTANCE, indexInBundle),
            WindowedValue.of(
                c.element(),
                c.timestamp(),
                GlobalWindow.INSTANCE,
                c.pane())));
        indexInBundle += 1;
      }
    }

    /**
     * A {@link DoFn} which creates {@link IsmRecord}s comparing successive elements windows
     * to locate the window boundaries. The {@link IsmRecord} has:
     * <ul>
     *   <li>Key 1: Window</li>
     *   <li>Key 2: Index offset within window</li>
     *   <li>Value: Windowed value</li>
     * </ul>
     */
    @SystemDoFnInternal
    static class ToIsmRecordForNonGlobalWindowDoFn<T, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>,
        IsmRecord<WindowedValue<T>>> {

      private final Coder<W> windowCoder;
      ToIsmRecordForNonGlobalWindowDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        long elementsInWindow = 0;
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        for (KV<W, WindowedValue<T>> value : c.element().getValue()) {
          Object currentWindowStructuralValue = windowCoder.structuralValue(value.getKey());
          // Compare to see if this is a new window so we can reset the index counter i
          if (previousWindowStructuralValue.isPresent()
              && !previousWindowStructuralValue.get().equals(currentWindowStructuralValue)) {
            // Reset i since we have a new window.
            elementsInWindow = 0;
          }
          c.output(IsmRecord.of(
              ImmutableList.of(value.getKey(), elementsInWindow),
              value.getValue()));
          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          elementsInWindow += 1;
        }
      }
    }

    private final DataflowRunner runner;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsList(DataflowRunner runner, View.AsList<T> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<List<T>> expand(PCollection<T> input) {
      PCollectionView<List<T>> view = PCollectionViews.listView(
          input, input.getWindowingStrategy(), input.getCoder());
      return applyForIterableLike(runner, input, view);
    }

    static <T, W extends BoundedWindow, ViewT> PCollectionView<ViewT> applyForIterableLike(
        DataflowRunner runner,
        PCollection<T> input,
        PCollectionView<ViewT> view) {

      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();

      IsmRecordCoder<WindowedValue<T>> ismCoder = coderForListLike(windowCoder, input.getCoder());

      // If we are working in the global window, we do not need to do a GBK using the window
      // as the key since all the elements of the input PCollection are already such.
      // We just reify the windowed value while converting them to IsmRecords and generating
      // an index based upon where we are within the bundle. Each bundle
      // maps to one file exactly.
      if (input.getWindowingStrategy().getWindowFn() instanceof GlobalWindows) {
        PCollection<IsmRecord<WindowedValue<T>>> reifiedPerWindowAndSorted =
            input.apply(ParDo.of(new ToIsmRecordForGlobalWindowDoFn<T>()));
        reifiedPerWindowAndSorted.setCoder(ismCoder);

        runner.addPCollectionRequiringIndexedFormat(reifiedPerWindowAndSorted);
        return reifiedPerWindowAndSorted.apply(
            CreateDataflowView.<IsmRecord<WindowedValue<T>>, ViewT>of(view));
      }

      PCollection<IsmRecord<WindowedValue<T>>> reifiedPerWindowAndSorted = input
          .apply(new GroupByWindowHashAsKeyAndWindowAsSortKey<T, W>(ismCoder))
          .apply(ParDo.of(new ToIsmRecordForNonGlobalWindowDoFn<T, W>(windowCoder)));
      reifiedPerWindowAndSorted.setCoder(ismCoder);

      runner.addPCollectionRequiringIndexedFormat(reifiedPerWindowAndSorted);
      return reifiedPerWindowAndSorted.apply(
          CreateDataflowView.<IsmRecord<WindowedValue<T>>, ViewT>of(view));
    }

    @Override
    protected String getKindString() {
      return "BatchViewAsList";
    }

    static <T> IsmRecordCoder<WindowedValue<T>> coderForListLike(
        Coder<? extends BoundedWindow> windowCoder, Coder<T> valueCoder) {
      // TODO: swap to use a variable length long coder which has values which compare
      // the same as their byte representation compare lexicographically within the key coder
      return IsmRecordCoder.of(
          1, // We hash using only the window
          0, // There are no metadata records
          ImmutableList.of(windowCoder, BigEndianLongCoder.of()),
          FullWindowedValueCoder.of(valueCoder, windowCoder));
    }
  }

  /**
   * Specialized implementation for
   * {@link org.apache.beam.sdk.transforms.View.AsIterable View.AsIterable} for the
   * Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the windows byte representation
   * and with records having:
   * <ul>
   *   <li>Key 1: Window</li>
   *   <li>Key 2: Index offset within window</li>
   *   <li>Value: Windowed value</li>
   * </ul>
   */
  static class BatchViewAsIterable<T>
      extends PTransform<PCollection<T>, PCollectionView<Iterable<T>>> {

    private final DataflowRunner runner;
    /**
     * Builds an instance of this class from the overridden transform.
     */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsIterable(DataflowRunner runner, View.AsIterable<T> transform) {
      this.runner = runner;
    }

    @Override
    public PCollectionView<Iterable<T>> expand(PCollection<T> input) {
      PCollectionView<Iterable<T>> view = PCollectionViews.iterableView(
          input, input.getWindowingStrategy(), input.getCoder());
      return BatchViewAsList.applyForIterableLike(runner, input, view);
    }
  }


  /**
   * A {@link Function} which converts {@code WindowedValue<V>} to {@code V}.
   */
  private static class WindowedValueToValue<V> implements
      Function<WindowedValue<V>, V>, Serializable {
    private static final WindowedValueToValue<?> INSTANCE = new WindowedValueToValue<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <V> WindowedValueToValue<V> of() {
      return (WindowedValueToValue) INSTANCE;
    }

    @Override
    public V apply(WindowedValue<V> input) {
      return input.getValue();
    }
  }

  /**
   * A {@link Function} which converts {@code Iterable<WindowedValue<V>>} to {@code Iterable<V>}.
   */
  private static class IterableWithWindowedValuesToIterable<V> implements
      Function<Iterable<WindowedValue<V>>, Iterable<V>>, Serializable {
    private static final IterableWithWindowedValuesToIterable<?> INSTANCE =
        new IterableWithWindowedValuesToIterable<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <V> IterableWithWindowedValuesToIterable<V> of() {
      return (IterableWithWindowedValuesToIterable) INSTANCE;
    }

    @Override
    public Iterable<V> apply(Iterable<WindowedValue<V>> input) {
      return Iterables.transform(input, WindowedValueToValue.<V>of());
    }
  }

  /**
   * A {@link PTransform} that groups the values by a hash of the window's byte representation
   * and sorts the values using the windows byte representation.
   */
  private static class GroupByWindowHashAsKeyAndWindowAsSortKey<T, W extends BoundedWindow> extends
      PTransform<PCollection<T>, PCollection<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>>> {

    /**
     * A {@link DoFn} that for each element outputs a {@code KV} structure suitable for
     * grouping by the hash of the window's byte representation and sorting the grouped values
     * using the window's byte representation.
     */
    @SystemDoFnInternal
    private static class UseWindowHashAsKeyAndWindowAsSortKeyDoFn<T, W extends BoundedWindow>
        extends DoFn<T, KV<Integer, KV<W, WindowedValue<T>>>> {

      private final IsmRecordCoder<?> ismCoderForHash;
      private UseWindowHashAsKeyAndWindowAsSortKeyDoFn(IsmRecordCoder<?> ismCoderForHash) {
        this.ismCoderForHash = ismCoderForHash;
      }

      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow untypedWindow) throws Exception {
        @SuppressWarnings("unchecked")
        W window = (W) untypedWindow;
        c.output(
            KV.of(ismCoderForHash.hash(ImmutableList.of(window)),
                KV.of(window,
                    WindowedValue.of(
                        c.element(),
                        c.timestamp(),
                        window,
                        c.pane()))));
      }
    }

    private final IsmRecordCoder<?> ismCoderForHash;
    private GroupByWindowHashAsKeyAndWindowAsSortKey(IsmRecordCoder<?> ismCoderForHash) {
      this.ismCoderForHash = ismCoderForHash;
    }

    @Override
    public PCollection<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>> expand(
        PCollection<T> input) {
      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>)
          input.getWindowingStrategy().getWindowFn().windowCoder();
      PCollection<KV<Integer, KV<W, WindowedValue<T>>>> rval =
          input.apply(ParDo.of(
              new UseWindowHashAsKeyAndWindowAsSortKeyDoFn<T, W>(ismCoderForHash)));
      rval.setCoder(
          KvCoder.of(
              VarIntCoder.of(),
              KvCoder.of(windowCoder,
                  FullWindowedValueCoder.of(input.getCoder(), windowCoder))));
      return rval.apply(new GroupByKeyAndSortValuesOnly<Integer, W, WindowedValue<T>>());
    }
  }

  /**
   * A {@link GroupByKey} transform for the {@link DataflowRunner} which sorts
   * values using the secondary key {@code K2}.
   *
   * <p>The {@link PCollection} created created by this {@link PTransform} will have values in
   * the empty window. Care must be taken *afterwards* to either re-window
   * (using {@link Window#into}) or only use {@link PTransform}s that do not depend on the
   * values being within a window.
   */
  static class GroupByKeyAndSortValuesOnly<K1, K2, V>
      extends PTransform<PCollection<KV<K1, KV<K2, V>>>, PCollection<KV<K1, Iterable<KV<K2, V>>>>> {
    GroupByKeyAndSortValuesOnly() {
    }

    @Override
    public PCollection<KV<K1, Iterable<KV<K2, V>>>> expand(PCollection<KV<K1, KV<K2, V>>> input) {
      PCollection<KV<K1, Iterable<KV<K2, V>>>> rval =
          PCollection.<KV<K1, Iterable<KV<K2, V>>>>createPrimitiveOutputInternal(
              input.getPipeline(),
              WindowingStrategy.globalDefault(),
              IsBounded.BOUNDED);

      @SuppressWarnings({"unchecked", "rawtypes"})
      KvCoder<K1, KV<K2, V>> inputCoder = (KvCoder) input.getCoder();
      rval.setCoder(
          KvCoder.of(inputCoder.getKeyCoder(),
              IterableCoder.of(inputCoder.getValueCoder())));
      return rval;
    }
  }


  /**
   * A {@code Map<K, V2>} backed by a {@code Map<K, V1>} and a function that transforms
   * {@code V1 -> V2}.
   */
  static class TransformedMap<K, V1, V2>
      extends ForwardingMap<K, V2> {
    private final Function<V1, V2> transform;
    private final Map<K, V1> originalMap;
    private final Map<K, V2> transformedMap;

    TransformedMap(Function<V1, V2> transform, Map<K, V1> originalMap) {
      this.transform = transform;
      this.originalMap = Collections.unmodifiableMap(originalMap);
      this.transformedMap = Maps.transformValues(originalMap, transform);
    }

    @Override
    protected Map<K, V2> delegate() {
      return transformedMap;
    }
  }

  /**
   * A {@link Coder} for {@link TransformedMap}s.
   */
  static class TransformedMapCoder<K, V1, V2>
      extends StandardCoder<TransformedMap<K, V1, V2>> {
    private final Coder<Function<V1, V2>> transformCoder;
    private final Coder<Map<K, V1>> originalMapCoder;

    private TransformedMapCoder(
        Coder<Function<V1, V2>> transformCoder, Coder<Map<K, V1>> originalMapCoder) {
      this.transformCoder = transformCoder;
      this.originalMapCoder = originalMapCoder;
    }

    public static <K, V1, V2> TransformedMapCoder<K, V1, V2> of(
        Coder<Function<V1, V2>> transformCoder, Coder<Map<K, V1>> originalMapCoder) {
      return new TransformedMapCoder<>(transformCoder, originalMapCoder);
    }

    @JsonCreator
    public static <K, V1, V2> TransformedMapCoder<K, V1, V2> of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
            List<Coder<?>> components) {
      checkArgument(components.size() == 2,
          "Expecting 2 components, got " + components.size());
      @SuppressWarnings("unchecked")
      Coder<Function<V1, V2>> transformCoder = (Coder<Function<V1, V2>>) components.get(0);
      @SuppressWarnings("unchecked")
      Coder<Map<K, V1>> originalMapCoder = (Coder<Map<K, V1>>) components.get(1);
      return of(transformCoder, originalMapCoder);
    }

    @Override
    public void encode(TransformedMap<K, V1, V2> value, OutputStream outStream,
        Coder.Context context) throws CoderException, IOException {
      transformCoder.encode(value.transform, outStream, context.nested());
      originalMapCoder.encode(value.originalMap, outStream, context);
    }

    @Override
    public TransformedMap<K, V1, V2> decode(
        InputStream inStream, Coder.Context context) throws CoderException, IOException {
      return new TransformedMap<>(
          transformCoder.decode(inStream, context.nested()),
          originalMapCoder.decode(inStream, context));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(transformCoder, originalMapCoder);
    }

    @Override
    public void verifyDeterministic()
        throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
      verifyDeterministic("Expected transform coder to be deterministic.", transformCoder);
      verifyDeterministic("Expected map coder to be deterministic.", originalMapCoder);
    }
  }

  static class BatchCombineGloballyAsSingletonViewFactory<ElemT, ViewT>
      extends SingleInputOutputOverrideFactory<
          PCollection<ElemT>, PCollectionView<ViewT>,
          Combine.GloballyAsSingletonView<ElemT, ViewT>> {
    private final DataflowRunner runner;

    BatchCombineGloballyAsSingletonViewFactory(DataflowRunner runner) {
      this.runner = runner;
    }

    @Override
    public PTransformReplacement<PCollection<ElemT>, PCollectionView<ViewT>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<ElemT>, PCollectionView<ViewT>,
                    GloballyAsSingletonView<ElemT, ViewT>>
                transform) {
      GloballyAsSingletonView<ElemT, ViewT> combine = transform.getTransform();
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new BatchCombineGloballyAsSingletonView<>(
              runner, combine.getCombineFn(), combine.getFanout(), combine.getInsertDefault()));
    }

    private static class BatchCombineGloballyAsSingletonView<ElemT, ViewT>
        extends PTransform<PCollection<ElemT>, PCollectionView<ViewT>> {
      private final DataflowRunner runner;
      private final GlobalCombineFn<? super ElemT, ?, ViewT> combineFn;
      private final int fanout;
      private final boolean insertDefault;

      BatchCombineGloballyAsSingletonView(
          DataflowRunner runner,
          GlobalCombineFn<? super ElemT, ?, ViewT> combineFn,
          int fanout,
          boolean insertDefault) {
        this.runner = runner;
        this.combineFn = combineFn;
        this.fanout = fanout;
        this.insertDefault = insertDefault;
      }

      @Override
      public PCollectionView<ViewT> expand(PCollection<ElemT> input) {
        PCollection<ViewT> combined =
            input.apply(Combine.globally(combineFn).withoutDefaults().withFanout(fanout));
        AsSingleton<ViewT> viewAsSingleton = View.asSingleton();
        if (insertDefault) {
          viewAsSingleton.withDefaultValue(combineFn.defaultValue());
        }
        return combined.apply(new BatchViewAsSingleton<>(runner, viewAsSingleton));
      }
    }
  }
}
