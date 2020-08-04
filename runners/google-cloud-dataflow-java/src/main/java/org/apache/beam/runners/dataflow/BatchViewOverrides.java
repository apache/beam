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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
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
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ForwardingMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Dataflow batch overrides for {@link CreatePCollectionView}, specialized for different view types.
 */
class BatchViewOverrides {
  /**
   * Specialized implementation for {@link org.apache.beam.sdk.transforms.View.AsMap View.AsMap} for
   * the Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the key's byte representation.
   * Each record is structured as follows:
   *
   * <ul>
   *   <li>Key 1: User key K
   *   <li>Key 2: Window
   *   <li>Key 3: 0L (constant)
   *   <li>Value: Windowed value
   * </ul>
   *
   * <p>Alongside the data records, there are the following metadata records:
   *
   * <ul>
   *   <li>Key 1: Metadata Key
   *   <li>Key 2: Window
   *   <li>Key 3: Index [0, size of map]
   *   <li>Value: variable length long byte representation of size of map if index is 0, otherwise
   *       the byte representation of a key
   * </ul>
   *
   * <p>The {@code [META, Window, 0]} record stores the number of unique keys per window, while
   * {@code [META, Window, i]} for {@code i} in {@code [1, size of map]} stores a the users key.
   * This allows for one to access the size of the map by looking at {@code [META, Window, 0]} and
   * iterate over all the keys by accessing {@code [META, Window, i]} for {@code i} in {@code [1,
   * size of map]}.
   *
   * <p>Note that in the case of a non-deterministic key coder, we fallback to using {@link
   * org.apache.beam.sdk.transforms.View.AsSingleton View.AsSingleton} printing a warning to users
   * to specify a deterministic key coder.
   */
  static class BatchViewAsMap<K, V> extends PTransform<PCollection<KV<K, V>>, PCollection<?>> {

    /**
     * A {@link DoFn} which groups elements by window boundaries. For each group, the group of
     * elements is transformed into a {@link TransformedMap}. The transformed {@code Map<K, V>} is
     * backed by a {@code Map<K, WindowedValue<V>>} and contains a function {@code WindowedValue<V>
     * -> V}.
     *
     * <p>Outputs {@link IsmRecord}s having:
     *
     * <ul>
     *   <li>Key 1: Window
     *   <li>Value: Transformed map containing a transform that removes the encapsulation of the
     *       window around each value, {@code Map<K, WindowedValue<V>> -> Map<K, V>}.
     * </ul>
     */
    static class ToMapDoFn<K, V, W extends BoundedWindow>
        extends DoFn<
            KV<Integer, Iterable<KV<W, WindowedValue<KV<K, V>>>>>,
            IsmRecord<WindowedValue<TransformedMap<K, WindowedValue<V>, V>>>> {

      private final Coder<W> windowCoder;

      ToMapDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        Optional<W> previousWindow = Optional.absent();
        Map<K, WindowedValue<V>> map = new HashMap<>();
        for (KV<W, WindowedValue<KV<K, V>>> kv : c.element().getValue()) {
          Object currentWindowStructuralValue = windowCoder.structuralValue(kv.getKey());
          if (previousWindowStructuralValue.isPresent()
              && !previousWindowStructuralValue.get().equals(currentWindowStructuralValue)) {
            // Construct the transformed map containing all the elements since we
            // are at a window boundary.
            c.output(
                IsmRecord.of(
                    ImmutableList.of(previousWindow.get()),
                    valueInEmptyWindows(new TransformedMap<>(WindowedValueToValue.of(), map))));
            map = new HashMap<>();
          }

          // Verify that the user isn't trying to insert the same key multiple times.
          checkState(
              !map.containsKey(kv.getValue().getValue().getKey()),
              "Multiple values [%s, %s] found for single key [%s] within window [%s].",
              map.get(kv.getValue().getValue().getKey()),
              kv.getValue().getValue().getValue(),
              kv.getKey());
          map.put(
              kv.getValue().getValue().getKey(),
              kv.getValue().withValue(kv.getValue().getValue().getValue()));
          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          previousWindow = Optional.of(kv.getKey());
        }

        // The last value for this hash is guaranteed to be at a window boundary
        // so we output a transformed map containing all the elements since the last
        // window boundary.
        c.output(
            IsmRecord.of(
                ImmutableList.of(previousWindow.get()),
                valueInEmptyWindows(new TransformedMap<>(WindowedValueToValue.of(), map))));
      }
    }

    private final transient DataflowRunner runner;
    private final PCollectionView<Map<K, V>> view;
    /** Builds an instance of this class from the overridden transform. */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsMap(
        DataflowRunner runner, CreatePCollectionView<KV<K, V>, Map<K, V>> transform) {
      this.runner = runner;
      this.view = transform.getView();
    }

    @Override
    public PCollection<?> expand(PCollection<KV<K, V>> input) {
      return this.applyInternal(input);
    }

    private <W extends BoundedWindow> PCollection<?> applyInternal(PCollection<KV<K, V>> input) {
      try {
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
    private <W extends BoundedWindow> PCollection<?> applyForSingletonFallback(
        PCollection<KV<K, V>> input) {
      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>) input.getWindowingStrategy().getWindowFn().windowCoder();

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

      return BatchViewAsSingleton.applyForSingleton(
          runner, input, new ToMapDoFn<>(windowCoder), finalValueCoder, view);
    }
  }

  /**
   * Specialized implementation for {@link org.apache.beam.sdk.transforms.View.AsMultimap
   * View.AsMultimap} for the Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the key's byte representation.
   * Each record is structured as follows:
   *
   * <ul>
   *   <li>Key 1: User key K
   *   <li>Key 2: Window
   *   <li>Key 3: Index offset for a given key and window.
   *   <li>Value: Windowed value
   * </ul>
   *
   * <p>Alongside the data records, there are the following metadata records:
   *
   * <ul>
   *   <li>Key 1: Metadata Key
   *   <li>Key 2: Window
   *   <li>Key 3: Index [0, size of map]
   *   <li>Value: variable length long byte representation of size of map if index is 0, otherwise
   *       the byte representation of a key
   * </ul>
   *
   * <p>The {@code [META, Window, 0]} record stores the number of unique keys per window, while
   * {@code [META, Window, i]} for {@code i} in {@code [1, size of map]} stores a the users key.
   * This allows for one to access the size of the map by looking at {@code [META, Window, 0]} and
   * iterate over all the keys by accessing {@code [META, Window, i]} for {@code i} in {@code [1,
   * size of map]}.
   *
   * <p>Note that in the case of a non-deterministic key coder, we fallback to using {@link
   * org.apache.beam.sdk.transforms.View.AsSingleton View.AsSingleton} printing a warning to users
   * to specify a deterministic key coder.
   */
  static class BatchViewAsMultimap<K, V> extends PTransform<PCollection<KV<K, V>>, PCollection<?>> {
    /**
     * A {@link PTransform} that groups elements by the hash of window's byte representation if the
     * input {@link PCollection} is not within the global window. Otherwise by the hash of the
     * window and key's byte representation. This {@link PTransform} also sorts the values by the
     * combination of the window and key's byte representations.
     */
    private static class GroupByKeyHashAndSortByKeyAndWindow<K, V, W extends BoundedWindow>
        extends PTransform<
            PCollection<KV<K, V>>,
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
              KV.of(
                  coder.hash(ImmutableList.of(c.element().getKey())),
                  KV.of(
                      KV.of(c.element().getKey(), window),
                      WindowedValue.of(
                          c.element().getValue(), c.timestamp(), untypedWindow, c.pane()))));
        }
      }

      private final IsmRecordCoder<?> coder;

      public GroupByKeyHashAndSortByKeyAndWindow(IsmRecordCoder<?> coder) {
        this.coder = coder;
      }

      @Override
      public PCollection<KV<Integer, Iterable<KV<KV<K, W>, WindowedValue<V>>>>> expand(
          PCollection<KV<K, V>> input) {

        @SuppressWarnings("unchecked")
        Coder<W> windowCoder = (Coder<W>) input.getWindowingStrategy().getWindowFn().windowCoder();
        @SuppressWarnings("unchecked")
        KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();

        PCollection<KV<Integer, KV<KV<K, W>, WindowedValue<V>>>> keyedByHash;
        keyedByHash =
            input.apply(ParDo.of(new GroupByKeyHashAndSortByKeyAndWindowDoFn<K, V, W>(coder)));
        keyedByHash.setCoder(
            KvCoder.of(
                VarIntCoder.of(),
                KvCoder.of(
                    KvCoder.of(inputCoder.getKeyCoder(), windowCoder),
                    FullWindowedValueCoder.of(inputCoder.getValueCoder(), windowCoder))));

        return keyedByHash.apply(new GroupByKeyAndSortValuesOnly<>());
      }
    }

    /**
     * A {@link DoFn} which creates {@link IsmRecord}s comparing successive elements windows and
     * keys to locate window and key boundaries. The main output {@link IsmRecord}s have:
     *
     * <ul>
     *   <li>Key 1: Window
     *   <li>Key 2: User key K
     *   <li>Key 3: Index offset for a given key and window.
     *   <li>Value: Windowed value
     * </ul>
     *
     * <p>Additionally, we output all the unique keys per window seen to {@code outputForEntrySet}
     * and the unique key count per window to {@code outputForSize}.
     *
     * <p>Finally, if this {@link DoFn} has been requested to perform unique key checking, it will
     * throw an {@link IllegalStateException} if more than one key per window is found.
     */
    static class ToIsmRecordForMapLikeDoFn<K, V, W extends BoundedWindow>
        extends DoFn<
            KV<Integer, Iterable<KV<KV<K, W>, WindowedValue<V>>>>, IsmRecord<WindowedValue<V>>> {

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
        Object currentKeyStructuralValue = keyCoder.structuralValue(currentValue.getKey().getKey());
        Object currentWindowStructuralValue =
            windowCoder.structuralValue(currentValue.getKey().getValue());

        while (iterator.hasNext()) {
          KV<KV<K, W>, WindowedValue<V>> nextValue = iterator.next();
          Object nextKeyStructuralValue = keyCoder.structuralValue(nextValue.getKey().getKey());
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
          } else if (!currentKeyStructuralValue.equals(nextKeyStructuralValue)) {
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
            throw new IllegalStateException(
                String.format(
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
        IsmRecord<WindowedValue<V>> ismRecord =
            IsmRecord.of(
                ImmutableList.of(value.getKey().getKey(), value.getKey().getValue(), keyIndex),
                value.getValue());
        c.output(ismRecord);
      }

      /**
       * This outputs records which will be used to compute the number of keys for a given window.
       */
      private void outputMetadataRecordForSize(
          ProcessContext c, KV<KV<K, W>, WindowedValue<V>> value, long uniqueKeyCount) {
        c.output(
            outputForSize,
            KV.of(
                ismCoder.hash(
                    ImmutableList.of(IsmFormat.getMetadataKey(), value.getKey().getValue())),
                KV.of(value.getKey().getValue(), uniqueKeyCount)));
      }

      /** This outputs records which will be used to construct the entry set. */
      private void outputMetadataRecordForEntrySet(
          ProcessContext c, KV<KV<K, W>, WindowedValue<V>> value) {
        c.output(
            outputForEntrySet,
            KV.of(
                ismCoder.hash(
                    ImmutableList.of(IsmFormat.getMetadataKey(), value.getKey().getValue())),
                KV.of(value.getKey().getValue(), value.getKey().getKey())));
      }
    }

    /**
     * A {@link DoFn} which outputs a metadata {@link IsmRecord} per window of:
     *
     * <ul>
     *   <li>Key 1: META key
     *   <li>Key 2: window
     *   <li>Key 3: 0L (constant)
     *   <li>Value: sum of values for window
     * </ul>
     *
     * <p>This {@link DoFn} is meant to be used to compute the number of unique keys per window for
     * map and multimap side inputs.
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
            c.output(
                IsmRecord.meta(
                    ImmutableList.of(IsmFormat.getMetadataKey(), currentValue.getKey(), 0L),
                    CoderUtils.encodeToByteArray(VarLongCoder.of(), size)));
            size = 0;
          }

          currentValue = nextValue;
          currentWindowStructuralValue = nextWindowStructuralValue;
        }

        size += currentValue.getValue();
        // Output the final value since it is guaranteed to be on a window boundary.
        c.output(
            IsmRecord.meta(
                ImmutableList.of(IsmFormat.getMetadataKey(), currentValue.getKey(), 0L),
                CoderUtils.encodeToByteArray(VarLongCoder.of(), size)));
      }
    }

    /**
     * A {@link DoFn} which outputs a metadata {@link IsmRecord} per window and key pair of:
     *
     * <ul>
     *   <li>Key 1: META key
     *   <li>Key 2: window
     *   <li>Key 3: index offset (1-based index)
     *   <li>Value: key
     * </ul>
     *
     * <p>This {@link DoFn} is meant to be used to output index to key records per window for map
     * and multimap side inputs.
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

          c.output(
              IsmRecord.meta(
                  ImmutableList.of(
                      IsmFormat.getMetadataKey(), currentValue.getKey(), elementsInWindow),
                  CoderUtils.encodeToByteArray(keyCoder, currentValue.getValue())));
          elementsInWindow += 1;

          if (!currentWindowStructuralValue.equals(nextWindowStructuralValue)) {
            elementsInWindow = 1;
          }

          currentValue = nextValue;
          currentWindowStructuralValue = nextWindowStructuralValue;
        }

        // Output the final value since it is guaranteed to be on a window boundary.
        c.output(
            IsmRecord.meta(
                ImmutableList.of(
                    IsmFormat.getMetadataKey(), currentValue.getKey(), elementsInWindow),
                CoderUtils.encodeToByteArray(keyCoder, currentValue.getValue())));
      }
    }

    /**
     * A {@link DoFn} which partitions sets of elements by window boundaries. Within each partition,
     * the set of elements is transformed into a {@link TransformedMap}. The transformed {@code
     * Map<K, Iterable<V>>} is backed by a {@code Map<K, Iterable<WindowedValue<V>>>} and contains a
     * function {@code Iterable<WindowedValue<V>> -> Iterable<V>}.
     *
     * <p>Outputs {@link IsmRecord}s having:
     *
     * <ul>
     *   <li>Key 1: Window
     *   <li>Value: Transformed map containing a transform that removes the encapsulation of the
     *       window around each value, {@code Map<K, Iterable<WindowedValue<V>>> -> Map<K,
     *       Iterable<V>>}.
     * </ul>
     */
    static class ToMultimapDoFn<K, V, W extends BoundedWindow>
        extends DoFn<
            KV<Integer, Iterable<KV<W, WindowedValue<KV<K, V>>>>>,
            IsmRecord<WindowedValue<TransformedMap<K, Iterable<WindowedValue<V>>, Iterable<V>>>>> {

      private final Coder<W> windowCoder;

      ToMultimapDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        Optional<W> previousWindow = Optional.absent();
        Multimap<K, WindowedValue<V>> multimap = ArrayListMultimap.create();
        for (KV<W, WindowedValue<KV<K, V>>> kv : c.element().getValue()) {
          Object currentWindowStructuralValue = windowCoder.structuralValue(kv.getKey());
          if (previousWindowStructuralValue.isPresent()
              && !previousWindowStructuralValue.get().equals(currentWindowStructuralValue)) {
            // Construct the transformed map containing all the elements since we
            // are at a window boundary.
            @SuppressWarnings({"unchecked", "rawtypes"})
            Map<K, Iterable<WindowedValue<V>>> resultMap = (Map) multimap.asMap();
            c.output(
                IsmRecord.of(
                    ImmutableList.of(previousWindow.get()),
                    valueInEmptyWindows(
                        new TransformedMap<>(
                            IterableWithWindowedValuesToIterable.of(), resultMap))));
            multimap = ArrayListMultimap.create();
          }

          multimap.put(
              kv.getValue().getValue().getKey(),
              kv.getValue().withValue(kv.getValue().getValue().getValue()));
          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          previousWindow = Optional.of(kv.getKey());
        }

        // The last value for this hash is guaranteed to be at a window boundary
        // so we output a transformed map containing all the elements since the last
        // window boundary.
        @SuppressWarnings({"unchecked", "rawtypes"})
        Map<K, Iterable<WindowedValue<V>>> resultMap = (Map) multimap.asMap();
        c.output(
            IsmRecord.of(
                ImmutableList.of(previousWindow.get()),
                valueInEmptyWindows(
                    new TransformedMap<>(IterableWithWindowedValuesToIterable.of(), resultMap))));
      }
    }

    private final transient DataflowRunner runner;
    private final PCollectionView<Map<K, Iterable<V>>> view;
    /** Builds an instance of this class from the overridden transform. */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsMultimap(
        DataflowRunner runner, CreatePCollectionView<KV<K, V>, Map<K, Iterable<V>>> transform) {
      this.runner = runner;
      this.view = transform.getView();
    }

    @Override
    public PCollection<?> expand(PCollection<KV<K, V>> input) {
      return this.applyInternal(input);
    }

    private <W extends BoundedWindow> PCollection<?> applyInternal(PCollection<KV<K, V>> input) {
      try {
        return applyForMapLike(runner, input, view, false /* unique keys not expected */);
      } catch (NonDeterministicException e) {
        runner.recordViewUsesNonDeterministicKeyCoder(this);

        // Since the key coder is not deterministic, we convert the map into a singleton
        // and return a singleton view equivalent.
        return applyForSingletonFallback(input);
      }
    }

    /** Transforms the input {@link PCollection} into a singleton {@link Map} per window. */
    private <W extends BoundedWindow> PCollection<?> applyForSingletonFallback(
        PCollection<KV<K, V>> input) {
      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>) input.getWindowingStrategy().getWindowFn().windowCoder();

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

      return BatchViewAsSingleton.applyForSingleton(
          runner, input, new ToMultimapDoFn<>(windowCoder), finalValueCoder, view);
    }

    private static <K, V, W extends BoundedWindow, ViewT> PCollection<?> applyForMapLike(
        DataflowRunner runner,
        PCollection<KV<K, V>> input,
        PCollectionView<ViewT> view,
        boolean uniqueKeysExpected)
        throws NonDeterministicException {

      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>) input.getWindowingStrategy().getWindowFn().windowCoder();

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
      PCollectionTuple outputTuple =
          input
              .apply("GBKaSVForData", new GroupByKeyHashAndSortByKeyAndWindow<K, V, W>(ismCoder))
              .apply(
                  ParDo.of(
                          new ToIsmRecordForMapLikeDoFn<>(
                              outputForSizeTag,
                              outputForEntrySetTag,
                              windowCoder,
                              inputCoder.getKeyCoder(),
                              ismCoder,
                              uniqueKeysExpected))
                      .withOutputTags(
                          mainOutputTag,
                          TupleTagList.of(
                              ImmutableList.of(outputForSizeTag, outputForEntrySetTag))));

      // Set the coder on the main data output.
      PCollection<IsmRecord<WindowedValue<V>>> perHashWithReifiedWindows =
          outputTuple.get(mainOutputTag);
      perHashWithReifiedWindows.setCoder(ismCoder);

      // Set the coder on the metadata output for size and process the entries
      // producing a [META, Window, 0L] record per window storing the number of unique keys
      // for each window.
      PCollection<KV<Integer, KV<W, Long>>> outputForSize = outputTuple.get(outputForSizeTag);
      outputForSize.setCoder(
          KvCoder.of(VarIntCoder.of(), KvCoder.of(windowCoder, VarLongCoder.of())));
      PCollection<IsmRecord<WindowedValue<V>>> windowMapSizeMetadata =
          outputForSize
              .apply("GBKaSVForSize", new GroupByKeyAndSortValuesOnly<>())
              .apply(ParDo.of(new ToIsmMetadataRecordForSizeDoFn<K, V, W>(windowCoder)));
      windowMapSizeMetadata.setCoder(ismCoder);

      // Set the coder on the metadata output destined to build the entry set and process the
      // entries producing a [META, Window, Index] record per window key pair storing the key.
      PCollection<KV<Integer, KV<W, K>>> outputForEntrySet = outputTuple.get(outputForEntrySetTag);
      outputForEntrySet.setCoder(
          KvCoder.of(VarIntCoder.of(), KvCoder.of(windowCoder, inputCoder.getKeyCoder())));
      PCollection<IsmRecord<WindowedValue<V>>> windowMapKeysMetadata =
          outputForEntrySet
              .apply("GBKaSVForKeys", new GroupByKeyAndSortValuesOnly<>())
              .apply(
                  ParDo.of(
                      new ToIsmMetadataRecordForKeyDoFn<K, V, W>(
                          inputCoder.getKeyCoder(), windowCoder)));
      windowMapKeysMetadata.setCoder(ismCoder);

      // Set that all these outputs should be materialized using an indexed format.
      runner.addPCollectionRequiringIndexedFormat(perHashWithReifiedWindows);
      runner.addPCollectionRequiringIndexedFormat(windowMapSizeMetadata);
      runner.addPCollectionRequiringIndexedFormat(windowMapKeysMetadata);

      PCollectionList<IsmRecord<WindowedValue<V>>> outputs =
          PCollectionList.of(
              ImmutableList.of(
                  perHashWithReifiedWindows, windowMapSizeMetadata, windowMapKeysMetadata));

      PCollection<IsmRecord<WindowedValue<V>>> flattenedOutputs =
          Pipeline.applyTransform(outputs, Flatten.pCollections());
      flattenedOutputs.apply(CreateDataflowView.forBatch(view));
      return flattenedOutputs;
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
          ImmutableList.of(MetadataKeyCoder.of(keyCoder), windowCoder, BigEndianLongCoder.of()),
          FullWindowedValueCoder.of(valueCoder, windowCoder));
    }
  }

  /**
   * Specialized implementation for {@link org.apache.beam.sdk.transforms.View.AsSingleton
   * View.AsSingleton} for the Dataflow runner in batch mode.
   *
   * <p>Creates a set of files in the {@link IsmFormat} sharded by the hash of the windows byte
   * representation and with records having:
   *
   * <ul>
   *   <li>Key 1: Window
   *   <li>Value: Windowed value
   * </ul>
   */
  static class BatchViewAsSingleton<T> extends PTransform<PCollection<T>, PCollection<?>> {

    /**
     * A {@link DoFn} that outputs {@link IsmRecord}s. These records are structured as follows:
     *
     * <ul>
     *   <li>Key 1: Window
     *   <li>Value: Windowed value
     * </ul>
     */
    static class IsmRecordForSingularValuePerWindowDoFn<T, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>, IsmRecord<WindowedValue<T>>> {

      private final Coder<W> windowCoder;

      IsmRecordForSingularValuePerWindowDoFn(Coder<W> windowCoder) {
        this.windowCoder = windowCoder;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        Optional<Object> previousWindowStructuralValue = Optional.absent();
        T previousValue = null;

        for (KV<W, WindowedValue<T>> next : c.element().getValue()) {
          Object currentWindowStructuralValue = windowCoder.structuralValue(next.getKey());

          // Verify that the user isn't trying to have more than one element per window as
          // a singleton.
          checkState(
              !previousWindowStructuralValue.isPresent()
                  || !previousWindowStructuralValue.get().equals(currentWindowStructuralValue),
              "Multiple values [%s, %s] found for singleton within window [%s].",
              previousValue,
              next.getValue().getValue(),
              next.getKey());

          c.output(IsmRecord.of(ImmutableList.of(next.getKey()), next.getValue()));

          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          previousValue = next.getValue().getValue();
        }
      }
    }

    private final transient DataflowRunner runner;
    private final PCollectionView<T> view;
    private final CombineFn<T, ?, T> combineFn;
    private final int fanout;

    public BatchViewAsSingleton(
        DataflowRunner runner,
        CreatePCollectionView<T, T> transform,
        CombineFn<T, ?, T> combineFn,
        int fanout) {
      this.runner = runner;
      this.view = transform.getView();
      this.combineFn = combineFn;
      this.fanout = fanout;
    }

    @Override
    public PCollection<?> expand(PCollection<T> input) {
      input = input.apply(Combine.globally(combineFn).withoutDefaults().withFanout(fanout));
      @SuppressWarnings("unchecked")
      Coder<BoundedWindow> windowCoder =
          (Coder<BoundedWindow>) input.getWindowingStrategy().getWindowFn().windowCoder();

      return BatchViewAsSingleton.applyForSingleton(
          runner,
          input,
          new IsmRecordForSingularValuePerWindowDoFn<>(windowCoder),
          input.getCoder(),
          view);
    }

    static <T, FinalT, ViewT, W extends BoundedWindow> PCollection<?> applyForSingleton(
        DataflowRunner runner,
        PCollection<T> input,
        DoFn<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>, IsmRecord<WindowedValue<FinalT>>> doFn,
        Coder<FinalT> defaultValueCoder,
        PCollectionView<ViewT> view) {

      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>) input.getWindowingStrategy().getWindowFn().windowCoder();

      IsmRecordCoder<WindowedValue<FinalT>> ismCoder =
          coderForSingleton(windowCoder, defaultValueCoder);

      PCollection<IsmRecord<WindowedValue<FinalT>>> reifiedPerWindowAndSorted =
          input
              .apply(new GroupByWindowHashAsKeyAndWindowAsSortKey<T, W>(ismCoder))
              .apply(ParDo.of(doFn));
      reifiedPerWindowAndSorted.setCoder(ismCoder);

      runner.addPCollectionRequiringIndexedFormat(reifiedPerWindowAndSorted);
      reifiedPerWindowAndSorted.apply(CreateDataflowView.forBatch(view));
      return reifiedPerWindowAndSorted;
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
          ImmutableList.of(windowCoder),
          FullWindowedValueCoder.of(valueCoder, windowCoder));
    }
  }

  /**
   * Specialized implementation for {@link org.apache.beam.sdk.transforms.View.AsList View.AsList}
   * for the Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the window's byte representation
   * and with records having:
   *
   * <ul>
   *   <li>Key 1: Window
   *   <li>Key 2: Index offset within window
   *   <li>Value: Windowed value
   * </ul>
   */
  static class BatchViewAsList<T> extends PTransform<PCollection<T>, PCollection<?>> {
    /**
     * A {@link DoFn} which creates {@link IsmRecord}s assuming that each element is within the
     * global window. Each {@link IsmRecord} has
     *
     * <ul>
     *   <li>Key 1: Global window
     *   <li>Key 2: Index offset within window
     *   <li>Value: Windowed value
     * </ul>
     */
    @SystemDoFnInternal
    static class ToIsmRecordForGlobalWindowDoFn<T> extends DoFn<T, IsmRecord<WindowedValue<T>>> {

      long indexInBundle;

      @StartBundle
      public void startBundle() throws Exception {
        indexInBundle = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        c.output(
            IsmRecord.of(
                ImmutableList.of(GlobalWindow.INSTANCE, indexInBundle),
                WindowedValue.of(c.element(), c.timestamp(), GlobalWindow.INSTANCE, c.pane())));
        indexInBundle += 1;
      }
    }

    /**
     * A {@link DoFn} which creates {@link IsmRecord}s comparing successive elements windows to
     * locate the window boundaries. The {@link IsmRecord} has:
     *
     * <ul>
     *   <li>Key 1: Window
     *   <li>Key 2: Index offset within window
     *   <li>Value: Windowed value
     * </ul>
     */
    @SystemDoFnInternal
    static class ToIsmRecordForNonGlobalWindowDoFn<T, W extends BoundedWindow>
        extends DoFn<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>, IsmRecord<WindowedValue<T>>> {

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
          c.output(
              IsmRecord.of(ImmutableList.of(value.getKey(), elementsInWindow), value.getValue()));
          previousWindowStructuralValue = Optional.of(currentWindowStructuralValue);
          elementsInWindow += 1;
        }
      }
    }

    private final transient DataflowRunner runner;
    private final PCollectionView<List<T>> view;
    /** Builds an instance of this class from the overridden transform. */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsList(DataflowRunner runner, CreatePCollectionView<T, List<T>> transform) {
      this.runner = runner;
      this.view = transform.getView();
    }

    @Override
    public PCollection<?> expand(PCollection<T> input) {
      return applyForIterableLike(runner, input, view);
    }

    static <T, W extends BoundedWindow, ViewT> PCollection<?> applyForIterableLike(
        DataflowRunner runner, PCollection<T> input, PCollectionView<ViewT> view) {

      @SuppressWarnings("unchecked")
      Coder<W> windowCoder = (Coder<W>) input.getWindowingStrategy().getWindowFn().windowCoder();

      IsmRecordCoder<WindowedValue<T>> ismCoder = coderForListLike(windowCoder, input.getCoder());

      // If we are working in the global window, we do not need to do a GBK using the window
      // as the key since all the elements of the input PCollection are already such.
      // We just reify the windowed value while converting them to IsmRecords and generating
      // an index based upon where we are within the bundle. Each bundle
      // maps to one file exactly.
      if (input.getWindowingStrategy().getWindowFn() instanceof GlobalWindows) {
        PCollection<IsmRecord<WindowedValue<T>>> reifiedPerWindowAndSorted =
            input.apply(ParDo.of(new ToIsmRecordForGlobalWindowDoFn<>()));
        reifiedPerWindowAndSorted.setCoder(ismCoder);

        runner.addPCollectionRequiringIndexedFormat(reifiedPerWindowAndSorted);
        reifiedPerWindowAndSorted.apply(CreateDataflowView.forBatch(view));
        return reifiedPerWindowAndSorted;
      }

      PCollection<IsmRecord<WindowedValue<T>>> reifiedPerWindowAndSorted =
          input
              .apply(new GroupByWindowHashAsKeyAndWindowAsSortKey<T, W>(ismCoder))
              .apply(ParDo.of(new ToIsmRecordForNonGlobalWindowDoFn<>(windowCoder)));
      reifiedPerWindowAndSorted.setCoder(ismCoder);

      runner.addPCollectionRequiringIndexedFormat(reifiedPerWindowAndSorted);
      reifiedPerWindowAndSorted.apply(CreateDataflowView.forBatch(view));
      return reifiedPerWindowAndSorted;
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
   * Specialized implementation for {@link org.apache.beam.sdk.transforms.View.AsIterable
   * View.AsIterable} for the Dataflow runner in batch mode.
   *
   * <p>Creates a set of {@code Ism} files sharded by the hash of the windows byte representation
   * and with records having:
   *
   * <ul>
   *   <li>Key 1: Window
   *   <li>Key 2: Index offset within window
   *   <li>Value: Windowed value
   * </ul>
   */
  static class BatchViewAsIterable<T> extends PTransform<PCollection<T>, PCollection<?>> {

    private final transient DataflowRunner runner;
    private final PCollectionView<Iterable<T>> view;
    /** Builds an instance of this class from the overridden transform. */
    @SuppressWarnings("unused") // used via reflection in DataflowRunner#apply()
    public BatchViewAsIterable(
        DataflowRunner runner, CreatePCollectionView<T, Iterable<T>> transform) {
      this.runner = runner;
      this.view = transform.getView();
    }

    @Override
    public PCollection<?> expand(PCollection<T> input) {
      return BatchViewAsList.applyForIterableLike(runner, input, view);
    }
  }

  /** A {@link Function} which converts {@code WindowedValue<V>} to {@code V}. */
  private static class WindowedValueToValue<V>
      implements Function<WindowedValue<V>, V>, Serializable {
    private static final WindowedValueToValue<?> INSTANCE = new WindowedValueToValue<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <V> WindowedValueToValue<V> of() {
      return (WindowedValueToValue) INSTANCE;
    }

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public V apply(@Nonnull WindowedValue<V> input) {
      return input.getValue();
    }
  }

  /**
   * A {@link Function} which converts {@code Iterable<WindowedValue<V>>} to {@code Iterable<V>}.
   */
  private static class IterableWithWindowedValuesToIterable<V>
      implements Function<Iterable<WindowedValue<V>>, Iterable<V>>, Serializable {
    private static final IterableWithWindowedValuesToIterable<?> INSTANCE =
        new IterableWithWindowedValuesToIterable<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <V> IterableWithWindowedValuesToIterable<V> of() {
      return (IterableWithWindowedValuesToIterable) INSTANCE;
    }

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public Iterable<V> apply(@Nonnull Iterable<WindowedValue<V>> input) {
      return Iterables.transform(input, WindowedValueToValue.of());
    }
  }

  /**
   * A {@link PTransform} that groups the values by a hash of the window's byte representation and
   * sorts the values using the windows byte representation.
   */
  private static class GroupByWindowHashAsKeyAndWindowAsSortKey<T, W extends BoundedWindow>
      extends PTransform<
          PCollection<T>, PCollection<KV<Integer, Iterable<KV<W, WindowedValue<T>>>>>> {

    /**
     * A {@link DoFn} that for each element outputs a {@code KV} structure suitable for grouping by
     * the hash of the window's byte representation and sorting the grouped values using the
     * window's byte representation.
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
            KV.of(
                ismCoderForHash.hash(ImmutableList.of(window)),
                KV.of(window, WindowedValue.of(c.element(), c.timestamp(), window, c.pane()))));
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
      Coder<W> windowCoder = (Coder<W>) input.getWindowingStrategy().getWindowFn().windowCoder();
      PCollection<KV<Integer, KV<W, WindowedValue<T>>>> rval =
          input.apply(
              ParDo.of(new UseWindowHashAsKeyAndWindowAsSortKeyDoFn<T, W>(ismCoderForHash)));
      rval.setCoder(
          KvCoder.of(
              VarIntCoder.of(),
              KvCoder.of(windowCoder, FullWindowedValueCoder.of(input.getCoder(), windowCoder))));
      return rval.apply(new GroupByKeyAndSortValuesOnly<>());
    }
  }

  /**
   * A {@link GroupByKey} transform for the {@link DataflowRunner} which sorts values using the
   * secondary key {@code K2}.
   *
   * <p>The {@link PCollection} created created by this {@link PTransform} will have values in the
   * empty window. Care must be taken *afterwards* to either re-window (using {@link Window#into})
   * or only use {@link PTransform}s that do not depend on the values being within a window.
   */
  static class GroupByKeyAndSortValuesOnly<K1, K2, V>
      extends PTransform<PCollection<KV<K1, KV<K2, V>>>, PCollection<KV<K1, Iterable<KV<K2, V>>>>> {
    GroupByKeyAndSortValuesOnly() {}

    @Override
    public PCollection<KV<K1, Iterable<KV<K2, V>>>> expand(PCollection<KV<K1, KV<K2, V>>> input) {
      @SuppressWarnings("unchecked")
      KvCoder<K1, KV<K2, V>> inputCoder = (KvCoder<K1, KV<K2, V>>) input.getCoder();
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          IsBounded.BOUNDED,
          KvCoder.of(inputCoder.getKeyCoder(), IterableCoder.of(inputCoder.getValueCoder())));
    }
  }

  /**
   * A {@code Map<K, V2>} backed by a {@code Map<K, V1>} and a function that transforms {@code V1 ->
   * V2}.
   */
  static class TransformedMap<K, V1, V2> extends ForwardingMap<K, V2> {
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

  /** A {@link Coder} for {@link TransformedMap}s. */
  static class TransformedMapCoder<K, V1, V2> extends StructuredCoder<TransformedMap<K, V1, V2>> {
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

    @Override
    public void encode(TransformedMap<K, V1, V2> value, OutputStream outStream)
        throws CoderException, IOException {
      transformCoder.encode(value.transform, outStream);
      originalMapCoder.encode(value.originalMap, outStream);
    }

    @Override
    public TransformedMap<K, V1, V2> decode(InputStream inStream)
        throws CoderException, IOException {
      return new TransformedMap<>(
          transformCoder.decode(inStream), originalMapCoder.decode(inStream));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(transformCoder, originalMapCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(this, "Expected transform coder to be deterministic.", transformCoder);
      verifyDeterministic(this, "Expected map coder to be deterministic.", originalMapCoder);
    }
  }

  /**
   * A hack to put a simple value (aka globally windowed) in a place where a WindowedValue is
   * expected.
   *
   * <p>This is not actually valid for Beam elements, because values in no windows do not really
   * exist and may be dropped at any time without further justification.
   */
  private static <T> WindowedValue<T> valueInEmptyWindows(T value) {
    return new ValueInEmptyWindows<>(value);
  }

  private static class ValueInEmptyWindows<T> extends WindowedValue<T> {

    private final T value;

    private ValueInEmptyWindows(T value) {
      this.value = value;
    }

    @Override
    public <NewT> WindowedValue<NewT> withValue(NewT value) {
      return new ValueInEmptyWindows<>(value);
    }

    @Override
    public T getValue() {
      return value;
    }

    @Override
    public Instant getTimestamp() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @Override
    public Collection<? extends BoundedWindow> getWindows() {
      return Collections.emptyList();
    }

    @Override
    public PaneInfo getPane() {
      return PaneInfo.NO_FIRING;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass()).add("value", getValue()).toString();
    }

    @Override
    public int hashCode() {
      return Objects.hash(getValue());
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (o instanceof ValueInEmptyWindows) {
        ValueInEmptyWindows<?> that = (ValueInEmptyWindows<?>) o;
        return Objects.equals(that.getValue(), this.getValue());
      } else {
        return super.equals(o);
      }
    }
  }
}
