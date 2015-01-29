/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.transforms;

import static com.google.cloud.dataflow.sdk.util.CoderUtils.encodeToByteArray;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.ValueWithMetadata;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.InvalidWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.NonMergingWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.GroupAlsoByWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.ReifyTimestampAndWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code GroupByKey<K, V>} takes a {@code PCollection<KV<K, V>>},
 * groups the values by key and windows, and returns a
 * {@code PCollection<KV<K, Iterable<V>>>} representing a map from
 * each distinct key and window of the input {@code PCollection} to an
 * {@code Iterable} over all the values associated with that key in
 * the input.  Each key in the output {@code PCollection} is unique within
 * each window.
 *
 * <p> {@code GroupByKey} is analogous to converting a multi-map into
 * a uni-map, and related to {@code GROUP BY} in SQL.  It corresponds
 * to the "shuffle" step between the Mapper and the Reducer in the
 * MapReduce framework.
 *
 * <p> Two keys of type {@code K} are compared for equality
 * <b>not</b> by regular Java {@link Object#equals}, but instead by
 * first encoding each of the keys using the {@code Coder} of the
 * keys of the input {@code PCollection}, and then comparing the
 * encoded bytes.  This admits efficient parallel evaluation.  Note that
 * this requires that the {@code Coder} of the keys be deterministic (see
 * {@link Coder#isDeterministic()}).  If the key {@code Coder} is not
 * deterministic, an exception is thrown at runtime.
 *
 * <p> By default, the {@code Coder} of the keys of the output
 * {@code PCollection} is the same as that of the keys of the input,
 * and the {@code Coder} of the elements of the {@code Iterable}
 * values of the output {@code PCollection} is the same as the
 * {@code Coder} of the values of the input.
 *
 * <p> Example of use:
 * <pre> {@code
 * PCollection<KV<String, Doc>> urlDocPairs = ...;
 * PCollection<KV<String, Iterable<Doc>>> urlToDocs =
 *     urlDocPairs.apply(GroupByKey.<String, Doc>create());
 * PCollection<R> results =
 *     urlToDocs.apply(ParDo.of(new DoFn<KV<String, Iterable<Doc>>, R>() {
 *       public void processElement(ProcessContext c) {
 *         String url = c.element().getKey();
 *         Iterable<Doc> docsWithThatUrl = c.element().getValue();
 *         ... process all docs having that url ...
 *       }}));
 * } </pre>
 *
 * <p> {@code GroupByKey} is a key primitive in data-parallel
 * processing, since it is the main way to efficiently bring
 * associated data together into one location.  It is also a key
 * determiner of the performance of a data-parallel pipeline.
 *
 * <p> See {@link com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey}
 * for a way to group multiple input PCollections by a common key at once.
 *
 * <p> See {@link Combine.PerKey} for a common pattern of
 * {@code GroupByKey} followed by {@link Combine.GroupedValues}.
 *
 * <p> When grouping, windows that can be merged according to the {@link WindowFn}
 * of the input {@code PCollection} will be merged together, and a group
 * corresponding to the new, merged window will be emitted.
 * The timestamp for each group is the upper bound of its window, e.g., the most
 * recent timestamp that can be assigned into the window, and the group will be
 * in the window that it corresponds to.  The output {@code PCollection} will
 * have the same {@link WindowFn} as the input.
 *
 * <p> If the {@link WindowFn} of the input requires merging, it is not
 * valid to apply another {@code GroupByKey} without first applying a new
 * {@link WindowFn}.
 *
 * @param <K> the type of the keys of the input and output
 * {@code PCollection}s
 * @param <V> the type of the values of the input {@code PCollection}
 * and the elements of the {@code Iterable}s in the output
 * {@code PCollection}
 */
@SuppressWarnings("serial")
public class GroupByKey<K, V>
    extends PTransform<PCollection<KV<K, V>>,
                       PCollection<KV<K, Iterable<V>>>> {
  /**
   * Returns a {@code GroupByKey<K, V>} {@code PTransform}.
   *
   * @param <K> the type of the keys of the input and output
   * {@code PCollection}s
   * @param <V> the type of the values of the input {@code PCollection}
   * and the elements of the {@code Iterable}s in the output
   * {@code PCollection}
   */
  public static <K, V> GroupByKey<K, V> create() {
    return new GroupByKey<>();
  }


  /////////////////////////////////////////////////////////////////////////////

  @Override
  public PCollection<KV<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
    return applyHelper(input, false, false);
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * Helper transform that makes timestamps and window assignments
   * explicit in the value part of each key/value pair.
   */
  public static class ReifyTimestampsAndWindows<K, V>
      extends PTransform<PCollection<KV<K, V>>,
                         PCollection<KV<K, WindowedValue<V>>>> {
    @Override
    public PCollection<KV<K, WindowedValue<V>>> apply(
        PCollection<KV<K, V>> input) {
      @SuppressWarnings("unchecked")
      KvCoder<K, V> inputKvCoder = (KvCoder<K, V>) getInput().getCoder();
      Coder<K> keyCoder = inputKvCoder.getKeyCoder();
      Coder<V> inputValueCoder = inputKvCoder.getValueCoder();
      Coder<WindowedValue<V>> outputValueCoder = FullWindowedValueCoder.of(
          inputValueCoder, getInput().getWindowFn().windowCoder());
      Coder<KV<K, WindowedValue<V>>> outputKvCoder =
          KvCoder.of(keyCoder, outputValueCoder);
      return input.apply(ParDo.of(new ReifyTimestampAndWindowsDoFn<K, V>()))
          .setCoder(outputKvCoder);
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * Helper transform that sorts the values associated with each key
   * by timestamp.
   */
  public static class SortValuesByTimestamp<K, V>
      extends PTransform<PCollection<KV<K, Iterable<WindowedValue<V>>>>,
                         PCollection<KV<K, Iterable<WindowedValue<V>>>>> {
    @Override
    public PCollection<KV<K, Iterable<WindowedValue<V>>>> apply(
        PCollection<KV<K, Iterable<WindowedValue<V>>>> input) {
      return input.apply(ParDo.of(
          new DoFn<KV<K, Iterable<WindowedValue<V>>>,
                   KV<K, Iterable<WindowedValue<V>>>>() {
            @Override
            public void processElement(ProcessContext c) {
              KV<K, Iterable<WindowedValue<V>>> kvs = c.element();
              K key = kvs.getKey();
              Iterable<WindowedValue<V>> unsortedValues = kvs.getValue();
              List<WindowedValue<V>> sortedValues = new ArrayList<>();
              for (WindowedValue<V> value : unsortedValues) {
                sortedValues.add(value);
              }
              Collections.sort(sortedValues,
                               new Comparator<WindowedValue<V>>() {
                  @Override
                  public int compare(WindowedValue<V> e1, WindowedValue<V> e2) {
                    return e1.getTimestamp().compareTo(e2.getTimestamp());
                  }
                });
              c.output(KV.<K, Iterable<WindowedValue<V>>>of(key, sortedValues));
            }}))
          .setCoder(getInput().getCoder());
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * Helper transform that takes a collection of timestamp-ordered
   * values associated with each key, groups the values by window,
   * combines windows as needed, and for each window in each key,
   * outputs a collection of key/value-list pairs implicitly assigned
   * to the window and with the timestamp derived from that window.
   */
  public static class GroupAlsoByWindow<K, V>
      extends PTransform<PCollection<KV<K, Iterable<WindowedValue<V>>>>,
                         PCollection<KV<K, Iterable<V>>>> {
    private final WindowFn<?, ?> windowFn;

    public GroupAlsoByWindow(WindowFn<?, ?> windowFn) {
      this.windowFn = windowFn;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PCollection<KV<K, Iterable<V>>> apply(
        PCollection<KV<K, Iterable<WindowedValue<V>>>> input) {
      @SuppressWarnings("unchecked")
      KvCoder<K, Iterable<WindowedValue<V>>> inputKvCoder =
          (KvCoder<K, Iterable<WindowedValue<V>>>) getInput().getCoder();
      Coder<K> keyCoder = inputKvCoder.getKeyCoder();
      Coder<Iterable<WindowedValue<V>>> inputValueCoder =
          inputKvCoder.getValueCoder();
      IterableCoder<WindowedValue<V>> inputIterableValueCoder =
          (IterableCoder<WindowedValue<V>>) inputValueCoder;
      Coder<WindowedValue<V>> inputIterableElementCoder =
          inputIterableValueCoder.getElemCoder();
      WindowedValueCoder<V> inputIterableWindowedValueCoder =
          (WindowedValueCoder<V>) inputIterableElementCoder;
      Coder<V> inputIterableElementValueCoder =
          inputIterableWindowedValueCoder.getValueCoder();
      Coder<Iterable<V>> outputValueCoder =
          IterableCoder.of(inputIterableElementValueCoder);
      Coder<KV<K, Iterable<V>>> outputKvCoder =
          KvCoder.of(keyCoder, outputValueCoder);

      return input.apply(ParDo.of(
          new GroupAlsoByWindowsDoFn<K, V, BoundedWindow>(
              (WindowFn) windowFn, inputIterableElementValueCoder)))
          .setCoder(outputKvCoder);
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * Primitive helper transform that groups by key only, ignoring any
   * window assignments.
   */
  public static class GroupByKeyOnly<K, V>
      extends PTransform<PCollection<KV<K, V>>,
                         PCollection<KV<K, Iterable<V>>>> {
    // TODO: Define and implement sorting by value.
    boolean sortsValues = false;

    public GroupByKeyOnly() { }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public PCollection<KV<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
      WindowFn windowFn = getInput().getWindowFn();
      if (!(windowFn instanceof NonMergingWindowFn)) {
        // Prevent merging windows again, without explicit user
        // involvement, e.g., by Window.into() or Window.remerge().
        windowFn = new InvalidWindows(
            "WindowFn has already been consumed by previous GroupByKey",
            windowFn);
      }
      return PCollection.<KV<K, Iterable<V>>>createPrimitiveOutputInternal(
          windowFn);
    }

    @Override
    public void finishSpecifying() {
      // Verify that the input Coder<KV<K, V>> is a KvCoder<K, V>, and that
      // the key coder is deterministic.
      Coder<K> keyCoder = getKeyCoder();
      if (!keyCoder.isDeterministic()) {
        throw new IllegalStateException(
            "the key Coder must be deterministic for grouping");
      }
      if (getOutput().isOrdered()) {
        throw new IllegalStateException(
            "the result of a GroupByKey cannot be specified to be ordered");
      }
      super.finishSpecifying();
    }

    /**
     * Returns the {@code Coder} of the input to this transform, which
     * should be a {@code KvCoder}.
     */
    @SuppressWarnings("unchecked")
    KvCoder<K, V> getInputKvCoder() {
      Coder<KV<K, V>> inputCoder = getInput().getCoder();
      if (!(inputCoder instanceof KvCoder)) {
        throw new IllegalStateException(
            "GroupByKey requires its input to use KvCoder");
      }
      return (KvCoder<K, V>) inputCoder;
    }

    /**
     * Returns the {@code Coder} of the keys of the input to this
     * transform, which is also used as the {@code Coder} of the keys of
     * the output of this transform.
     */
    Coder<K> getKeyCoder() {
      return getInputKvCoder().getKeyCoder();
    }

    /**
     * Returns the {@code Coder} of the values of the input to this transform.
     */
    Coder<V> getInputValueCoder() {
      return getInputKvCoder().getValueCoder();
    }

    /**
     * Returns the {@code Coder} of the {@code Iterable} values of the
     * output of this transform.
     */
    Coder<Iterable<V>> getOutputValueCoder() {
      return IterableCoder.of(getInputValueCoder());
    }

    /**
     * Returns the {@code Coder} of the output of this transform.
     */
    KvCoder<K, Iterable<V>> getOutputKvCoder() {
      return KvCoder.of(getKeyCoder(), getOutputValueCoder());
    }

    @Override
    protected Coder<KV<K, Iterable<V>>> getDefaultOutputCoder() {
      return getOutputKvCoder();
    }

    /**
     * Returns whether this GBK sorts values.
     */
    boolean sortsValues() {
      return sortsValues;
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  static {
    registerWithDirectPipelineRunner();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static <K, V> void registerWithDirectPipelineRunner() {
    DirectPipelineRunner.registerDefaultTransformEvaluator(
        GroupByKeyOnly.class,
        new DirectPipelineRunner.TransformEvaluator<GroupByKeyOnly>() {
          @Override
          public void evaluate(
              GroupByKeyOnly transform,
              DirectPipelineRunner.EvaluationContext context) {
            evaluateHelper(transform, context);
          }
        });
  }

  private static <K, V> void evaluateHelper(
      GroupByKeyOnly<K, V> transform,
      DirectPipelineRunner.EvaluationContext context) {
    PCollection<KV<K, V>> input = transform.getInput();

    List<ValueWithMetadata<KV<K, V>>> inputElems =
        context.getPCollectionValuesWithMetadata(input);

    Coder<K> keyCoder = transform.getKeyCoder();

    Map<GroupingKey<K>, List<V>> groupingMap = new HashMap<>();

    for (ValueWithMetadata<KV<K, V>> elem : inputElems) {
      K key = elem.getValue().getKey();
      V value = elem.getValue().getValue();
      byte[] encodedKey;
      try {
        encodedKey = encodeToByteArray(keyCoder, key);
      } catch (CoderException exn) {
        // TODO: Put in better element printing:
        // truncate if too long.
        throw new IllegalArgumentException(
            "unable to encode key " + key + " of input to " + transform +
            " using " + keyCoder,
            exn);
      }
      GroupingKey<K> groupingKey = new GroupingKey<>(key, encodedKey);
      List<V> values = groupingMap.get(groupingKey);
      if (values == null) {
        values = new ArrayList<V>();
        groupingMap.put(groupingKey, values);
      }
      values.add(value);
    }

    List<ValueWithMetadata<KV<K, Iterable<V>>>> outputElems =
        new ArrayList<>();
    for (Map.Entry<GroupingKey<K>, List<V>> entry : groupingMap.entrySet()) {
      GroupingKey<K> groupingKey = entry.getKey();
      K key = groupingKey.getKey();
      List<V> values = entry.getValue();
      values = context.randomizeIfUnordered(
          transform.sortsValues(), values, true /* inPlaceAllowed */);
      outputElems.add(ValueWithMetadata
                      .of(WindowedValue.valueInEmptyWindows(KV.<K, Iterable<V>>of(key, values)))
                      .withKey(key));
    }

    context.setPCollectionValuesWithMetadata(transform.getOutput(),
                                             outputElems);
  }

  public PCollection<KV<K, Iterable<V>>> applyHelper(
      PCollection<KV<K, V>> input, boolean isStreaming, boolean runnerSortsByTimestamp) {
    Coder<KV<K, V>> inputCoder = getInput().getCoder();
    if (!(inputCoder instanceof KvCoder)) {
      throw new IllegalStateException(
          "GroupByKey requires its input to use KvCoder");
    }
    // This operation groups by the combination of key and window,
    // merging windows as needed, using the windows assigned to the
    // key/value input elements and the window merge operation of the
    // window function associated with the input PCollection.
    WindowFn<?, ?> windowFn = getInput().getWindowFn();
    if (windowFn instanceof InvalidWindows) {
      String cause = ((InvalidWindows<?>) windowFn).getCause();
      throw new IllegalStateException(
          "GroupByKey must have a valid Window merge function.  "
          + "Invalid because: " + cause);
    }
    if (windowFn.isCompatible(new GlobalWindows())) {
      // The input PCollection is using the degenerate default
      // window function, which uses a single global window for all
      // elements.  We can implement this using a more-primitive
      // non-window-aware GBK transform.
      return input.apply(new GroupByKeyOnly<K, V>());

    } else if (isStreaming) {
      // If using the streaming runner, the service will do the insertion of
      // the GroupAlsoByWindow step.
      // TODO: Remove this case once the Dataflow Runner handles GBK directly
      return input.apply(new GroupByKeyOnly<K, V>());

    } else {
      // By default, implement GroupByKey[AndWindow] via a series of lower-level
      // operations.
      PCollection<KV<K, Iterable<WindowedValue<V>>>> gbkOutput = input
          // Make each input element's timestamp and assigned windows
          // explicit, in the value part.
          .apply(new ReifyTimestampsAndWindows<K, V>())

          // Group by just the key.
          .apply(new GroupByKeyOnly<K, WindowedValue<V>>());

      if (!runnerSortsByTimestamp) {
        // Sort each key's values by timestamp. GroupAlsoByWindow requires
        // its input to be sorted by timestamp.
        gbkOutput = gbkOutput.apply(new SortValuesByTimestamp<K, V>());
      }

      return gbkOutput
          // Group each key's values by window, merging windows as needed.
          .apply(new GroupAlsoByWindow<K, V>(windowFn));
    }
  }

  private static class GroupingKey<K> {
    private K key;
    private byte[] encodedKey;

    public GroupingKey(K key, byte[] encodedKey) {
      this.key = key;
      this.encodedKey = encodedKey;
    }

    public K getKey() { return key; }

    @Override
    public boolean equals(Object o) {
      if (o instanceof GroupingKey) {
        GroupingKey<?> that = (GroupingKey<?>) o;
        return Arrays.equals(this.encodedKey, that.encodedKey);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() { return Arrays.hashCode(encodedKey); }
  }
}
