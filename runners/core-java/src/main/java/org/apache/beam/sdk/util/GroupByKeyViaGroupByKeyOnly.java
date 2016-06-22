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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * An implementation of {@link GroupByKey} built on top of a lower-level {@link GroupByKeyOnly}
 * primitive.
 *
 * <p>This implementation of {@link GroupByKey} proceeds via the following steps:
 * <ol>
 *   <li>{@code ReifyTimestampsAndWindowsDoFn ParDo(ReifyTimestampsAndWindows)}: This embeds
 *       the previously-implicit timestamp and window into the elements themselves, so a
 *       window-and-timestamp-unaware transform can operate on them.</li>
 *   <li>{@code GroupByKeyOnly}: This lower-level primitive groups by keys, ignoring windows
 *       and timestamps. Many window-unaware runners have such a primitive already.</li>
 *   <li>{@code SortValuesByTimestamp ParDo(SortValuesByTimestamp)}: The values in the iterables
 *       output by {@link GroupByKeyOnly} are sorted by timestamp.</li>
 *   <li>{@code GroupAlsoByWindow}: This primitive processes the sorted values. Today it is
 *       implemented as a {@link ParDo} that calls reserved internal methods.</li>
 * </ol>
 *
 * <p>This implementation of {@link GroupByKey} has severe limitations unless its component
 * transforms are replaced. As-is, it is only applicable for in-memory runners using a batch-style
 * execution strategy. Specifically:
 *
 * <ul>
 *   <li>Every iterable output by {@link GroupByKeyOnly} must contain all elements for that key.
 *       A streaming-style partition, with multiple elements for the same key, will not yield
 *       correct results.</li>
 *   <li>Sorting of values by timestamp is performed on an in-memory list. It will not succeed
 *       for large iterables.</li>
 *   <li>The implementation of {@code GroupAlsoByWindow} does not support timers. This is only
 *       appropriate for runners which also do not support timers.</li>
 * </ul>
 */
public class GroupByKeyViaGroupByKeyOnly<K, V>
    extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

  private GroupByKey<K, V> gbkTransform;

  public GroupByKeyViaGroupByKeyOnly(GroupByKey<K, V> originalTransform) {
    this.gbkTransform = originalTransform;
  }

  @Override
  public PCollection<KV<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
    WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();

    return input
        // Make each input element's timestamp and assigned windows
        // explicit, in the value part.
        .apply(new ReifyTimestampsAndWindows<K, V>())

        // Group by just the key.
        // Combiner lifting will not happen regardless of the disallowCombinerLifting value.
        // There will be no combiners right after the GroupByKeyOnly because of the two ParDos
        // introduced in here.
        .apply(new GroupByKeyOnly<K, WindowedValue<V>>())

        // Sort each key's values by timestamp. GroupAlsoByWindow requires
        // its input to be sorted by timestamp.
        .apply(new SortValuesByTimestamp<K, V>())

        // Group each key's values by window, merging windows as needed.
        .apply(new GroupAlsoByWindow<K, V>(windowingStrategy))

        // And update the windowing strategy as appropriate.
        .setWindowingStrategyInternal(
            gbkTransform.updateWindowingStrategy(windowingStrategy));
  }

  /**
   * Runner-specific primitive that groups by key only, ignoring any window assignments. A
   * runner that uses {@link GroupByKeyViaGroupByKeyOnly} should have a primitive way to translate
   * or evaluate this class.
   */
  public static class GroupByKeyOnly<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public PCollection<KV<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
      return PCollection.<KV<K, Iterable<V>>>createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded());
    }

    @Override
    public Coder<KV<K, Iterable<V>>> getDefaultOutputCoder(PCollection<KV<K, V>> input) {
      return GroupByKey.getOutputKvCoder(input.getCoder());
    }
  }

  /**
   * Helper transform that sorts the values associated with each key by timestamp.
   */
  private static class SortValuesByTimestamp<K, V>
      extends PTransform<
          PCollection<KV<K, Iterable<WindowedValue<V>>>>,
          PCollection<KV<K, Iterable<WindowedValue<V>>>>> {
    @Override
    public PCollection<KV<K, Iterable<WindowedValue<V>>>> apply(
        PCollection<KV<K, Iterable<WindowedValue<V>>>> input) {
      return input
          .apply(
              ParDo.of(
                  new DoFn<KV<K, Iterable<WindowedValue<V>>>, KV<K, Iterable<WindowedValue<V>>>>() {
                    @Override
                    public void processElement(ProcessContext c) {
                      KV<K, Iterable<WindowedValue<V>>> kvs = c.element();
                      K key = kvs.getKey();
                      Iterable<WindowedValue<V>> unsortedValues = kvs.getValue();
                      List<WindowedValue<V>> sortedValues = new ArrayList<>();
                      for (WindowedValue<V> value : unsortedValues) {
                        sortedValues.add(value);
                      }
                      Collections.sort(
                          sortedValues,
                          new Comparator<WindowedValue<V>>() {
                            @Override
                            public int compare(WindowedValue<V> e1, WindowedValue<V> e2) {
                              return e1.getTimestamp().compareTo(e2.getTimestamp());
                            }
                          });
                      c.output(KV.<K, Iterable<WindowedValue<V>>>of(key, sortedValues));
                    }
                  }))
          .setCoder(input.getCoder());
    }
  }

  /**
   * Helper transform that takes a collection of timestamp-ordered
   * values associated with each key, groups the values by window,
   * combines windows as needed, and for each window in each key,
   * outputs a collection of key/value-list pairs implicitly assigned
   * to the window and with the timestamp derived from that window.
   */
  private static class GroupAlsoByWindow<K, V>
      extends PTransform<
          PCollection<KV<K, Iterable<WindowedValue<V>>>>, PCollection<KV<K, Iterable<V>>>> {
    private final WindowingStrategy<?, ?> windowingStrategy;

    public GroupAlsoByWindow(WindowingStrategy<?, ?> windowingStrategy) {
      this.windowingStrategy = windowingStrategy;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PCollection<KV<K, Iterable<V>>> apply(
        PCollection<KV<K, Iterable<WindowedValue<V>>>> input) {
      @SuppressWarnings("unchecked")
      KvCoder<K, Iterable<WindowedValue<V>>> inputKvCoder =
          (KvCoder<K, Iterable<WindowedValue<V>>>) input.getCoder();

      Coder<K> keyCoder = inputKvCoder.getKeyCoder();
      Coder<Iterable<WindowedValue<V>>> inputValueCoder = inputKvCoder.getValueCoder();

      IterableCoder<WindowedValue<V>> inputIterableValueCoder =
          (IterableCoder<WindowedValue<V>>) inputValueCoder;
      Coder<WindowedValue<V>> inputIterableElementCoder = inputIterableValueCoder.getElemCoder();
      WindowedValueCoder<V> inputIterableWindowedValueCoder =
          (WindowedValueCoder<V>) inputIterableElementCoder;

      Coder<V> inputIterableElementValueCoder = inputIterableWindowedValueCoder.getValueCoder();
      Coder<Iterable<V>> outputValueCoder = IterableCoder.of(inputIterableElementValueCoder);
      Coder<KV<K, Iterable<V>>> outputKvCoder = KvCoder.of(keyCoder, outputValueCoder);

      return input
          .apply(ParDo.of(groupAlsoByWindowsFn(windowingStrategy, inputIterableElementValueCoder)))
          .setCoder(outputKvCoder);
    }

    private <W extends BoundedWindow>
        GroupAlsoByWindowsViaOutputBufferDoFn<K, V, Iterable<V>, W> groupAlsoByWindowsFn(
            WindowingStrategy<?, W> strategy, Coder<V> inputIterableElementValueCoder) {
      return new GroupAlsoByWindowsViaOutputBufferDoFn<K, V, Iterable<V>, W>(
          strategy, SystemReduceFn.<K, V, W>buffering(inputIterableElementValueCoder));
    }
  }
}
