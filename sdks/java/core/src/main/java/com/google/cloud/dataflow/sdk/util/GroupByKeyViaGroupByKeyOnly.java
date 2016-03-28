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

package com.google.cloud.dataflow.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * An implementation of {@link GroupByKey} built on top of a simpler {@link GroupByKeyOnly}
 * primitive.
 *
 * <p>This implementation of {@link GroupByKey} proceeds by reifying windows and timestamps (making
 * them part of the element rather than metadata), performing a {@link GroupByKeyOnly} primitive,
 * then using a {@link GroupAlsoByWindow} transform to further group the resulting elements by
 * window.
 *
 * <p>Today {@link GroupAlsoByWindow} is implemented as a {@link ParDo} that calls reserved
 * internal methods.
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
   * Helper transform that makes timestamps and window assignments
   * explicit in the value part of each key/value pair.
   */
  public static class ReifyTimestampsAndWindows<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, WindowedValue<V>>>> {
    @Override
    public PCollection<KV<K, WindowedValue<V>>> apply(PCollection<KV<K, V>> input) {

      // The requirement to use a KvCoder *is* actually a model-level requirement, not specific
      // to this implementation of GBK. All runners need a way to get the key.
      checkArgument(input.getCoder() instanceof KvCoder,
          "%s requires its input to use a %s",
          GroupByKey.class.getSimpleName(),
          KvCoder.class.getSimpleName());

      @SuppressWarnings("unchecked")
      KvCoder<K, V> inputKvCoder = (KvCoder<K, V>) input.getCoder();
      Coder<K> keyCoder = inputKvCoder.getKeyCoder();
      Coder<V> inputValueCoder = inputKvCoder.getValueCoder();
      Coder<WindowedValue<V>> outputValueCoder =
          FullWindowedValueCoder.of(
              inputValueCoder, input.getWindowingStrategy().getWindowFn().windowCoder());
      Coder<KV<K, WindowedValue<V>>> outputKvCoder = KvCoder.of(keyCoder, outputValueCoder);
      return input
          .apply(ParDo.of(new ReifyTimestampAndWindowsDoFn<K, V>()))
          .setCoder(outputKvCoder);
    }
  }

  /**
   * Helper transform that sorts the values associated with each key
   * by timestamp.
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
