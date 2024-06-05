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
package org.apache.beam.runners.direct;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.ForwardingPTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;

class DirectGroupByKey<K, V>
    extends ForwardingPTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {
  private final PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> original;

  static final String DIRECT_GBKO_URN = "beam:directrunner:transforms:gbko:v1";
  static final String DIRECT_GABW_URN = "beam:directrunner:transforms:gabw:v1";
  private final WindowingStrategy<?, ?> outputWindowingStrategy;

  DirectGroupByKey(
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> original,
      WindowingStrategy<?, ?> outputWindowingStrategy) {
    this.original = original;
    this.outputWindowingStrategy = outputWindowingStrategy;
  }

  @Override
  public PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> delegate() {
    return original;
  }

  @Override
  public PCollection<KV<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
    // This operation groups by the combination of key and window,
    // merging windows as needed, using the windows assigned to the
    // key/value input elements and the window merge operation of the
    // window function associated with the input PCollection.
    WindowingStrategy<?, ?> inputWindowingStrategy = input.getWindowingStrategy();

    // By default, implement GroupByKey via a series of lower-level operations.
    return input
        .apply(new DirectGroupByKeyOnly<>())

        // Group each key's values by window, merging windows as needed.
        .apply(
            "GroupAlsoByWindow",
            new DirectGroupAlsoByWindow<>(inputWindowingStrategy, outputWindowingStrategy));
  }

  static final class DirectGroupByKeyOnly<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KeyedWorkItem<K, V>>> {
    @Override
    public PCollection<KeyedWorkItem<K, V>> expand(PCollection<KV<K, V>> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          input.isBounded(),
          KeyedWorkItemCoder.of(
              GroupByKey.getKeyCoder(input.getCoder()),
              GroupByKey.getInputValueCoder(input.getCoder()),
              input.getWindowingStrategy().getWindowFn().windowCoder()));
    }

    DirectGroupByKeyOnly() {}
  }

  static final class DirectGroupAlsoByWindow<K, V>
      extends PTransform<PCollection<KeyedWorkItem<K, V>>, PCollection<KV<K, Iterable<V>>>> {

    private final WindowingStrategy<?, ?> inputWindowingStrategy;
    private final WindowingStrategy<?, ?> outputWindowingStrategy;

    public DirectGroupAlsoByWindow(
        WindowingStrategy<?, ?> inputWindowingStrategy,
        WindowingStrategy<?, ?> outputWindowingStrategy) {
      this.inputWindowingStrategy = inputWindowingStrategy;
      this.outputWindowingStrategy = outputWindowingStrategy;
    }

    public WindowingStrategy<?, ?> getInputWindowingStrategy() {
      return inputWindowingStrategy;
    }

    private KeyedWorkItemCoder<K, V> getKeyedWorkItemCoder(Coder<KeyedWorkItem<K, V>> inputCoder) {
      // Coder<KV<...>> --> KvCoder<...>
      checkArgument(
          inputCoder instanceof KeyedWorkItemCoder,
          "%s requires a %s<...> but got %s",
          getClass().getSimpleName(),
          KvCoder.class.getSimpleName(),
          inputCoder);
      @SuppressWarnings("unchecked")
      KeyedWorkItemCoder<K, V> kvCoder = (KeyedWorkItemCoder<K, V>) inputCoder;
      return kvCoder;
    }

    public Coder<V> getValueCoder(Coder<KeyedWorkItem<K, V>> inputCoder) {
      return getKeyedWorkItemCoder(inputCoder).getElementCoder();
    }

    @Override
    public PCollection<KV<K, Iterable<V>>> expand(PCollection<KeyedWorkItem<K, V>> input) {
      KeyedWorkItemCoder<K, V> inputCoder = getKeyedWorkItemCoder(input.getCoder());
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          outputWindowingStrategy,
          input.isBounded(),
          KvCoder.of(inputCoder.getKeyCoder(), IterableCoder.of(inputCoder.getElementCoder())));
    }
  }
}
