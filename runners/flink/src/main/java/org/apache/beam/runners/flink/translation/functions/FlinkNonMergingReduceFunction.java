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
package org.apache.beam.runners.flink.translation.functions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.PeekingIterator;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

/**
 * Reduce function for non-merging GBK implementation. Implementation tries to return non-iterable
 * results when possible, so we do not have to materialize all values for a single key in memory.
 *
 * @param <K> Key type.
 * @param <InputT> Input type.
 */
public class FlinkNonMergingReduceFunction<K, InputT>
    implements GroupReduceFunction<
        WindowedValue<KV<K, InputT>>, WindowedValue<KV<K, Iterable<InputT>>>> {

  private static class OnceIterable<T> implements Iterable<T> {

    private final Iterator<T> iterator;

    private final AtomicBoolean used = new AtomicBoolean(false);

    OnceIterable(Iterator<T> iterator) {
      this.iterator = iterator;
    }

    @Override
    public Iterator<T> iterator() {
      if (used.compareAndSet(false, true)) {
        return iterator;
      }
      throw new IllegalStateException(
          "GBK result is not re-iterable. You can enable re-iterations by setting '--reIterableGroupByKeyResult'.");
    }
  }

  private final WindowingStrategy<?, ?> windowingStrategy;
  private final boolean reIterableResult;

  public FlinkNonMergingReduceFunction(
      WindowingStrategy<?, ?> windowingStrategy, boolean reIterableResult) {
    this.windowingStrategy = windowingStrategy;
    this.reIterableResult = reIterableResult;
  }

  @Override
  public void reduce(
      Iterable<WindowedValue<KV<K, InputT>>> input,
      Collector<WindowedValue<KV<K, Iterable<InputT>>>> coll) {
    final PeekingIterator<WindowedValue<KV<K, InputT>>> iterator =
        Iterators.peekingIterator(input.iterator());
    final WindowedValue<KV<K, InputT>> first = iterator.peek();
    final BoundedWindow window = Iterables.getOnlyElement(first.getWindows());
    @SuppressWarnings("unchecked")
    final Instant outputTimestamp =
        ((WindowingStrategy) windowingStrategy)
            .getWindowFn()
            .getOutputTime(first.getTimestamp(), window);
    final Instant combinedTimestamp =
        windowingStrategy.getTimestampCombiner().assign(window, outputTimestamp);
    final Iterable<InputT> values;
    if (reIterableResult) {
      final List<InputT> lst = new ArrayList<>();
      iterator.forEachRemaining(wv -> lst.add(wv.getValue().getValue()));
      values = lst;
    } else {
      values =
          new OnceIterable<>(
              Iterators.transform(
                  iterator,
                  (WindowedValue<KV<K, InputT>> wv) ->
                      Objects.requireNonNull(wv).getValue().getValue()));
    }
    coll.collect(
        WindowedValue.of(
            KV.of(first.getValue().getKey(), values),
            combinedTimestamp,
            first.getWindows(),
            PaneInfo.ON_TIME_AND_ONLY_FIRING));
  }
}
