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
package org.apache.beam.runners.dataflow.worker.util;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.values.WindowingStrategy;

/** Factory methods for creating {@link BatchGroupAlsoByWindowFn} for bounded inputs. */
public class BatchGroupAlsoByWindowsDoFns {
  /**
   * Create a {@link BatchGroupAlsoByWindowFn} without a combine function. Depending on the {@code
   * windowFn} this will either use iterators or window sets to implement the grouping.
   *
   * @param windowingStrategy The window function and trigger to use for grouping
   * @param inputCoder the input coder to use
   */
  public static <K, V, W extends BoundedWindow>
      BatchGroupAlsoByWindowFn<K, V, Iterable<V>> createForIterable(
          WindowingStrategy<?, W> windowingStrategy,
          StateInternalsFactory<K> stateInternalsFactory,
          Coder<V> inputCoder) {
    // If the windowing strategy indicates we're doing a reshuffle, use the special-path.
    if (BatchGroupAlsoByWindowReshuffleFn.isReshuffle(windowingStrategy)) {
      return new BatchGroupAlsoByWindowReshuffleFn<>();
    } else if (BatchGroupAlsoByWindowViaIteratorsFn.isSupported(windowingStrategy)) {
      return new BatchGroupAlsoByWindowViaIteratorsFn<K, V, W>(windowingStrategy);
    }
    return new BatchGroupAlsoByWindowViaOutputBufferFn<>(
        windowingStrategy, stateInternalsFactory, SystemReduceFn.buffering(inputCoder));
  }

  /** Construct a {@link BatchGroupAlsoByWindowFn} using the {@code combineFn} if available. */
  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow>
      BatchGroupAlsoByWindowFn<K, InputT, OutputT> create(
          final WindowingStrategy<?, W> windowingStrategy,
          final AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
    checkNotNull(combineFn);
    return new BatchGroupAlsoByWindowAndCombineFn<>(windowingStrategy, combineFn.getFn());
  }
}
