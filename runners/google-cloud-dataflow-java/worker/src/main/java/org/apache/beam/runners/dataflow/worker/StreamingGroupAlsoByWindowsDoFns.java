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
package org.apache.beam.runners.dataflow.worker;

import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/** {@link GroupAlsoByWindowFn}'s that merge windows and groups elements in those windows. */
public abstract class StreamingGroupAlsoByWindowsDoFns {
  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow>
      GroupAlsoByWindowFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> create(
          final WindowingStrategy<?, W> windowingStrategy,
          StateInternalsFactory<K> stateInternalsFactory,
          final AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn,
          final Coder<K> keyCoder) {
    Preconditions.checkNotNull(combineFn);
    return StreamingGroupAlsoByWindowViaWindowSetFn.create(
        windowingStrategy, stateInternalsFactory, SystemReduceFn.combining(keyCoder, combineFn));
  }

  public static <K, V, W extends BoundedWindow>
      GroupAlsoByWindowFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> createForIterable(
          final WindowingStrategy<?, W> windowingStrategy,
          StateInternalsFactory<K> stateInternalsFactory,
          Coder<V> inputCoder) {
    // If the windowing strategy indicates we're doing a reshuffle, use the special-path.
    if (StreamingGroupAlsoByWindowReshuffleFn.isReshuffle(windowingStrategy)) {
      return new StreamingGroupAlsoByWindowReshuffleFn<>();
    } else {
      return StreamingGroupAlsoByWindowViaWindowSetFn.create(
          windowingStrategy, stateInternalsFactory, SystemReduceFn.buffering(inputCoder));
    }
  }
}
