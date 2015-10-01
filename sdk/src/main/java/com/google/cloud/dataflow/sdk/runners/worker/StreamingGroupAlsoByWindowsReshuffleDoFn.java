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
package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.ReshuffleTrigger;
import com.google.cloud.dataflow.sdk.util.SystemDoFnInternal;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;

import java.util.Collections;

/**
 * Implementation of {@link StreamingGroupAlsoByWindowsDoFn} used for the {@link ReshuffleTrigger}
 * which outputs each element as a separate pane.
 *
 * @param <K> key type
 * @param <T> value element type
 */
@SystemDoFnInternal
public class StreamingGroupAlsoByWindowsReshuffleDoFn<K, T>
    extends DoFn<KeyedWorkItem<T>, KV<K, Iterable<T>>> {

  public static boolean isReshuffle(WindowingStrategy<?, ?> strategy) {
    return strategy.getTrigger().getSpec() instanceof ReshuffleTrigger;
  }

  @Override
  public void processElement(ProcessContext c) throws Exception {
    @SuppressWarnings("unchecked")
    K key = (K) c.element().key();
    for (WindowedValue<T> item : c.element().elementsIterable()) {
      c.windowingInternals().outputWindowedValue(
          KV.of(key, (Iterable<T>) Collections.singletonList(item.getValue())),
          item.getTimestamp(), item.getWindows(), item.getPane());
    }
  }
}
