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

import java.util.Collections;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.ReshuffleTrigger;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Implementation of {@link BatchGroupAlsoByWindowFn} used for the {@link ReshuffleTrigger} which
 * outputs each element as a separate pane.
 *
 * @param <K> key type
 * @param <V> value element type
 * @param <W> window type
 */
public class BatchGroupAlsoByWindowReshuffleFn<K, V, W extends BoundedWindow>
    extends BatchGroupAlsoByWindowFn<K, V, Iterable<V>> {

  public static boolean isReshuffle(WindowingStrategy<?, ?> strategy) {
    return strategy.getTrigger() instanceof ReshuffleTrigger;
  }

  @Override
  public void processElement(
      KV<K, Iterable<WindowedValue<V>>> element,
      PipelineOptions options,
      StepContext stepContext,
      SideInputReader sideInputReader,
      OutputWindowedValue<KV<K, Iterable<V>>> output)
      throws Exception {
    K key = element.getKey();
    for (WindowedValue<V> item : element.getValue()) {
      output.outputWindowedValue(
          KV.<K, Iterable<V>>of(key, Collections.singletonList(item.getValue())),
          item.getTimestamp(),
          item.getWindows(),
          item.getPane());
    }
  }
}
