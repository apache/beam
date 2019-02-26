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

import java.util.Collections;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.dataflow.worker.util.StreamingGroupAlsoByWindowFn;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.ReshuffleTrigger;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Implementation of {@link StreamingGroupAlsoByWindowsDoFns} used for the {@link ReshuffleTrigger}
 * which outputs each element as a separate pane.
 *
 * @param <K> key type
 * @param <T> value element type
 */
public class StreamingGroupAlsoByWindowReshuffleFn<K, T>
    extends StreamingGroupAlsoByWindowFn<K, T, Iterable<T>> {

  public static boolean isReshuffle(WindowingStrategy<?, ?> strategy) {
    return strategy.getTrigger() instanceof ReshuffleTrigger;
  }

  @Override
  public void processElement(
      KeyedWorkItem<K, T> element,
      PipelineOptions options,
      StepContext stepContext,
      SideInputReader sideInputReader,
      OutputWindowedValue<KV<K, Iterable<T>>> output)
      throws Exception {
    @SuppressWarnings("unchecked")
    K key = element.key();
    for (WindowedValue<T> item : element.elementsIterable()) {
      output.outputWindowedValue(
          KV.of(key, Collections.singletonList(item.getValue())),
          item.getTimestamp(),
          item.getWindows(),
          item.getPane());
    }
  }
}
