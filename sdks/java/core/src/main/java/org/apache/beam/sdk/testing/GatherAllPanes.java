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
package org.apache.beam.sdk.testing;

import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * Gathers all panes of each window into exactly one output.
 *
 * <p>Note that this will delay the output of a window until the garbage collection time (when the
 * watermark passes the end of the window plus allowed lateness) even if the upstream triggers
 * closed the window earlier.
 */
class GatherAllPanes<T>
    extends PTransform<PCollection<T>, PCollection<Iterable<ValueInSingleWindow<T>>>> {
  /**
   * Gathers all panes of each window into a single output element.
   *
   * <p>This will gather all output panes into a single element, which causes them to be colocated
   * on a single worker. As a result, this is only suitable for {@link PCollection PCollections}
   * where all of the output elements for each pane fit in memory, such as in tests.
   */
  public static <T> GatherAllPanes<T> globally() {
    return new GatherAllPanes<>();
  }

  private GatherAllPanes() {}

  @Override
  public PCollection<Iterable<ValueInSingleWindow<T>>> expand(PCollection<T> input) {
    WindowFn<?, ?> originalWindowFn = input.getWindowingStrategy().getWindowFn();

    return input
        .apply(Reify.windows())
        .apply(
            WithKeys.<Integer, ValueInSingleWindow<T>>of(0)
                .withKeyType(new TypeDescriptor<Integer>() {}))
        .apply(
            Window.into(
                    new IdentityWindowFn<KV<Integer, ValueInSingleWindow<T>>>(
                        originalWindowFn.windowCoder()))
                .triggering(Never.ever())
                .withAllowedLateness(input.getWindowingStrategy().getAllowedLateness())
                .discardingFiredPanes())
        // all values have the same key so they all appear as a single output element
        .apply(GroupByKey.create())
        .apply(Values.create())
        .setWindowingStrategyInternal(input.getWindowingStrategy());
  }
}
