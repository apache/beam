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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.context;

import java.util.Set;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/** A {@link SideInputReader} that fetches side inputs from the streaming worker's cache. */
public class StreamingModeSideInputReader implements SideInputReader {

  private final StreamingModeExecutionContext context;
  private final Set<PCollectionView<?>> viewSet;

  private StreamingModeSideInputReader(
      Iterable<? extends PCollectionView<?>> views, StreamingModeExecutionContext context) {
    this.context = context;
    this.viewSet = ImmutableSet.copyOf(views);
  }

  public static StreamingModeSideInputReader of(
      Iterable<? extends PCollectionView<?>> views, StreamingModeExecutionContext context) {
    return new StreamingModeSideInputReader(views, context);
  }

  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    if (!contains(view)) {
      throw new RuntimeException("get() called with unknown view");
    }

    // We are only fetching the cached value here, so we don't need stateFamily or
    // readStateSupplier.
    return context
        .fetchSideInput(
            view,
            window,
            null /* unused stateFamily */,
            SideInputState.CACHED_IN_WORK_ITEM,
            null /* unused readStateSupplier */)
        .value()
        .orElse(null);
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return viewSet.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return viewSet.isEmpty();
  }
}
