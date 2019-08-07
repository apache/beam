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

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;

/** {@link org.apache.beam.sdk.transforms.windowing.WindowFn.AssignContext} for Flink functions. */
class FlinkAssignContext<InputT, W extends BoundedWindow>
    extends WindowFn<InputT, W>.AssignContext {
  private final WindowedValue<InputT> value;

  FlinkAssignContext(WindowFn<InputT, W> fn, WindowedValue<InputT> value) {
    fn.super();
    if (Iterables.size(value.getWindows()) != 1) {
      throw new IllegalArgumentException(
          String.format(
              "%s passed to window assignment must be in a single window, but it was in %s: %s",
              WindowedValue.class.getSimpleName(),
              Iterables.size(value.getWindows()),
              value.getWindows()));
    }
    this.value = value;
  }

  @Override
  public InputT element() {
    return value.getValue();
  }

  @Override
  public Instant timestamp() {
    return value.getTimestamp();
  }

  @Override
  public BoundedWindow window() {
    return Iterables.getOnlyElement(value.getWindows());
  }
}
