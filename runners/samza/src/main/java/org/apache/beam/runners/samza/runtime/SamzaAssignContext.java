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
package org.apache.beam.runners.samza.runtime;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;

@SuppressWarnings({"keyfor", "nullness"}) // TODO(https://github.com/apache/beam/issues/20497)
class SamzaAssignContext<InT, W extends BoundedWindow> extends WindowFn<InT, W>.AssignContext {
  private final WindowedValue<InT> value;

  public SamzaAssignContext(WindowFn<InT, W> fn, WindowedValue<InT> value) {
    fn.super();
    this.value = value;

    if (value.getWindows().size() != 1) {
      throw new IllegalArgumentException(
          String.format(
              "Only single windowed value allowed for assignment. Windows: %s",
              value.getWindows()));
    }
  }

  @Override
  public InT element() {
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
