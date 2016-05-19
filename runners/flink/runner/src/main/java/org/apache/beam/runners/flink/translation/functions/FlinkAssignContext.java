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

import org.joda.time.Instant;

import java.util.Collection;

/**
 * {@link org.apache.beam.sdk.transforms.windowing.WindowFn.AssignContext} for
 * Flink functions.
 */
class FlinkAssignContext<InputT, W extends BoundedWindow>
    extends WindowFn<InputT, W>.AssignContext {
  private final WindowedValue<InputT> value;

  FlinkAssignContext(WindowFn<InputT, W> fn, WindowedValue<InputT> value) {
    fn.super();
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
  public Collection<? extends BoundedWindow> windows() {
    return value.getWindows();
  }

}
