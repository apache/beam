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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;

/**
 * A set of Beam windowing parameters.
 *
 * @param <T> type of elements being windowed
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by {@link WindowFn}
 * accessible through {@link #getWindowFn()}
 */
public class WindowingDesc<T, W extends BoundedWindow> implements Serializable {

  private final WindowFn<T, W> windowFn;
  private final Trigger trigger;
  private final WindowingStrategy.AccumulationMode accumulationMode;

  public WindowingDesc(WindowFn<T, W> windowFn,
      Trigger trigger, AccumulationMode accumulationMode) {
    this.windowFn = windowFn;
    this.trigger = trigger;
    this.accumulationMode = accumulationMode;
  }

  public WindowFn<T, W> getWindowFn() {
    return windowFn;
  }

  public Trigger getTrigger() {
    return trigger;
  }

  public AccumulationMode getAccumulationMode() {
    return accumulationMode;
  }
}
