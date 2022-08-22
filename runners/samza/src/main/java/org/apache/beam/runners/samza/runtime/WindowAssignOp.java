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

import java.util.Collection;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;

/** Samza operator for {@link org.apache.beam.sdk.transforms.windowing.Window.Assign}. */
public class WindowAssignOp<T, W extends BoundedWindow> implements Op<T, T, Void> {
  private final WindowFn<T, W> windowFn;

  public WindowAssignOp(WindowFn<T, W> windowFn) {
    this.windowFn = windowFn;
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    final Collection<W> windows;
    try {
      windows = windowFn.assignWindows(new SamzaAssignContext<>(windowFn, inputElement));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    windows.stream()
        .map(
            window ->
                WindowedValue.of(
                    inputElement.getValue(),
                    inputElement.getTimestamp(),
                    window,
                    inputElement.getPane()))
        .forEach(outputElement -> emitter.emitElement(outputElement));
  }
}
