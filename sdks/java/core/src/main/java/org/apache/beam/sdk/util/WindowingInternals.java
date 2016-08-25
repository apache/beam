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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface that may be required by some (internal) {@code OldDoFn}s to implement windowing. It
 * should not be necessary for general user code to interact with this at all.
 *
 * <p>This interface should be provided by runner implementors to support windowing on their runner.
 *
 * @param <InputT> input type
 * @param <OutputT> output type
 */
public interface WindowingInternals<InputT, OutputT> {

  /**
   * Unsupported state internals. The key type is unknown. It is up to the user to use the
   * correct type of key.
   */
  StateInternals<?> stateInternals();

  /**
   * Output the value at the specified timestamp in the listed windows.
   */
  void outputWindowedValue(OutputT output, Instant timestamp,
      Collection<? extends BoundedWindow> windows, PaneInfo pane);

  /**
   * Return the timer manager provided by the underlying system, or null if Timers need
   * to be emulated.
   */
  TimerInternals timerInternals();

  /**
   * Access the windows the element is being processed in without "exploding" it.
   */
  Collection<? extends BoundedWindow> windows();

  /**
   * Access the pane of the current window(s).
   */
  PaneInfo pane();

  /**
   * Write the given {@link PCollectionView} data to a location accessible by other workers.
   */
  <T> void writePCollectionViewData(
      TupleTag<?> tag,
      Iterable<WindowedValue<T>> data,
      Coder<T> elemCoder) throws IOException;

  /**
   * Return the value of the side input for the window of a main input element.
   */
  <T> T sideInput(PCollectionView<T> view, BoundedWindow mainInputWindow);
}
