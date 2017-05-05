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
package org.apache.beam.runners.core;

import java.util.Collection;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * Adapters from {@link WindowingInternals} to {@link SideInputReader} and {@link
 * OutputWindowedValue}.
 */
public class WindowingInternalsAdapters {
  static SideInputReader sideInputReader(final WindowingInternals<?, ?> windowingInternals) {
    return new SideInputReader() {
      @Override
      public <T> T get(PCollectionView<T> view, BoundedWindow sideInputWindow) {
        return windowingInternals.sideInput(view, sideInputWindow);
      }

      @Override
      public <T> boolean contains(PCollectionView<T> view) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isEmpty() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public static <OutputT> OutputWindowedValue<OutputT> outputWindowedValue(
      final WindowingInternals<?, OutputT> windowingInternals) {
    return new OutputWindowedValue<OutputT>() {
      @Override
      public void outputWindowedValue(
          OutputT output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo pane) {
        windowingInternals.outputWindowedValue(output, timestamp, windows, pane);
      }

      @Override
      public <AdditionalOutputT> void outputWindowedValue(
          TupleTag<AdditionalOutputT> tag,
          AdditionalOutputT output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo pane) {
        windowingInternals.outputWindowedValue(tag, output, timestamp, windows, pane);
      }
    };
  }
}
