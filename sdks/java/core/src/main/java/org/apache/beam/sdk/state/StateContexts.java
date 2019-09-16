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
package org.apache.beam.sdk.state;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

/** <b><i>For internal use only; no backwards-compatibility guarantees.</i></b> */
@Internal
public class StateContexts {
  private static final StateContext<BoundedWindow> NULL_CONTEXT =
      new StateContext<BoundedWindow>() {
        @Override
        public PipelineOptions getPipelineOptions() {
          throw new IllegalArgumentException("cannot call getPipelineOptions() in a null context");
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view) {
          throw new IllegalArgumentException("cannot call sideInput() in a null context");
        }

        @Override
        public BoundedWindow window() {
          throw new IllegalArgumentException("cannot call window() in a null context");
        }
      };

  /** Returns a fake {@link StateContext}. */
  @SuppressWarnings("unchecked")
  public static <W extends BoundedWindow> StateContext<W> nullContext() {
    return (StateContext<W>) NULL_CONTEXT;
  }

  public static <W extends BoundedWindow> StateContext<W> windowOnlyContext(W window) {
    return new WindowOnlyContext<>(window);
  }

  private static class WindowOnlyContext<W extends BoundedWindow> implements StateContext<W> {
    private final W window;

    private WindowOnlyContext(W window) {
      this.window = window;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      throw new IllegalArgumentException(
          "cannot call getPipelineOptions() in a window-only context");
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      throw new IllegalArgumentException("cannot call sideInput() in a window-only context");
    }

    @Override
    public W window() {
      return window;
    }
  }
}
