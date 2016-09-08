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
package org.apache.beam.sdk.util.state;

import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Factory that produces {@link StateContext} based on different inputs.
 */
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
        }};

  /**
   * Returns a fake {@link StateContext}.
   */
  @SuppressWarnings("unchecked")
  public static <W extends BoundedWindow> StateContext<W> nullContext() {
    return (StateContext<W>) NULL_CONTEXT;
  }

  /**
   * Returns a {@link StateContext} that only contains the state window.
   */
  public static <W extends BoundedWindow> StateContext<W> windowOnly(final W window) {
    return new StateContext<W>() {
      @Override
      public PipelineOptions getPipelineOptions() {
        throw new IllegalArgumentException(
            "cannot call getPipelineOptions() in a window only context");
      }
      @Override
      public <T> T sideInput(PCollectionView<T> view) {
        throw new IllegalArgumentException("cannot call sideInput() in a window only context");
      }
      @Override
      public W window() {
        return window;
      }
    };
  }

  /**
   * Returns a {@link StateContext} from {@code PipelineOptions}, {@link WindowingInternals},
   * and the state window.
   */
  public static <W extends BoundedWindow> StateContext<W> createFromComponents(
      @Nullable final PipelineOptions options,
      final WindowingInternals<?, ?> windowingInternals,
      final W window) {
    @SuppressWarnings("unchecked")
    StateContext<W> typedNullContext = (StateContext<W>) NULL_CONTEXT;
    if (options == null) {
      return typedNullContext;
    } else {
      return new StateContext<W>() {

        @Override
        public PipelineOptions getPipelineOptions() {
          return options;
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view) {
          return windowingInternals.sideInput(view, window);
        }

        @Override
        public W window() {
          return window;
        }
      };
    }
  }
}
