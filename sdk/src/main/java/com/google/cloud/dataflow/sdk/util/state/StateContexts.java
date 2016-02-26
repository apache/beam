/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import javax.annotation.Nullable;

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
