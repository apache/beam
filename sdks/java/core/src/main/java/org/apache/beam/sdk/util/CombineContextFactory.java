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

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.values.PCollectionView;

/** Factory that produces {@code Combine.Context} based on different inputs. */
@Internal
public class CombineContextFactory {

  private static final Context NULL_CONTEXT =
      new Context() {
        @Override
        public PipelineOptions getPipelineOptions() {
          throw new IllegalArgumentException("cannot call getPipelineOptions() in a null context");
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view) {
          throw new IllegalArgumentException("cannot call sideInput() in a null context");
        }
      };

  /** Returns a fake {@code Combine.Context} for tests. */
  public static Context nullContext() {
    return NULL_CONTEXT;
  }

  /** Returns a {@code Combine.Context} that wraps a {@link StateContext}. */
  public static Context createFromStateContext(final StateContext<?> c) {
    return new Context() {
      @Override
      public PipelineOptions getPipelineOptions() {
        return c.getPipelineOptions();
      }

      @Override
      public <T> T sideInput(PCollectionView<T> view) {
        return c.sideInput(view);
      }
    };
  }
}
