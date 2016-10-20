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

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Information accessible the state API.
 */
public interface StateContext<W extends BoundedWindow> {
  /**
   * Returns the {@code PipelineOptions} specified with the
   * {@link org.apache.beam.sdk.runners.PipelineRunner}.
   */
  PipelineOptions getPipelineOptions();

  /**
   * Returns the value of the side input for the corresponding state window.
   */
  <T> T sideInput(PCollectionView<T> view);

  /**
   * Returns the window corresponding to the state.
   */
  W window();
}
