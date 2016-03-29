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
import com.google.cloud.dataflow.sdk.values.PCollectionView;

/**
 * Information accessible the state API.
 */
public interface StateContext<W extends BoundedWindow> {
  /**
   * Returns the {@code PipelineOptions} specified with the
   * {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner}.
   */
  public abstract PipelineOptions getPipelineOptions();

  /**
   * Returns the value of the side input for the corresponding state window.
   */
  public abstract <T> T sideInput(PCollectionView<T> view);

  /**
   * Returns the window corresponding to the state.
   */
  public abstract W window();
}
