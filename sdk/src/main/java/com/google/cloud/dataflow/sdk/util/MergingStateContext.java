/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.state.MergeableState;
import com.google.cloud.dataflow.sdk.util.state.State;
import com.google.cloud.dataflow.sdk.util.state.StateTag;

import java.util.Map;

/** Interface for interacting with persistent state within {@link #onMerge}. */
public interface MergingStateContext extends StateContext {
  /**
   * Analogous to {@link #access}, but across all windows which are about to be merged.
   */
  <StateT extends MergeableState<?, ?>> StateT mergingAccess(StateTag<StateT> address);

  /**
   * Analogous to {@link #access}, but returned as a map from each window which is
   * about to be merged to the corresponding state.
   */
  public abstract <StateT extends State> Map<BoundedWindow, StateT>
      mergingAccessInEachMergingWindow(StateTag<StateT> address);
}
