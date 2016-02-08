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

import com.google.cloud.dataflow.sdk.util.state.MergeableState;
import com.google.cloud.dataflow.sdk.util.state.State;
import com.google.cloud.dataflow.sdk.util.state.StateTag;

/** Interface for interacting with persistent state. */
public interface StateContext {
  /**
   * Access the storage for the given {@code address} in the current window.
   *
   * <p>Never accounts for merged windows. When windows are merged, any state accessed via
   * this method must be eagerly combined and written into the result window.
   */
  <StateT extends State> StateT access(StateTag<StateT> address);

  /**
   * Access the storage for the given {@code address} in all of the windows that were
   * merged into the current window.
   *
   * <p>If no windows were merged, this reads and writes to just the current window.
   * Otherwise, when windows merge we do not eagerly combine state, but rather defer the
   * combination to reading time. Thus reads will be from all 'merged windows' for the
   * current window, and writes will be to the designated 'writing window' for the current window.
   */
  <StateT extends MergeableState<?, ?>> StateT accessAcrossMergedWindows(StateTag<StateT> address);
}
