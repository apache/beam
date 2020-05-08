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

import java.util.Map;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * Interface for accessing persistent state while windows are merging.
 *
 * <p>For internal use only.
 */
public interface MergingStateAccessor<K, W extends BoundedWindow> extends StateAccessor<K> {
  /**
   * Analogous to {@link #access}, but returned as a map from each window which is about to be
   * merged to the corresponding state. Only includes windows which are known to have state.
   */
  <StateT extends State> Map<W, StateT> accessInEachMergingWindow(StateTag<StateT> address);
}
