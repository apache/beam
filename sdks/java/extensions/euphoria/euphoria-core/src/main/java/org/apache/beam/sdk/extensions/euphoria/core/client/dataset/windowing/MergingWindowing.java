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
package org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;

import java.util.Collection;

/**
 * @param <T>
 * @param <W>
 */
@Audience(Audience.Type.CLIENT)
public interface MergingWindowing<T, W extends Window<W>> extends Windowing<T, W> {
  /**
   * Given a set of active windows (for a single key) determine which of the windows can be merged.
   * The pairs in the return value define: "(windows-to-be-merged, merge-window)". Returning the
   * same window to be merged multiple times into another is considered erroneous. Note that a
   * window's identity is solely determined by the {@link Window}.
   *
   * @param actives a set of active windows
   * @return a set of merge commands
   */
  Collection<Pair<Collection<W>, W>> mergeWindows(Collection<W> actives);
}
