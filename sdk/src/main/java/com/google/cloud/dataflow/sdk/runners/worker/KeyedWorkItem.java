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
package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowedValue;

/**
 * Interface that contains all the timers and elements associated with a specific work item.
 *
 * <p>Used as the input type of {@link StreamingGroupAlsoByWindowsDoFn}.
 *
 * @param <K> the key type
 * @param <ElemT> the element type
 */
public interface KeyedWorkItem<K, ElemT> {

  /**
   * Returns the key.
   */
  public K key();

  /**
   * Returns the timers iterable.
   */
  public Iterable<TimerData> timersIterable();

  /**
   * Returns the elements iterable.
   */
  public Iterable<WindowedValue<ElemT>> elementsIterable();
}
