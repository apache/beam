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
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * An wrapper interface that represents the execution of a {@link DoFn}.
 */
public interface DoFnRunner<InputT, OutputT> {
  /**
   * Prepares and calls {@link DoFn#startBundle}.
   */
  public void startBundle();

  /**
   * Calls {@link DoFn#processElement} with a {@link ProcessContext} containing the current element.
   */
  public void processElement(WindowedValue<InputT> elem);

  /**
   * Calls {@link DoFn#finishBundle} and performs additional tasks, such as
   * flushing in-memory states.
   */
  public void finishBundle();

  /**
   * An internal interface for signaling that a {@link DoFn} requires late data dropping.
   */
  public interface ReduceFnExecutor<K, InputT, OutputT, W> {
    /**
     * Gets this object as a {@link DoFn}.
     *
     * Most implementors of this interface are expected to be {@link DoFn} instances, and will
     * return themselves.
     */
    DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> asDoFn();

    /**
     * Returns an aggregator that tracks elements that are dropped due to being late.
     */
    Aggregator<Long, Long> getDroppedDueToLatenessAggregator();
  }
}
