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
package org.apache.beam.runners.samza.runtime;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A future collector that buffers the output from the users {@link
 * org.apache.beam.sdk.transforms.DoFn} and propagates the result future to downstream operators
 * only after {@link #finish()} is invoked.
 *
 * @param <OutT> type of the output element
 */
public interface FutureCollector<OutT> {
  /**
   * Outputs the element to the collector.
   *
   * @param element to add to the collector
   */
  void add(CompletionStage<WindowedValue<OutT>> element);

  /**
   * Outputs a collection of elements to the collector.
   *
   * @param elements to add to the collector
   */
  void addAll(CompletionStage<Collection<WindowedValue<OutT>>> elements);

  /**
   * Discards the elements within the collector. Once the elements have been discarded, callers need
   * to prepare the collector again before invoking {@link #add(CompletionStage)}.
   */
  void discard();

  /**
   * Seals this {@link FutureCollector}, returning a {@link CompletionStage} containing all of the
   * elements that were added to it. The {@link #add(CompletionStage)} method will throw an {@link
   * IllegalStateException} if called after a call to finish.
   *
   * <p>The {@link FutureCollector} needs to be started again to collect newer batch of output.
   */
  CompletionStage<Collection<WindowedValue<OutT>>> finish();

  /**
   * Prepares the {@link FutureCollector} to accept output elements. The {@link
   * #add(CompletionStage)} method will throw an {@link IllegalStateException} if called without
   * preparing the collector.
   */
  void prepare();
}
