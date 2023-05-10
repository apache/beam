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
package org.apache.beam.sdk.state;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;

/**
 * A {@link ReadableState} cell that combines multiple input values and outputs a single value of a
 * different type.
 *
 * <p>This generalizes {@link GroupByKey} and {@link Combine} styles of grouping.
 *
 * @param <InputT> the type of values added to the state
 * @param <OutputT> the type of value extracted from the state
 */
public interface GroupingState<InputT, OutputT> extends ReadableState<OutputT>, State {
  /**
   * Add a value to the buffer.
   *
   * <p>Elements added will not be reflected in {@code OutputT} objects returned by previous calls
   * to {@link #read}.
   */
  void add(InputT value);

  /**
   * Returns a {@link ReadableState} whose {@link #read} method will return true if this state is
   * empty at the point when that {@link #read} call returns.
   */
  ReadableState<Boolean> isEmpty();

  @Override
  GroupingState<InputT, OutputT> readLater();
}
