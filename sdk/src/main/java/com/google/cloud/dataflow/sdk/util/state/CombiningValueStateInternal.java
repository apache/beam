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
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;

/**
 * State for a single value that is managed by a {@link CombineFn}. This is an internal extension
 * to {@link CombiningValueState} that includes the {@code AccumT} type.
 *
 * @param <InputT> the type of values added to the state
 * @param <AccumT> the type of accumulator
 * @param <OutputT> the type of value extracted from the state
 */
public interface CombiningValueStateInternal<InputT, AccumT, OutputT>
    extends CombiningValueState<InputT, OutputT> {

  /**
   * Read the merged accumulator for this combining value.
   */
  StateContents<AccumT> getAccum();

  /**
   * Add an accumulator to this combining value. Depending on implementation this may immediately
   * merge it with the previous accumulator, or may buffer this accumulator for a future merge.
   */
  void addAccum(AccumT accum);
}
